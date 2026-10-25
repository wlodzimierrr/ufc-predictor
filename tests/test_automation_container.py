"""Offline guards for the image's input boundary and build-only publication.

These checks deliberately do not build/start Docker or invoke refresh/scoring.
An actual image build and a reviewed runtime smoke test remain operational gates.
"""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
import re
import shlex

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _instructions() -> list[tuple[str, str]]:
    content = re.sub(r"\\\n\s*", " ", (ROOT / "Dockerfile").read_text())
    return [
        tuple(line.split(None, 1))
        for line in content.splitlines()
        if line.strip() and not line.startswith("#")
    ]


def _context_includes(path: str) -> bool:
    """Evaluate the deliberately restricted, file-only build-context allowlist.

    Docker also traverses excluded parents of matching exception paths. Our
    policy never re-includes directories, so matching a file needs no parent
    inheritance. Fail if the policy grows syntax this offline guard cannot model.
    """
    patterns = [
        line.strip()
        for line in (ROOT / ".dockerignore").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert patterns[0] == "**", "Build context must exclude new files by default"
    included = False
    for pattern in patterns[1:]:
        assert pattern.startswith("!"), "Only file exceptions follow default deny"
        pattern = pattern[1:]
        assert not pattern.endswith("/") and "**" not in pattern
        assert not any(char in pattern for char in "?[]\\")
        expression = re.escape(pattern).replace(r"\*", "[^/]*")
        if re.fullmatch(expression, path):
            included = True
    return included


@pytest.mark.parametrize(
    "path",
    [
        ".env", ".env.production", ".git/config", ".aws/credentials",
        ".codex/config.toml", "models/production_model.json",
        "models/xgb/current/model.joblib", "data/events.csv",
        "data/audits/receipt.json", "data/experiments/evidence.py",
        "docs/implementation-reports/private.json", "logs/refresh.log",
        "betting/.env", "features/credentials.json", "modeling/model.joblib",
        "warehouse/tests/test_db.py", "modeling/__pycache__/data.cpython-311.pyc",
        "scripts/automation/.env", "scripts/automation/private/key.pem",
        "scraper/UFC-Web-Scraping-main/settings.py", "unreviewed-new-file.py",
    ],
)
def test_build_context_excludes_secrets_and_operator_artifacts(path: str) -> None:
    assert not _context_includes(path), path


def test_copy_sources_have_only_reviewed_code_and_config() -> None:
    permitted_dirs = {"betting", "features", "modeling", "warehouse", "scripts/automation"}
    permitted_files = {
        "Makefile", "predict.py", "requirements-automation.txt",
        "configs/betting_conservative_candidate.toml",
    }
    copied_files: set[str] = set()
    for instruction, value in _instructions():
        assert instruction != "ADD", "ADD can bypass the explicit local COPY boundary"
        if instruction != "COPY":
            continue
        words = [word for word in shlex.split(value) if not word.startswith("--")]
        for source in words[:-1]:
            source = source.rstrip("/")
            assert source in permitted_dirs | permitted_files, source
            source_path = ROOT / source
            assert source_path.exists(), f"Required image source is absent: {source}"
            candidates = source_path.rglob("*") if source_path.is_dir() else [source_path]
            for path in candidates:
                relative = path.relative_to(ROOT).as_posix()
                if path.is_dir() or not _context_includes(relative):
                    continue
                assert not path.is_symlink(), f"Image source cannot link outside context: {relative}"
                assert path.suffix in {".py", ".sh", ".toml", ".txt"} or relative == "Makefile"
                copied_files.add(relative)
    assert permitted_files <= copied_files
    assert {"scripts/automation/refresh.sh", "scripts/automation/prepare_runtime.py"} <= copied_files
    assert "warehouse/db.py" in copied_files
    assert "modeling/score_upcoming.py" in copied_files
    assert "features/build_upcoming.py" in copied_files
    assert "betting/recommend.py" in copied_files


def test_container_enters_runtime_wrapper_as_nonroot_with_signal_forwarding() -> None:
    instructions = _instructions()
    final = dict(instructions)
    assert final["WORKDIR"] == "/app"
    assert final["USER"] == "10001:10001"
    entrypoint = json.loads(final["ENTRYPOINT"])
    assert entrypoint[:3] == ["/usr/bin/tini", "-g", "--"]
    wrapper = PurePosixPath(entrypoint[3])
    assert wrapper == PurePosixPath("/app/scripts/automation/refresh.sh")
    assert json.loads(final["CMD"]) == []
    assert len(entrypoint) == 4, "Runtime flags must be passed to the validated wrapper"
    assert not any(key in {"ARG", "ENV"} and "DATABASE_URL" in value for key, value in instructions)
    assert not any(key == "COPY" and "runtime-inputs" in value for key, value in instructions)


def test_publish_only_builds_and_pushes_immutable_revision_images() -> None:
    yaml = pytest.importorskip("yaml")
    # BaseLoader preserves GitHub's `on` key rather than applying YAML 1.1 bools.
    workflow = yaml.load((ROOT / ".github/workflows/publish.yml").read_text(), Loader=yaml.BaseLoader)
    assert set(workflow["on"]) == {"push", "pull_request", "workflow_dispatch"}
    assert workflow["on"]["push"]["branches"] == ["main"]
    assert workflow["env"] == {"REGISTRY": "ghcr.io", "IMAGE_NAME": "wlodzimierrr/ufc-data"}
    assert set(workflow["jobs"]) == {"publish"}, "Publication cannot deploy or run refreshes"
    job = workflow["jobs"]["publish"]
    assert job["permissions"] == {"contents": "read", "packages": "write"}
    allowed_actions = {
        "actions/checkout@v4", "docker/setup-buildx-action@v3", "docker/login-action@v3",
        "docker/metadata-action@v5", "docker/build-push-action@v6",
    }
    assert {step.get("uses") for step in job["steps"]} == allowed_actions
    assert not any("run" in step for step in job["steps"])
    steps = {step["uses"].split("@")[0]: step for step in job["steps"]}
    assert steps["docker/login-action"]["if"] == "github.event_name != 'pull_request'"
    build = steps["docker/build-push-action"]["with"]
    assert build["push"] == "${{ github.event_name != 'pull_request' }}"
    assert build["context"] == "."
    assert build["build-args"].strip() == "IMAGE_REVISION=${{ github.sha }}"
    metadata = steps["docker/metadata-action"]["with"]
    assert "type=sha,format=long" in metadata["tags"].splitlines()
    assert build["tags"] == "${{ steps.meta.outputs.tags }}"
    assert metadata["images"] == "${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}"

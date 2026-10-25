"""Image smoke orchestration tests: no Docker, network, DB, or model loading."""
from __future__ import annotations

import builtins
import json
from pathlib import Path
import subprocess

import pytest

from scripts.automation import smoke_image as smoke
from scripts.automation.prepare_runtime import prepare_runtime, validate_bundle


DIGEST = "sha256:" + "a" * 64
REVISION = "b" * 40
IMAGE_REF = f"{smoke.IMAGE}@{DIGEST}"


def image_metadata() -> dict:
    return {"Id": "sha256:" + "c" * 64, "RepoDigests": [IMAGE_REF], "Config": {
        "User": "10001:10001", "WorkingDir": "/app", "Cmd": [],
        "Entrypoint": ["/usr/bin/tini", "-g", "--", "/app/scripts/automation/refresh.sh"],
        "Labels": {"org.opencontainers.image.revision": REVISION},
        "Env": ["IMAGE_REVISION=" + REVISION],
    }}


def measurement(check: str) -> dict:
    common = {"check": check, "uid": 10001, "gid": 10001, "revision": REVISION}
    if check == "imports":
        return dict(common, modules=list(smoke.COMMAND_MODULES))
    return dict(common, source_immutable=True, copies_match_source=True,
                pointer_remapped=True, writable_copies=True, read_only_root=True,
                copied_data_files=5, copied_model_files=3)


class FakeDocker:
    def __init__(self):
        self.calls = []
        self.metadata = image_metadata()
        self.fail_at = None
        self.timeout_at = None
        self.outputs = {}
        self.after_preflight = None

    def __call__(self, arguments, **kwargs):
        self.calls.append((arguments, kwargs))
        assert kwargs == {"capture_output": True, "text": True, "check": False,
                          "timeout": {"pull": 300, "image": 30, "rm": 30,
                                      "run": 120 if arguments[-1] == smoke.IMPORT_SCRIPT else 300}[arguments[1]]}
        stage = arguments[1]
        if stage == "image":
            output = json.dumps([self.metadata])
        elif stage == "run":
            stage = "imports" if arguments[-1] == smoke.IMPORT_SCRIPT else "preflight"
            output = smoke.MARKER + json.dumps(measurement(stage)) + "\n"
            if stage == "preflight" and self.after_preflight:
                self.after_preflight()
        else:
            output = "pulled fake image\n"
        if stage == self.timeout_at:
            raise subprocess.TimeoutExpired("private docker command", kwargs["timeout"], output="password")
        output = self.outputs.get(stage, output)
        if stage == self.fail_at:
            return subprocess.CompletedProcess(arguments, 1, "private-input-data", "postgres://private:password@warehouse/db")
        return subprocess.CompletedProcess(arguments, 0, output, "irrelevant warning with private-input-data")


@pytest.fixture
def bundle(tmp_path):
    return smoke.create_fixture(tmp_path / "inputs")


def test_synthetic_fixture_matches_preflight_contract_without_model_or_warehouse_imports(tmp_path, monkeypatch):
    original_import = builtins.__import__

    def reject_runtime_imports(name, *args, **kwargs):
        assert name.split(".")[0] not in {"joblib", "warehouse", "modeling", "psycopg2"}
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_runtime_imports)
    data, models = smoke.create_fixture(tmp_path / "fixture")
    before = [smoke.snapshot(path) for path in (data, models)]
    counts, pointer, suffix = validate_bundle(data, models)
    assert counts == {"events.csv": 1, "fights.csv": 1, "fighters.csv": 2,
                      "manifests/events_manifest.csv": 1, "odds/fight_odds.csv": 1}
    assert pointer["artifact_path"].startswith("/synthetic/")
    sentinel = (models / suffix / "model.joblib").read_bytes()
    assert sentinel.startswith(b"NOT A SERIALIZED MODEL")
    runtime = tmp_path / "runtime"
    prepare_runtime(runtime, data, models)
    assert (runtime / "models" / suffix / "model.joblib").read_bytes() == sentinel
    assert json.loads((runtime / "models/production_model.json").read_text())["artifact_path"] == str(runtime / "models" / suffix)
    assert [smoke.snapshot(path) for path in (data, models)] == before
    assert all(path.stat().st_mode & 0o004 for root in (data, models) for path in root.rglob("*") if path.is_file())


def test_default_smoke_pulls_exact_digest_and_exercises_image_user_offline():
    docker = FakeDocker()
    evidence = smoke.run_smoke(DIGEST, REVISION, runner=docker)
    assert evidence["status"] == "succeeded"
    assert evidence["input_mode"] == "synthetic_fixture"
    assert evidence["input_review_verified"] is False
    assert evidence["model_serialization_verified"] is False
    assert evidence["database_access_verified"] is False
    assert evidence["refresh_executed"] is False
    assert evidence["checks"]["host_source_immutability"]["status"] == "passed"
    assert [call[0][:2] for call in docker.calls] == [["docker", "pull"], ["docker", "image"], ["docker", "run"], ["docker", "run"]]
    assert docker.calls[0][0] == ["docker", "pull", IMAGE_REF]
    assert docker.calls[1][0] == ["docker", "image", "inspect", IMAGE_REF]
    for arguments, _ in docker.calls[2:]:
        assert arguments[arguments.index("--network") + 1] == "none"
        assert "--read-only" in arguments and "--rm" in arguments
        assert "--user" not in arguments, "Smoke must exercise image USER"
        assert "HOME=/tmp" not in arguments
        name = arguments[arguments.index("--name") + 1]
        assert name.startswith("ufc-image-smoke-") and len(name) == len("ufc-image-smoke-") + 32
        assert arguments[arguments.index("--security-opt") + 1] == "no-new-privileges"
        assert arguments[arguments.index("--cap-drop") + 1] == "ALL"
        temporary = [arguments[index + 1] for index, value in enumerate(arguments) if value == "--tmpfs"]
        assert {value.split(":")[0] for value in temporary} == {"/app/data", "/app/models", "/tmp"}
        assert all("noexec,nosuid,nodev" in value for value in temporary)
        assert IMAGE_REF in arguments
        assert not any("DATABASE_URL" in value or "POSTGRES" in value for value in arguments)
    imports, preflight = [call[0] for call in docker.calls[2:]]
    assert "--mount" not in imports, "Command imports do not need input contents"
    assert imports[-3:] == [IMAGE_REF, "-c", smoke.IMPORT_SCRIPT]
    assert preflight[-6:] == [IMAGE_REF, "-g", "--", "python3", "-c", smoke.PREFLIGHT_SCRIPT]
    mounts = [preflight[index + 1] for index, value in enumerate(preflight) if value == "--mount"]
    assert len(mounts) == 2
    assert all(value.startswith("type=bind,src=") and value.endswith(",readonly") for value in mounts)
    assert {value.split("dst=")[1].split(",")[0] for value in mounts} == {"/runtime-inputs/data", "/runtime-inputs/models"}


@pytest.mark.parametrize("digest,revision", [
    ("latest", REVISION), ("sha-aabbcc", REVISION), ("sha256:" + "a" * 63, REVISION),
    (DIGEST + ";id", REVISION), ("other.example/image@" + DIGEST, REVISION),
    (DIGEST, "main"), (DIGEST, "b" * 39), (DIGEST, REVISION + "\n"),
])
def test_invalid_or_mutable_image_identity_is_rejected_before_docker(digest, revision):
    docker = FakeDocker()
    with pytest.raises(smoke.SmokeFailure, match="^invalid_image_identity$"):
        smoke.run_smoke(digest, revision, runner=docker)
    assert not docker.calls


@pytest.mark.parametrize("mutation", [
    lambda meta: meta.update(RepoDigests=[]),
    lambda meta: meta["Config"].update(User="0"),
    lambda meta: meta["Config"].update(User="10001"),
    lambda meta: meta["Config"].update(WorkingDir="/"),
    lambda meta: meta["Config"].update(Cmd=["make", "update_dashboard_data"]),
    lambda meta: meta["Config"].update(Entrypoint=["/bin/sh"]),
    lambda meta: meta["Config"].update(Env=["IMAGE_REVISION=" + "d" * 40]),
    lambda meta: meta["Config"]["Labels"].update({"org.opencontainers.image.revision": "d" * 40}),
])
def test_unexpected_image_configuration_cannot_start_a_container(mutation):
    docker = FakeDocker()
    mutation(docker.metadata)
    evidence = smoke.run_smoke(DIGEST, REVISION, runner=docker)
    assert evidence["status"] == "failed" and evidence["error"] == "image_identity_mismatch"
    assert not any(call[0][1] == "run" for call in docker.calls)


@pytest.mark.parametrize("stage,error", [
    ("pull", "image_pull_failed"), ("image", "image_inspect_failed"),
    ("imports", "command_imports_failed"), ("preflight", "runtime_preflight_failed"),
])
def test_failed_docker_operation_stops_and_sanitizes_evidence(stage, error):
    docker = FakeDocker()
    docker.fail_at = stage
    evidence = smoke.run_smoke(DIGEST, REVISION, runner=docker)
    assert evidence["status"] == "failed" and evidence["error"] == error
    serialized = json.dumps(evidence)
    assert "password" not in serialized and "private-input-data" not in serialized
    assert "postgres://" not in serialized
    assert evidence["finished_at"] is not None


@pytest.mark.parametrize("change", ["extra_secret", "wrong_uid", "missing_marker", "duplicate_marker", "zero_copies"])
def test_arbitrary_or_incomplete_container_output_is_not_trusted(change):
    docker = FakeDocker()
    result = measurement("preflight")
    if change == "extra_secret":
        result["private"] = "postgres://private:password@warehouse/db"
    if change == "wrong_uid":
        result["uid"] = 0
    if change == "zero_copies":
        result["copied_data_files"] = 0
    output = smoke.MARKER + json.dumps(result) + "\n"
    if change == "missing_marker":
        output = "private traceback with password\n"
    if change == "duplicate_marker":
        output *= 2
    docker.outputs["preflight"] = output
    evidence = smoke.run_smoke(DIGEST, REVISION, runner=docker)
    assert evidence["error"] == "invalid_preflight_evidence"
    assert evidence["status"] == "failed"
    assert "password" not in json.dumps(evidence)


def test_local_bundle_source_change_is_detected_independently_of_container_claims(bundle):
    data, models = bundle
    docker = FakeDocker()
    docker.after_preflight = lambda: (data / "events.csv").write_text("unexpected write")
    evidence = smoke.run_smoke(DIGEST, REVISION, data_dir=data, models_dir=models, runner=docker)
    assert evidence["input_mode"] == "local_bundle"
    assert evidence["input_review_verified"] is False
    assert evidence["checks"]["preflight"]["source_immutable"] is True
    assert evidence["checks"]["host_source_immutability"]["status"] == "failed"
    assert evidence["status"] == "failed" and evidence["error"] == "source_inputs_changed"
    assert str(data) not in json.dumps(evidence)


def test_local_bundle_mode_does_not_modify_inputs_or_leak_paths(bundle, tmp_path):
    data, models = bundle
    before = [smoke.snapshot(path) for path in bundle]
    evidence = smoke.run_smoke(DIGEST, REVISION, data_dir=data, models_dir=models, runner=FakeDocker())
    assert evidence["status"] == "succeeded"
    assert evidence["model_serialization_verified"] is False
    assert [smoke.snapshot(path) for path in bundle] == before
    output = tmp_path / "artifacts/evidence.json"
    smoke.write_evidence(output, evidence)
    assert output.stat().st_mode & 0o777 == 0o600
    assert str(data) not in output.read_text() and "Synthetic card" not in output.read_text()
    assert "NOT A SERIALIZED MODEL" not in output.read_text()


def test_bundle_mount_spec_injection_and_links_are_rejected(bundle, tmp_path):
    data, models = bundle
    comma = tmp_path / "input,readonly=false"
    comma.mkdir()
    docker = FakeDocker()
    result = smoke.run_smoke(DIGEST, REVISION, data_dir=comma, models_dir=models, runner=docker)
    assert result["error"] == "invalid_input_directory" and not docker.calls
    link = tmp_path / "linked-input"
    link.symlink_to(data, target_is_directory=True)
    result = smoke.run_smoke(DIGEST, REVISION, data_dir=link, models_dir=models, runner=docker)
    assert result["error"] == "invalid_input_directory" and not docker.calls


@pytest.mark.parametrize("failure,error", [
    (FileNotFoundError("private executable path"), "docker_unavailable"),
    (subprocess.TimeoutExpired("private docker command", 1, output="password"), "docker_timeout"),
])
def test_docker_unavailable_and_timeout_have_sanitized_receipts(failure, error):
    def unavailable(*args, **kwargs):
        raise failure
    evidence = smoke.run_smoke(DIGEST, REVISION, runner=unavailable)
    assert evidence["error"] == error
    assert "private" not in json.dumps(evidence) and "password" not in json.dumps(evidence)


def test_cli_cannot_write_evidence_inside_reviewed_bundle(bundle):
    data, models = bundle
    before = [smoke.snapshot(path) for path in bundle]
    assert smoke.main(["--digest", DIGEST, "--expected-revision", REVISION,
                       "--data-dir", str(data), "--models-dir", str(models),
                       "--evidence", str(data / "evidence.json")]) == 2
    assert [smoke.snapshot(path) for path in bundle] == before


def test_container_timeout_attempts_cleanup_of_only_the_generated_container():
    docker = FakeDocker()
    docker.timeout_at = "imports"
    evidence = smoke.run_smoke(DIGEST, REVISION, runner=docker)
    assert evidence["error"] == "docker_timeout"
    assert evidence["checks"]["failed_container_cleanup"] == {"attempted": True, "command_succeeded": True}
    arguments = docker.calls[2][0]
    name = arguments[arguments.index("--name") + 1]
    assert docker.calls[3][0] == ["docker", "rm", "--force", name]
    assert len(docker.calls) == 4, "A timed-out import must not progress to preflight"
    assert "password" not in json.dumps(evidence)


def test_cli_invalid_identity_retains_failure_evidence_without_raw_input(tmp_path):
    output = tmp_path / "artifacts/image-smoke.json"
    assert smoke.main(["--digest", "private;malicious value", "--expected-revision", REVISION,
                       "--evidence", str(output)]) == 2
    evidence = json.loads(output.read_text())
    assert evidence["error"] == "invalid_image_identity" and evidence["status"] == "failed"
    assert evidence["input_review_verified"] is False
    assert "private;malicious value" not in output.read_text()


def test_embedded_container_programs_compile_and_have_only_fixed_preflight_command():
    # These programs are executed only in the chosen Docker image, never here.
    compile(smoke.IMPORT_SCRIPT, "imports-probe", "exec")
    compile(smoke.PREFLIGHT_SCRIPT, "preflight-probe", "exec")
    import ast
    program = ast.parse(smoke.PREFLIGHT_SCRIPT)
    calls = [node for node in ast.walk(program) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)
             and node.func.value.id == "subprocess"]
    assert len(calls) == 1 and calls[0].func.attr == "run"
    assert ast.literal_eval(calls[0].args[0]) == ["/app/scripts/automation/refresh.sh", "--preflight-only"]


def test_workflow_only_accepts_identity_inputs_and_uploads_sanitized_json():
    yaml = pytest.importorskip("yaml")
    root = Path(__file__).resolve().parents[1]
    workflow = yaml.load((root / ".github/workflows/validate-image.yml").read_text(), Loader=yaml.BaseLoader)
    assert set(workflow["on"]) == {"workflow_dispatch"}
    assert set(workflow["on"]["workflow_dispatch"]["inputs"]) == {"digest", "expected_revision"}
    assert workflow["permissions"] == {"contents": "read", "packages": "read"}
    assert set(workflow["jobs"]) == {"smoke"}
    steps = workflow["jobs"]["smoke"]["steps"]
    assert {step["uses"] for step in steps if "uses" in step} == {
        "actions/checkout@v4", "docker/login-action@v3", "actions/upload-artifact@v4",
    }
    runners = [step for step in steps if "run" in step]
    assert len(runners) == 1
    assert "${{" not in runners[0]["run"], "Untrusted inputs belong in quoted env args"
    assert runners[0]["env"] == {"IMAGE_DIGEST": "${{ inputs.digest }}", "EXPECTED_REVISION": "${{ inputs.expected_revision }}"}
    assert "--data-dir" not in runners[0]["run"] and "--models-dir" not in runners[0]["run"]
    upload = next(step for step in steps if step.get("uses", "").startswith("actions/upload-artifact"))
    assert upload["if"] == "always()"
    assert upload["with"]["path"] == "artifacts/image-smoke.json"
    assert "*" not in upload["with"]["path"]

"""Validate one published image without database access, scoring, or model loading.

The default bundle is synthetic, including an intentionally non-serialized model
sentinel. Optional local bundle directories are only mounted read-only; neither
their files nor command output are written to the sanitized evidence receipt.
This is an input-copy/import smoke test, never model serialization qualification.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
from uuid import uuid4


IMAGE = "ghcr.io/wlodzimierrr/ufc-data"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
REVISION = re.compile(r"[0-9a-f]{40}\Z")
MARKER = "UFC_IMAGE_SMOKE="
COMMAND_MODULES = (
    "warehouse.load_events", "warehouse.load_fights", "warehouse.load_upcoming_fights",
    "warehouse.load_fight_odds", "modeling.build_pre_event_prediction_log",
    "features.build_upcoming", "modeling.score_upcoming", "predict",
    "betting.backtest", "betting.recommend", "warehouse.load_betting_reports",
    "scripts.automation.run_refresh",
)


class SmokeFailure(RuntimeError):
    """Fixed failure labels only; never include raw subprocess or input errors."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_fixture(directory: Path) -> tuple[Path, Path]:
    """Write a minimal preflight fixture using only Python's standard library."""
    data, models = directory / "data", directory / "models"
    rows = {
        "events.csv": [("event_id", "name", "date"), ("event-fixture", "Synthetic card", "2099-01-01")],
        "fights.csv": [("fight_id", "event_id", "fighter_1_id", "fighter_2_id"),
                       ("fight-fixture", "event-fixture", "fighter-a", "fighter-b")],
        "fighters.csv": [("fighter_id", "full_name"), ("fighter-a", "Synthetic A"),
                         ("fighter-b", "Synthetic B")],
        "manifests/events_manifest.csv": [("event_id", "event_status", "fight_urls"),
                                          ("event-fixture", "upcoming", "fixture://fight")],
        "odds/fight_odds.csv": [("fight_id", "event_id", "fighter_id", "odds_timestamp"),
                                ("fight-fixture", "event-fixture", "fighter-a", "2098-12-31T12:00:00Z")],
    }
    for name, values in rows.items():
        path = data / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as stream:
            csv.writer(stream).writerows(values)
    artifact = models / "synthetic_sentinel" / "fixture-v1"
    artifact.mkdir(parents=True)
    (artifact / "model.joblib").write_bytes(b"NOT A SERIALIZED MODEL: input-copy smoke sentinel only\n")
    (artifact / "metadata.json").write_text(json.dumps({"feature_cols": ["synthetic_feature"]}) + "\n")
    (models / "production_model.json").write_text(json.dumps({
        "selected_model": "synthetic_sentinel",
        "artifact_path": "/synthetic/developer/checkout/models/synthetic_sentinel/fixture-v1",
    }) + "\n")
    # The image runs as UID10001; change only files created here, never real inputs.
    for root in (data, models):
        root.chmod(0o755)
        for path in root.rglob("*"):
            path.chmod(0o755 if path.is_dir() else 0o644)
    return data, models


def snapshot(directory: Path) -> dict[str, tuple]:
    """Keep content/mode/mtime measurements in memory, outside the evidence file."""
    if directory.is_symlink() or not directory.is_dir():
        raise SmokeFailure("invalid_input_directory")
    result = {}
    for path in (directory, *sorted(directory.rglob("*"))):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise SmokeFailure("unsupported_input_file_type")
        info = path.stat()
        digest = None
        if path.is_file():
            hasher = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    hasher.update(chunk)
            digest = hasher.hexdigest()
        result[path.relative_to(directory).as_posix()] = (digest, info.st_mode, info.st_size if path.is_file() else None, info.st_mtime_ns)
    return result


# Fixed scripts, never supplied by workflow/CLI inputs. Output is parsed and
# reduced to known fields on the host; warnings/tracebacks are not retained.
IMPORT_SCRIPT = r'''
import importlib, json, os
if os.getuid() != 10001 or os.getgid() != 10001:
    raise RuntimeError("unexpected_runtime_user")
if os.environ.get("IMAGE_REVISION") != os.environ["SMOKE_EXPECTED_REVISION"]:
    raise RuntimeError("unexpected_runtime_revision")
if not os.statvfs("/").f_flag & os.ST_RDONLY:
    raise RuntimeError("writable_root")
def forbidden(*args, **kwargs):
    raise RuntimeError("database_or_model_loading_forbidden")
import psycopg2, joblib
psycopg2.connect = forbidden
joblib.load = forbidden
modules = json.loads(os.environ["SMOKE_MODULES"])
for name in modules:
    importlib.import_module(name)
print("UFC_IMAGE_SMOKE=" + json.dumps({"check": "imports", "uid": os.getuid(),
    "gid": os.getgid(), "revision": os.environ["IMAGE_REVISION"], "modules": modules}))
'''

PREFLIGHT_SCRIPT = r'''
import hashlib, json, os, pathlib, subprocess
if os.getuid() != 10001 or os.getgid() != 10001:
    raise RuntimeError("unexpected_runtime_user")
if os.environ.get("IMAGE_REVISION") != os.environ["SMOKE_EXPECTED_REVISION"]:
    raise RuntimeError("unexpected_runtime_revision")
for directory in ("/", "/runtime-inputs/data", "/runtime-inputs/models"):
    if not os.statvfs(directory).f_flag & os.ST_RDONLY:
        raise RuntimeError("writable_immutable_mount")
def tree(root):
    result = {}
    for path in (root, *sorted(root.rglob("*"))):
        if path.is_symlink() or not (path.is_dir() or path.is_file()):
            raise RuntimeError("unsupported_input_file_type")
        info = path.stat()
        digest = None
        if path.is_file():
            h = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    h.update(chunk)
            digest = h.hexdigest()
        result[path.relative_to(root).as_posix()] = (digest, info.st_mode, info.st_mtime_ns)
    return result
sources = [pathlib.Path("/runtime-inputs/data"), pathlib.Path("/runtime-inputs/models")]
before = [tree(path) for path in sources]
completed = subprocess.run(["/app/scripts/automation/refresh.sh", "--preflight-only"],
                           capture_output=True, text=True, timeout=240)
if completed.returncode != 0 or "automation preflight=succeeded input_rows=" not in completed.stdout:
    raise RuntimeError("wrapper_preflight_failed")
if "automation end=" not in completed.stdout or "status=succeeded exit_code=0" not in completed.stdout:
    raise RuntimeError("wrapper_status_missing")
if [tree(path) for path in sources] != before:
    raise RuntimeError("source_changed")
copied_counts = []
for source, destination in zip(sources, [pathlib.Path("/app/data"), pathlib.Path("/app/models")]):
    original, copied = tree(source), tree(destination)
    if set(copied) != set(original) | {".automation-runtime-copy.json"}:
        raise RuntimeError("copy_file_set_mismatch")
    for name, measurement in original.items():
        if source.name == "models" and name == "production_model.json":
            continue
        if copied[name][0] != measurement[0]:
            raise RuntimeError("copy_content_mismatch")
    ownership = json.loads((destination / ".automation-runtime-copy.json").read_text())
    if ownership != {"contract": "private-refresh-copy-v1", "destination": str(destination)}:
        raise RuntimeError("copy_ownership_missing")
    probe = destination / ".image-smoke-write-probe"
    probe.write_text("disposable output only\n")
    probe.unlink()
    copied_counts.append(sum(value[0] is not None for value in original.values()))
original_pointer = json.loads((sources[1] / "production_model.json").read_text())
copied_pointer = json.loads(pathlib.Path("/app/models/production_model.json").read_text())
parts = pathlib.Path(original_pointer["artifact_path"]).parts
suffix = pathlib.Path(*parts[parts.index("models") + 1:])
expected_pointer = dict(original_pointer, artifact_path=str(pathlib.Path("/app/models") / suffix))
if copied_pointer != expected_pointer:
    raise RuntimeError("pointer_translation_mismatch")
print("UFC_IMAGE_SMOKE=" + json.dumps({"check": "preflight", "uid": os.getuid(),
    "gid": os.getgid(), "revision": os.environ["IMAGE_REVISION"], "source_immutable": True,
    "copies_match_source": True, "pointer_remapped": True, "writable_copies": True,
    "read_only_root": True, "copied_data_files": copied_counts[0],
    "copied_model_files": copied_counts[1]}))
'''


def docker_run_arguments(image_ref: str, revision: str) -> list[str]:
    """No --user override: verify and exercise the image's configured USER."""
    return [
        "docker", "run", "--rm", "--network", "none", "--read-only",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--pids-limit", "128", "--memory", "4g", "--cpus", "2",
        "--tmpfs", "/app/data:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700,size=1g",
        "--tmpfs", "/app/models:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700,size=2g",
        "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,mode=1777,size=256m",
        "--env", "PYTHONDONTWRITEBYTECODE=1",
        "--env", "OMP_NUM_THREADS=1", "--env", "OPENBLAS_NUM_THREADS=1",
        "--env", "MKL_NUM_THREADS=1", "--env", "NUMEXPR_MAX_THREADS=1",
        "--env", f"SMOKE_EXPECTED_REVISION={revision}",
    ]


def _execute(arguments: list[str], *, runner, timeout: int, failure: str):
    try:
        result = runner(arguments, capture_output=True, text=True, check=False, timeout=timeout)
    except FileNotFoundError as exc:
        raise SmokeFailure("docker_unavailable") from exc
    except subprocess.TimeoutExpired as exc:
        raise SmokeFailure("docker_timeout") from exc
    except OSError as exc:
        raise SmokeFailure("docker_execution_failed") from exc
    if result.returncode:
        raise SmokeFailure(failure)
    return result


def _execute_container(arguments: list[str], *, runner, timeout: int,
                       failure: str, checks: dict):
    # A killed Docker client can leave its container running. Restrict cleanup
    # to a generated name belonging to this single attempt, including on timeout.
    name = "ufc-image-smoke-" + uuid4().hex
    arguments = arguments[:2] + ["--name", name] + arguments[2:]
    try:
        return _execute(arguments, runner=runner, timeout=timeout, failure=failure)
    except SmokeFailure:
        cleaned = False
        try:
            result = runner(["docker", "rm", "--force", name], capture_output=True,
                            text=True, check=False, timeout=30)
            cleaned = result.returncode == 0
        except Exception:
            pass
        # With --rm, an exited container may already be absent (rm exits 1).
        # Record the measured cleanup result without claiming it still exists.
        checks["failed_container_cleanup"] = {"attempted": True, "command_succeeded": cleaned}
        raise


def _measurement(result, *, check: str, revision: str) -> dict:
    try:
        lines = [line[len(MARKER):] for line in result.stdout.splitlines() if line.startswith(MARKER)]
        if len(lines) != 1:
            raise ValueError
        measurement = json.loads(lines[0])
        common = {"check": check, "uid": 10001, "gid": 10001, "revision": revision}
        if check == "imports":
            if measurement != dict(common, modules=list(COMMAND_MODULES)):
                raise ValueError
            return {"status": "passed", "uid": 10001, "gid": 10001, "module_count": len(COMMAND_MODULES)}
        expected = dict(common, source_immutable=True, copies_match_source=True,
                        pointer_remapped=True, writable_copies=True, read_only_root=True)
        for key in ("copied_data_files", "copied_model_files"):
            count = measurement.get(key)
            if type(count) is not int or count <= 0:
                raise ValueError
            expected[key] = count
        if measurement != expected:
            raise ValueError
        return {"status": "passed", **{key: value for key, value in expected.items()
                                          if key not in {"check", "revision"}}}
    except (AttributeError, TypeError, ValueError):
        raise SmokeFailure(f"invalid_{check}_evidence") from None


def run_smoke(digest: str, revision: str, *, data_dir: Path | None = None,
              models_dir: Path | None = None, runner=subprocess.run) -> dict:
    """Run fixed Docker operations; inject a fake runner for offline tests."""
    if not DIGEST.fullmatch(digest) or not REVISION.fullmatch(revision):
        raise SmokeFailure("invalid_image_identity")
    if (data_dir is None) != (models_dir is None):
        raise SmokeFailure("both_bundle_directories_required")
    image_ref = f"{IMAGE}@{digest}"
    evidence = {
        "schema": "ufc-data-image-smoke-v1", "started_at": utc_now(), "finished_at": None,
        "image_ref": image_ref, "expected_revision": revision, "status": "failed",
        "input_mode": "local_bundle" if data_dir is not None else "synthetic_fixture",
        "input_review_verified": False,
        "scope": "no-network command imports and input-copy preflight only",
        "model_serialization_verified": False, "database_access_verified": False,
        "refresh_executed": False, "checks": {}, "error": None,
    }
    before = None
    with tempfile.TemporaryDirectory(prefix="ufc-image-smoke-") as temporary:
        try:
            if data_dir is None:
                data_dir, models_dir = create_fixture(Path(temporary))
            # Resolve only after rejecting a top-level link, preserving the source
            # policy enforced by prepare_runtime. Docker --mount uses commas.
            for path in (data_dir, models_dir):
                if path.is_symlink() or "," in str(path.resolve()):
                    raise SmokeFailure("invalid_input_directory")
            data_dir, models_dir = data_dir.resolve(), models_dir.resolve()
            before = [snapshot(path) for path in (data_dir, models_dir)]
            _execute(["docker", "pull", image_ref], runner=runner, timeout=300, failure="image_pull_failed")
            evidence["checks"]["pull"] = {"status": "passed"}
            inspected = _execute(["docker", "image", "inspect", image_ref], runner=runner,
                                 timeout=30, failure="image_inspect_failed")
            try:
                images = json.loads(inspected.stdout)
                if len(images) != 1:
                    raise ValueError
                image = images[0]
                config = image["Config"]
                environment = dict(entry.split("=", 1) for entry in config["Env"])
                if (image_ref not in image["RepoDigests"] or not DIGEST.fullmatch(image["Id"])
                        or config["User"] != "10001:10001" or config["WorkingDir"] != "/app"
                        or config["Labels"]["org.opencontainers.image.revision"] != revision
                        or environment.get("IMAGE_REVISION") != revision
                        or config["Entrypoint"] != ["/usr/bin/tini", "-g", "--", "/app/scripts/automation/refresh.sh"]
                        or config.get("Cmd") not in (None, [])):
                    raise ValueError
            except (KeyError, TypeError, ValueError):
                raise SmokeFailure("image_identity_mismatch") from None
            evidence["checks"]["identity"] = {"status": "passed", "configured_user": "10001:10001",
                                               "image_config_digest": image["Id"]}
            arguments = docker_run_arguments(image_ref, revision)
            imported = _execute_container(arguments + ["--env", "SMOKE_MODULES=" + json.dumps(COMMAND_MODULES),
                "--entrypoint", "python3", image_ref, "-c", IMPORT_SCRIPT],
                runner=runner, timeout=120, failure="command_imports_failed", checks=evidence["checks"])
            evidence["checks"]["imports"] = _measurement(imported, check="imports", revision=revision)
            prepared = _execute_container(arguments + [
                "--mount", f"type=bind,src={data_dir},dst=/runtime-inputs/data,readonly",
                "--mount", f"type=bind,src={models_dir},dst=/runtime-inputs/models,readonly",
                "--entrypoint", "/usr/bin/tini", image_ref, "-g", "--", "python3", "-c", PREFLIGHT_SCRIPT,
            ], runner=runner, timeout=300, failure="runtime_preflight_failed", checks=evidence["checks"])
            evidence["checks"]["preflight"] = _measurement(prepared, check="preflight", revision=revision)
            evidence["status"] = "succeeded"
        except SmokeFailure as exc:
            evidence["error"] = str(exc)
        except Exception:
            evidence["error"] = "smoke_internal_error"
        finally:
            if before is not None:
                try:
                    unchanged = [snapshot(path) for path in (data_dir, models_dir)] == before
                except Exception:
                    unchanged = False
                evidence["checks"]["host_source_immutability"] = {"status": "passed" if unchanged else "failed"}
                if not unchanged:
                    evidence.update(status="failed", error="source_inputs_changed")
            evidence["finished_at"] = utc_now()
    return evidence


def write_evidence(path: Path, evidence: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # No raw logs, environment dumps, bundle names/paths/contents or file hashes.
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".image-smoke-", delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(evidence, stream, indent=2, sort_keys=True)
        stream.write("\n")
    temporary.replace(path)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--digest", required=True, help="Full sha256: digest; image repository is fixed")
    parser.add_argument("--expected-revision", required=True, help="Full 40-character source Git SHA")
    parser.add_argument("--evidence", type=Path, required=True, help="Sanitized JSON receipt path")
    parser.add_argument("--data-dir", type=Path, help="Optional local data bundle; this test does not certify input review")
    parser.add_argument("--models-dir", type=Path, help="Optional local model bundle; never loaded")
    args = parser.parse_args(argv)
    try:
        # Evidence must never replace or add a file in an operator's input tree.
        for source in (args.data_dir, args.models_dir):
            if source is not None and args.evidence.resolve().is_relative_to(source.resolve()):
                raise SmokeFailure("evidence_overlaps_input_bundle")
    except Exception:
        print("image smoke=failed category=invalid_arguments_or_evidence_path", flush=True)
        return 2
    invalid_arguments = False
    try:
        evidence = run_smoke(args.digest, args.expected_revision, data_dir=args.data_dir, models_dir=args.models_dir)
    except Exception as exc:
        invalid_arguments = True
        error = str(exc) if isinstance(exc, SmokeFailure) else "smoke_internal_error"
        evidence = {"schema": "ufc-data-image-smoke-v1", "status": "failed", "error": error,
                    "finished_at": utc_now(), "input_review_verified": False,
                    "input_mode": "local_bundle" if args.data_dir or args.models_dir else "synthetic_fixture",
                    "model_serialization_verified": False, "database_access_verified": False,
                    "refresh_executed": False, "checks": {}}
    try:
        write_evidence(args.evidence, evidence)
    except Exception:
        print("image smoke=failed category=evidence_write_failed", flush=True)
        return 2
    print(f"image smoke={evidence['status']} input_mode={evidence['input_mode']} model_serialization_verified=false", flush=True)
    if invalid_arguments:
        return 2
    return 0 if evidence["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())

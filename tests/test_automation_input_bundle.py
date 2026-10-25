"""Candidate packaging tests use only disposable local files, never model loading."""
import csv
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

from scripts.automation import build_input_bundle as builder
from scripts.automation.prepare_runtime import DATA_FILES, prepare_runtime, validate_bundle

ROOT = Path(__file__).resolve().parents[1]


def sources(tmp_path):
    data = tmp_path / "source/data"
    models = tmp_path / "source/models"
    for name, columns in DATA_FILES.items():
        path = data / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=sorted(columns))
            writer.writeheader()
            writer.writerow({column: "synthetic" for column in columns})
    selected = models / "xgb/selected"
    selected.mkdir(parents=True)
    (selected / "model.joblib").write_bytes(b"synthetic binary, never deserialize\x00\xff")
    (selected / "metadata.json").write_text(json.dumps({"feature_cols": ["example_feature"]}))
    (selected / "support/config.json").parent.mkdir()
    (selected / "support/config.json").write_text('{"support":true}\n')
    (models / "production_model.json").write_text(json.dumps({
        "selected_model": "Synthetic", "artifact_path": "/old/private-checkout/models/xgb/selected"}) + "\n")
    excluded = [data / "raw/source.html", data / "reports/old.csv", data / ".env",
                models / "xgb/old/model.joblib", models / "holdouts/evidence.csv",
                models / "experiments/unsafe.py", models / "upcoming/prediction_history.csv"]
    for path in excluded:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("private excluded content")
    return data, models


def file_states(root):
    return {path.relative_to(root).as_posix(): (path.read_bytes(), stat.S_IMODE(path.stat().st_mode))
            for path in root.rglob("*") if path.is_file()}


def test_exact_minimal_bytes_modes_and_manifest_are_deterministic(tmp_path):
    data, models = sources(tmp_path)
    (models / "production_model.json").chmod(0o440)
    (models / "xgb/selected/model.joblib").chmod(0o640)
    before = file_states(data), file_states(models)
    first, second = tmp_path / "first", tmp_path / "second"
    manifest = builder.build_input_bundle(data, models, first)
    builder.build_input_bundle(data, models, second)
    expected = {f"data/{name}" for name in DATA_FILES} | {
        "models/production_model.json", "models/xgb/selected/model.joblib",
        "models/xgb/selected/metadata.json", "models/xgb/selected/support/config.json"}
    assert {path.relative_to(first).as_posix() for path in first.rglob("*") if path.is_file()} == expected | {builder.MANIFEST}
    assert before == (file_states(data), file_states(models))
    for entry in manifest["files"]:
        relative = Path(entry["path"])
        original = (data if relative.parts[0] == "data" else models) / Path(*relative.parts[1:])
        copied = first / relative
        assert copied.read_bytes() == original.read_bytes()
        assert stat.S_IMODE(copied.stat().st_mode) == stat.S_IMODE(original.stat().st_mode) == entry["mode"]
        assert entry["sha256"] == entry["source_sha256"] == hashlib.sha256(copied.read_bytes()).hexdigest()
        assert entry["size_bytes"] == len(copied.read_bytes())
    serialized = (first / builder.MANIFEST).read_bytes()
    assert serialized == (second / builder.MANIFEST).read_bytes()
    assert str(data).encode() not in serialized and str(models).encode() not in serialized
    assert b"private-checkout" not in serialized and b"private excluded content" not in serialized
    assert manifest["selected_artifact"] == "models/xgb/selected"
    assert manifest["status"] == "candidate" and manifest["review_status"] == "not_reviewed"
    assert manifest["input_rows"] == {f"data/{name}": 1 for name in DATA_FILES}
    assert not (first / builder.INCOMPLETE).exists()
    assert validate_bundle(first / "data", first / "models")[0] == {name: 1 for name in DATA_FILES}
    # Packaging preserves the old pointer; only runtime preparation translates it.
    assert (first / "models/production_model.json").read_bytes() == (models / "production_model.json").read_bytes()
    runtime = tmp_path / "runtime"
    prepare_runtime(runtime, first / "data", first / "models")
    assert json.loads((runtime / "models/production_model.json").read_text())["artifact_path"] == str(runtime / "models/xgb/selected")


@pytest.mark.parametrize("target", ["data", "inside_data", "models", "inside_models", "source_ancestor"])
def test_output_source_overlap_is_rejected_without_source_changes(tmp_path, target):
    data, models = sources(tmp_path)
    before = file_states(data), file_states(models)
    output = {"data": data, "inside_data": data / "candidate", "models": models,
              "inside_models": models / "candidate", "source_ancestor": data.parent}[target]
    with pytest.raises(builder.BundleError, match="output_source_overlap"):
        builder.build_input_bundle(data, models, output)
    assert before == (file_states(data), file_states(models))


@pytest.mark.parametrize("existing", ["empty_dir", "nonempty_dir", "file", "dangling_symlink"])
def test_output_never_overwrites_existing_namespace(tmp_path, existing):
    data, models = sources(tmp_path)
    output = tmp_path / "candidate"
    if existing == "file":
        output.write_text("operator-owned")
    elif existing == "dangling_symlink":
        output.symlink_to(tmp_path / "missing")
    else:
        output.mkdir()
        if existing == "nonempty_dir":
            (output / "keep").write_text("operator-owned")
    with pytest.raises(builder.BundleError, match="output_already_exists"):
        builder.build_input_bundle(data, models, output)
    assert not (output / builder.MANIFEST).is_file()
    if existing == "nonempty_dir":
        assert (output / "keep").read_text() == "operator-owned"
    if existing == "file":
        assert output.read_text() == "operator-owned"
    if existing == "dangling_symlink":
        assert output.is_symlink() and not (tmp_path / "missing").exists()


@pytest.mark.parametrize("location", ["data_root", "models_root", "csv", "artifact", "excluded"])
def test_source_symlinks_are_rejected_even_outside_selected_files(tmp_path, location):
    data, models = sources(tmp_path)
    if location.endswith("root"):
        link = tmp_path / "source-alias"
        link.symlink_to(data if location == "data_root" else models, target_is_directory=True)
        data, models = (link, models) if location == "data_root" else (data, link)
    elif location == "csv":
        path = data / "events.csv"
        original = data / "original.csv"
        path.rename(original)
        path.symlink_to(original)
    else:
        path = models / ("xgb/selected/link" if location == "artifact" else "holdouts/link")
        path.symlink_to(models / "production_model.json")
    output = tmp_path / "candidate"
    with pytest.raises(builder.BundleError, match="source_symlink"):
        builder.build_input_bundle(data, models, output)
    assert not output.exists()


@pytest.mark.parametrize("missing", ["data/events.csv", "models/xgb/selected/model.joblib"])
def test_invalid_bundle_fails_before_reserving_output(tmp_path, missing):
    data, models = sources(tmp_path)
    (data.parent / missing).unlink()
    output = tmp_path / "candidate"
    with pytest.raises(FileNotFoundError if missing.endswith("csv") else ValueError):
        builder.build_input_bundle(data, models, output)
    assert not output.exists()


def test_source_mutation_during_copy_leaves_explicit_incomplete_inventory(tmp_path, monkeypatch):
    data, models = sources(tmp_path)
    original = builder.copy_checked
    changed = data / "events.csv"
    initial = changed.stat()
    copied = []
    def dirty_copy(item, output):
        original(item, output)
        copied.append(item.relative.as_posix())
        if len(copied) == 9:
            # Keep size and mtime unchanged: hashes/ctime must still detect this.
            changed.write_bytes(changed.read_bytes().replace(b"synthetic", b"different"))
            os.utime(changed, ns=(initial.st_atime_ns, initial.st_mtime_ns))
    monkeypatch.setattr(builder, "copy_checked", dirty_copy)
    output = tmp_path / "candidate"
    with pytest.raises(builder.BundleError, match="source_changed"):
        builder.build_input_bundle(data, models, output)
    assert not (output / builder.MANIFEST).exists()
    marker = json.loads((output / builder.INCOMPLETE).read_text())
    assert marker["status"] == "incomplete" and len(marker["files"]) == 9
    with pytest.raises(builder.BundleError, match="output_already_exists"):
        builder.build_input_bundle(data, models, output)


def test_source_inventory_addition_during_copy_is_detected(tmp_path, monkeypatch):
    data, models = sources(tmp_path)
    original = builder.copy_checked
    def add_file(item, output):
        original(item, output)
        (models / "xgb/selected/new-support.json").write_text("{}")
    monkeypatch.setattr(builder, "copy_checked", add_file)
    output = tmp_path / "candidate"
    with pytest.raises(builder.BundleError, match="source_inventory_changed"):
        builder.build_input_bundle(data, models, output)
    assert (output / builder.INCOMPLETE).exists() and not (output / builder.MANIFEST).exists()


def test_concurrent_output_creation_is_preserved(tmp_path, monkeypatch):
    data, models = sources(tmp_path)
    output = tmp_path / "candidate"
    original = builder.read_source
    created = []
    def concurrent_read(*args, **kwargs):
        result = original(*args, **kwargs)
        if not created:
            output.mkdir()
            created.append(True)
        return result
    monkeypatch.setattr(builder, "read_source", concurrent_read)
    with pytest.raises(FileExistsError):
        builder.build_input_bundle(data, models, output)
    assert output.is_dir() and list(output.iterdir()) == []


def test_partial_copy_failure_has_inventory_and_never_claims_completion(tmp_path, monkeypatch):
    data, models = sources(tmp_path)
    def fail_copy(*args):
        raise OSError("synthetic interruption")
    monkeypatch.setattr(builder, "copy_checked", fail_copy)
    output = tmp_path / "candidate"
    with pytest.raises(OSError):
        builder.build_input_bundle(data, models, output)
    assert json.loads((output / builder.INCOMPLETE).read_text())["status"] == "incomplete"
    assert not (output / builder.MANIFEST).exists()


def test_cli_local_only_success_and_sanitized_error(tmp_path):
    data, models = sources(tmp_path)
    script = ROOT / "scripts/automation/build_input_bundle.py"
    output = tmp_path / "candidate"
    environment = dict(os.environ, DATABASE_URL="postgresql://must-not-connect:password@invalid/db")
    command = [sys.executable, str(script), "--data-source", str(data), "--models-source", str(models), "--output", str(output)]
    result = subprocess.run(command, env=environment, capture_output=True, text=True)
    assert result.returncode == 0 and "status=candidate review_status=not_reviewed files=9" in result.stdout
    result = subprocess.run(command, env=environment, capture_output=True, text=True)
    assert result.returncode == 1 and "code=output_already_exists" in result.stderr
    assert "password" not in result.stdout + result.stderr
    assert str(data) not in result.stdout + result.stderr

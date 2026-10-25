"""Validate reviewed input bundles and copy them into a disposable runtime tree."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import shutil

COPY_MARKER = ".automation-runtime-copy.json"

DATA_FILES = {
    "events.csv": {"event_id", "name", "date"},
    "fights.csv": {"fight_id", "event_id", "fighter_1_id", "fighter_2_id"},
    "fighters.csv": {"fighter_id", "full_name"},
    "manifests/events_manifest.csv": {"event_id", "event_status", "fight_urls"},
    "odds/fight_odds.csv": {"fight_id", "event_id", "fighter_id", "odds_timestamp"},
}


class InputError(ValueError):
    """An input-bundle contract violation; messages contain no input values."""


def model_suffix(pointer: dict) -> Path:
    raw = pointer.get("artifact_path")
    if not isinstance(raw, str) or not raw or not pointer.get("selected_model"):
        raise InputError("invalid_model_pointer")
    path = Path(raw)
    if ".." in path.parts or "models" not in path.parts:
        raise InputError("model_pointer_outside_bundle")
    # A former developer checkout may have a different absolute prefix.
    index = path.parts.index("models")
    if not path.is_absolute() and index != 0:
        raise InputError("model_pointer_outside_bundle")
    suffix = Path(*path.parts[index + 1:])
    if not suffix.parts:
        raise InputError("invalid_model_pointer")
    return suffix


def validate_bundle(data: Path, models: Path) -> tuple[dict[str, int], dict, Path]:
    counts = {}
    for name, required in DATA_FILES.items():
        with (data / name).open(newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            if not required.issubset(reader.fieldnames or []):
                raise InputError("invalid_input_csv_columns")
            # Scraper output can contain embedded headers; loaders skip these too.
            counts[name] = sum(1 for row in reader if any(row.values()) and
                               sum(v == k for k, v in row.items()) < max(3, len(row) // 3))
            if counts[name] == 0:
                raise InputError("empty_input_csv")
    pointer = json.loads((models / "production_model.json").read_text())
    if not isinstance(pointer, dict):
        raise InputError("invalid_model_pointer")
    suffix = model_suffix(pointer)
    artifact = models / suffix
    for name in ("model.joblib", "metadata.json"):
        if not (artifact / name).is_file() or (artifact / name).stat().st_size == 0:
            raise InputError("missing_production_model_artifact")
    metadata = json.loads((artifact / "metadata.json").read_text())
    if not isinstance(metadata, dict) or not metadata.get("feature_cols"):
        raise InputError("invalid_model_metadata")
    return counts, pointer, suffix


def prepare_runtime(repo: Path, source_data: Path, source_models: Path) -> dict[str, int]:
    """Never write to source bundles, including their model selection pointer."""
    repo = repo.resolve()
    sources = (source_data.resolve(), source_models.resolve())
    destinations = (repo / "data", repo / "models")
    for original in (source_data, source_models):
        if original.is_symlink():
            raise InputError("bundle_symlinks_not_supported")
    for source, destination in zip(sources, destinations):
        resolved = destination.resolve()
        if not source.is_dir():
            raise InputError("missing_input_bundle")
        for other_destination in destinations:
            resolved = other_destination.resolve()
            if source == resolved or source in resolved.parents or resolved in source.parents:
                raise InputError("source_destination_overlap")
        # Following bundle links could escape the reviewed bundle or mutate sources
        # through a previously copied destination link. Require ordinary files.
        if source.is_symlink() or any(p.is_symlink() for p in source.rglob("*")):
            raise InputError("bundle_symlinks_not_supported")
        if destination.is_symlink() or (destination.exists() and
                any(p.is_symlink() for p in destination.rglob("*"))):
            raise InputError("runtime_symlinks_not_supported")
    # Existing developer data is never a disposable runtime destination. Only
    # directories explicitly marked by a previous preparation may be cleared.
    for destination in destinations:
        if destination.exists() and any(destination.iterdir()):
            marker = destination / COPY_MARKER
            expected = {"contract": "private-refresh-copy-v1", "destination": str(destination)}
            try:
                owned = json.loads(marker.read_text()) == expected
            except (OSError, ValueError):
                owned = False
            if not owned:
                raise InputError("nonempty_runtime_destination")
    counts, pointer, suffix = validate_bundle(*sources)
    for source, destination in zip(sources, destinations):
        destination.mkdir(parents=True, exist_ok=True)
        marker = destination / COPY_MARKER
        if marker.exists():
            for previous in destination.iterdir():
                if previous.is_dir():
                    shutil.rmtree(previous)
                else:
                    previous.unlink()
        ownership = {"contract": "private-refresh-copy-v1", "destination": str(destination)}
        marker.write_text(json.dumps(ownership) + "\n")
        shutil.copytree(source, destination, dirs_exist_ok=True)
        for copied in (destination, *destination.rglob("*")):
            copied.chmod(copied.stat().st_mode | (0o700 if copied.is_dir() else 0o600))
        marker.write_text(json.dumps(ownership) + "\n")
    # Translate only this disposable copy. Model bytes and source pointer stay exact.
    pointer["artifact_path"] = str(destinations[1] / suffix)
    (destinations[1] / "production_model.json").write_text(json.dumps(pointer, indent=2) + "\n")
    validate_bundle(*destinations)
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("/app"))
    parser.add_argument("--data-source", type=Path, default=Path("/runtime-inputs/data"))
    parser.add_argument("--models-source", type=Path, default=Path("/runtime-inputs/models"))
    args = parser.parse_args()
    try:
        counts = prepare_runtime(args.repo_root, args.data_source, args.models_source)
    except Exception as exc:
        print(f"automation preflight=failed error_category={type(exc).__name__}", flush=True)
        return 1
    print("automation preflight=succeeded input_rows=" + json.dumps(counts, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

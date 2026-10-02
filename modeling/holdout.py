"""Read prediction identities for training exclusions, without reading outcomes.

This helper is deliberately not connected to a training pipeline. It reads only
accepted manifests, outcome-free predictions, and identity/exclusion evidence.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

DEFAULT_HOLDOUT_DIR = (
    Path(__file__).resolve().parents[1] / "data/holdouts/historical_2026_apr_aug"
)
COHORT_NAME = "historical_2026_apr_aug"
PRE_EVENT_COHORT = "pre_event_2026_apr_aug"
SCHEMA_VERSION = 2
EXPECTED_FIGHTS = 146
POPULATIONS = {
    COHORT_NAME: {"count": 146, "classification": "mixed_pre_event_catchup_historical"},
    PRE_EVENT_COHORT: {"count": 108, "classification": "original_pre_event_forecasts"},
}
# Reject evaluation columns if a historical joined report is supplied by mistake.
OUTCOME_COLUMNS = frozenset({
    "label", "actual_label", "resolved", "correct", "is_correct",
    "predicted_correct", "actual_winner_name", "actual_fight_id",
    "winner_fighter_id", "result_type", "finish_method", "finish_round",
    "finish_time_seconds", "fighter_1_outcome", "fighter_2_outcome",
})


class HoldoutError(ValueError):
    """Holdout data is absent, unaccepted, or invalid."""


def _read_manifest(directory: Path) -> dict:
    path = directory / "manifest.json"
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise HoldoutError(f"Cannot read frozen manifest: {path}") from exc
    if not isinstance(manifest, dict):
        raise HoldoutError("Manifest must be an object")
    population = POPULATIONS.get(manifest.get("cohort_name"))
    if population is None:
        raise HoldoutError("Manifest cohort_name is not an approved population")
    expected = {
        "status": "FROZEN", "classification": population["classification"],
        "schema_version": SCHEMA_VERSION, "total_row_count": population["count"],
        "unique_fight_count": population["count"],
    }
    for key, value in expected.items():
        if type(manifest.get(key)) is not type(value) or manifest[key] != value:
            raise HoldoutError(f"Manifest {key} must equal {value!r}")
    return manifest


def _checked_file(directory: Path, entry: dict) -> tuple[Path, bytes]:
    if not isinstance(entry, dict):
        raise HoldoutError("Manifest file entry must be an object")
    filename, expected_hash = entry.get("path"), entry.get("sha256")
    if not isinstance(filename, str) or not filename:
        raise HoldoutError("Manifest file path is missing")
    relative = Path(filename)
    if relative.is_absolute() or ".." in relative.parts:
        raise HoldoutError("Manifest file path must stay inside the holdout directory")
    path = directory / relative
    if not path.resolve().is_relative_to(directory.resolve()):
        raise HoldoutError("Manifest file symlink escapes the holdout directory")
    if not isinstance(expected_hash, str) or len(expected_hash) != 64:
        raise HoldoutError(f"Missing SHA-256 for {filename}")
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise HoldoutError(f"Cannot read frozen file: {path}") from exc
    if hashlib.sha256(data).hexdigest() != expected_hash:
        raise HoldoutError(f"SHA-256 mismatch: {filename}")
    return path, data


def _read_predictions(directory: Path, manifest: dict) -> tuple[list[str], list[dict]]:
    files = manifest.get("files")
    if not isinstance(files, dict) or "predictions" not in files:
        raise HoldoutError("Manifest is missing the predictions file")
    if files["predictions"].get("path") != "predictions.csv":
        raise HoldoutError("Prediction path must be predictions.csv inside the holdout directory")
    _, data = _checked_file(directory, files["predictions"])
    try:
        reader = csv.DictReader(data.decode("utf-8").splitlines())
        fields = reader.fieldnames or []
        if len(fields) != len(set(fields)):
            raise HoldoutError("Duplicate prediction column names")
        if OUTCOME_COLUMNS.intersection(fields):
            raise HoldoutError("Prediction file contains outcome columns")
        if "fight_id" not in fields:
            raise HoldoutError("Prediction file is missing fight_id")
        rows = list(reader)
    except (UnicodeError, csv.Error) as exc:
        raise HoldoutError("Cannot parse prediction CSV") from exc
    if any(None in row or None in row.values() for row in rows):
        raise HoldoutError("Malformed prediction CSV row")
    ids = [row["fight_id"] for row in rows]
    if any(not fid or fid != fid.strip() for fid in ids):
        raise HoldoutError("Blank or padded prediction fight_id")
    if len(ids) != len(set(ids)):
        raise HoldoutError("Duplicate prediction fight IDs")
    count = POPULATIONS[manifest["cohort_name"]]["count"]
    if len(ids) != count:
        raise HoldoutError(f"Prediction cohort must contain exactly {count} fights")
    return fields, rows


def _read_identity_exclusions(directory: Path, manifest: dict) -> dict:
    """Check label-free evidence; both populations exclude the entire parent."""
    files = manifest.get("files", {})
    if "identity_exclusions" not in files:
        raise HoldoutError("Manifest is missing identity/exclusion evidence")
    if files["identity_exclusions"].get("path") != "identity-exclusions.json":
        raise HoldoutError("Identity/exclusion path must be identity-exclusions.json inside the holdout directory")
    _, raw = _checked_file(directory, files["identity_exclusions"])
    try:
        evidence = json.loads(raw)
    except (UnicodeError, ValueError) as exc:
        raise HoldoutError("Cannot parse identity/exclusion evidence") from exc

    def check_keys(value):
        if isinstance(value, dict):
            if OUTCOME_COLUMNS.intersection(value):
                raise HoldoutError("Identity/exclusion evidence contains outcome columns")
            for child in value.values():
                check_keys(child)
        elif isinstance(value, list):
            for child in value:
                check_keys(child)
    check_keys(evidence)
    if not isinstance(evidence, dict) or evidence.get("schema_version") != SCHEMA_VERSION:
        raise HoldoutError("Invalid identity/exclusion schema")
    rows = evidence.get("identities")
    if not isinstance(rows, list) or len(rows) != EXPECTED_FIGHTS:
        raise HoldoutError("Identity/exclusion evidence must contain all 146 original IDs")
    ids = [row.get("fight_id") for row in rows]
    if any(not isinstance(fid, str) or not fid or fid != fid.strip() for fid in ids) or len(set(ids)) != len(ids):
        raise HoldoutError("Invalid or duplicate identity/exclusion IDs")
    exclusions = set(ids)
    for row in rows:
        required = {"fight_id", "event_id", "event_date", "fighter_1_id", "fighter_2_id"}
        if any(not isinstance(row.get(key), str) or not row[key].strip() for key in required):
            raise HoldoutError("Missing identity/exclusion fields")
        alias = row.get("alternate_bout")
        if alias is None:
            continue
        if not isinstance(alias, dict) or any(not isinstance(alias.get(key), str) or not alias[key].strip() for key in required):
            raise HoldoutError("Missing alternate bout identity fields")
        original_pair = [row.get("fighter_1_id"), row.get("fighter_2_id")]
        pair = [alias.get("fighter_1_id"), alias.get("fighter_2_id")]
        same = (all(original_pair + pair) and row.get("event_id") == alias.get("event_id")
                and row.get("event_date") == alias.get("event_date")
                and set(original_pair) == set(pair))
        status = "verified_same_bout" if same else "unresolved_identity"
        if alias.get("status") != status:
            raise HoldoutError("Alias identity status differs from event/fighter evidence")
        orientation = original_pair == pair[::-1] if same else None
        if alias.get("orientation_reversed") is not orientation:
            raise HoldoutError("Alias orientation differs from fighter IDs")
        fid = alias.get("fight_id")
        if not isinstance(fid, str) or not fid or fid != fid.strip() or fid in exclusions:
            raise HoldoutError("Invalid or duplicate alternate bout ID")
        # Unverified review targets are precautionary exclusions, not certified aliases.
        exclusions.add(fid)
    if evidence.get("exclusion_ids") != sorted(exclusions):
        raise HoldoutError("Exclusion IDs differ from identity evidence")
    if manifest.get("training_exclusion_count") != len(exclusions):
        raise HoldoutError("Manifest training exclusion count differs")
    return evidence


def load_holdout_fight_ids(holdout_dir: str | Path = DEFAULT_HOLDOUT_DIR) -> frozenset[str]:
    """Return all parent IDs plus verified/precautionary alternate exclusions.

    Passing the pre-event subset still excludes all 146 parent records. Neither
    outcomes nor joined recovery/warehouse evidence is opened or verified.
    """
    directory = Path(holdout_dir)
    manifest = _read_manifest(directory)
    _, rows = _read_predictions(directory, manifest)
    evidence = _read_identity_exclusions(directory, manifest)
    by_id = {row["fight_id"]: row for row in evidence["identities"]}
    if not {row["fight_id"] for row in rows}.issubset(by_id):
        raise HoldoutError("Prediction IDs are absent from identity/exclusion evidence")
    for row in rows:
        if any(row.get(key) != by_id[row["fight_id"]][key] for key in
               ("event_id", "event_date", "fighter_1_id", "fighter_2_id")):
            raise HoldoutError("Prediction identity differs from exclusion evidence")
    return frozenset(evidence["exclusion_ids"])


def assert_no_holdout_fights(training_df, holdout_dir: str | Path = DEFAULT_HOLDOUT_DIR) -> None:
    """Raise if a DataFrame lacks usable IDs or overlaps the frozen holdout.

    IDs may be strings or UUID objects. No rows are filtered or mutated. A missing
    or unaccepted holdout is an error, including for an empty training DataFrame.
    """
    ids = load_holdout_fight_ids(holdout_dir)
    if "fight_id" not in training_df.columns:
        raise HoldoutError("Training DataFrame must contain fight_id")
    column = training_df["fight_id"]
    if column.isna().any():
        raise HoldoutError("Training DataFrame contains null fight IDs")
    values = column.map(str)
    if values.map(lambda value: not value.strip()).any():
        raise HoldoutError("Training DataFrame contains blank fight IDs")
    overlap = sorted(set(values.map(str.strip)) & ids)
    if overlap:
        raise HoldoutError(
            f"Training DataFrame contains {len(overlap)} holdout fight IDs: "
            + ", ".join(overlap)
        )

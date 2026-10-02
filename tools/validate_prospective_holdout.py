"""Offline, read-only validation/evaluation of the Phase 1 frozen holdout.

Run: python3 tools/validate_prospective_holdout.py [--holdout-dir DIRECTORY]
This utility may read outcomes. Training code must only use modeling.holdout.
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modeling.holdout import (
    COHORT_NAME, PRE_EVENT_COHORT, DEFAULT_HOLDOUT_DIR, OUTCOME_COLUMNS,
    HoldoutError, _checked_file, _read_manifest, _read_predictions, _read_identity_exclusions,
)

ROOT = Path(__file__).resolve().parents[1]
SOURCE_BLOB = "e7a6e34bb35e015d91975f182572613ad4d5b48e"
SOURCE_SHA256 = "8b66ee10d706061a739604aaa74a6b71fcd3b02731d534c4a134eb9243c5d5ae"
EVIDENCE_DIR = "docs/implementation-reports/phase1-holdout-evidence"
SOURCE_PATH = f"{EVIDENCE_DIR}/recovered-report-{SOURCE_BLOB}.csv"
MODEL_ARTIFACT = "/home/wlodzimierrr/ufc-data/models/xgb/20260328T221117Z"
EXPECTED_METRICS = {
    "total": (146, 81), "uncertain": (55, 22),
    "actionable": (91, 59), "high_confidence": (44, 30), "strongest_57": (57, 39),
}
PRE_EVENT_METRICS = {
    "total": (108, 57), "uncertain": (46, 17),
    "actionable": (62, 40), "high_confidence": (23, 16),
}
METRIC_DEFINITIONS = {
    "latent_prediction": "calibrated_prob_f1 >= 0.5 predicts fighter_1; otherwise fighter_2",
    "total": "All originally resolved selected records",
    "uncertain": "0.40 <= calibrated_prob_f1 <= 0.60 (inclusive)",
    "actionable": "calibrated_prob_f1 < 0.40 or calibrated_prob_f1 > 0.60",
    "high_confidence": "calibrated_prob_f1 <= 0.30 or calibrated_prob_f1 >= 0.70 (inclusive)",
    "strongest_57": "Historical only: abs(calibrated_prob_f1 - 0.5) descending, fight_id ascending; first 57; selection uses no outcomes",
}
PREDICTION_FIELDS = frozenset({
    "fight_id", "event_id", "event_name", "event_date", "fighter_1_id",
    "fighter_2_id", "fighter_1_name", "fighter_2_name", "scored_at",
    "pre_event_evidence", "prediction_source", "predicted_prob_f1",
    "calibrated_prob_f1", "confidence_tier", "is_uncertain", "model_name",
    "model_artifact",
})
OUTCOME_FIELDS = frozenset({"fight_id", "actual_label", "resolved", "correct"})


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise HoldoutError(message)


def _bool(value: str, field: str) -> bool:
    _require(value in {"True", "False", "true", "false"}, f"Invalid {field}: {value!r}")
    return value.lower() == "true"


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        _require(parsed.tzinfo is not None, "Timestamp must include a timezone")
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError) as exc:
        raise HoldoutError(f"Invalid timestamp: {value!r}") from exc


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise HoldoutError(f"Invalid date: {value!r}") from exc


def _probability(value: str) -> Decimal:
    try:
        result = Decimal(value)
    except (TypeError, InvalidOperation) as exc:
        raise HoldoutError(f"Invalid probability: {value!r}") from exc
    _require(result.is_finite() and 0 <= result <= 1, f"Probability outside [0, 1]: {value!r}")
    return result


def _required_fields(fields: list[str], rows: list[dict], required: frozenset,
                     allow_blank: frozenset = frozenset()) -> None:
    _require(required.issubset(fields), f"Missing required fields: {sorted(required - set(fields))}")
    for row in rows:
        for field in required - allow_blank:
            _require(bool(row.get(field, "").strip()), f"Missing {field} for {row.get('fight_id')}")


def strongest_predictions(rows: list[dict], count: int = 57) -> list[dict]:
    """Rank using probabilities and IDs only; preserve original records."""
    return sorted(rows, key=lambda row: (-abs(_probability(str(row["calibrated_prob_f1"]))
                                             - Decimal(".5")), row["fight_id"]))[:count]


def _label(value: str) -> int:
    try:
        label = Decimal(value)
    except (TypeError, InvalidOperation) as exc:
        raise HoldoutError(f"Invalid actual label: {value!r}") from exc
    _require(label.is_finite() and label in {Decimal(0), Decimal(1)}, "Invalid actual label")
    return int(label)


def _csv_bytes(raw: bytes) -> tuple[list[str], list[dict]]:
    try:
        reader = csv.DictReader(raw.decode("utf-8").splitlines())
        fields, rows = reader.fieldnames or [], list(reader)
    except (UnicodeError, csv.Error) as exc:
        raise HoldoutError("Cannot parse evidence CSV") from exc
    _require(len(fields) == len(set(fields)), "Duplicate evidence column names")
    _require(not any(None in row or None in row.values() for row in rows), "Malformed evidence CSV")
    return fields, rows


def _verify_source_and_identities(directory, manifest, predictions, outcomes, identities):
    """Independently compare frozen records with the exact adopted evidence."""
    sources = {entry["role"]: entry for entry in manifest["sources"]}
    _require(len(manifest["sources"]) == 3 and set(sources) == {"recovered_report", "candidate_provenance", "identity_outcome_audit"},
             "Unexpected source provenance roles")
    _require(sources["candidate_provenance"].get("path") == f"{EVIDENCE_DIR}/candidate-provenance-audit.json"
             and sources["candidate_provenance"].get("sha256") == "bef6d8aa41f98ac267186800e7c7291ea14a971b5daea715b4bf45bfab18d31d"
             and sources["identity_outcome_audit"].get("path") == f"{EVIDENCE_DIR}/phase1b-identity-outcome-audit.json",
             "Unexpected identity/provenance sources")
    report = sources["recovered_report"]
    _require(report.get("path") == SOURCE_PATH and report.get("sha256") == SOURCE_SHA256
             and report.get("git_blob") == SOURCE_BLOB, "Unexpected adopted source")
    raws = {role: _checked_file(ROOT, entry)[1] for role, entry in sources.items()}
    fields, source = _csv_bytes(raws["recovered_report"])
    selected = sorted([r for r in source if "2026-04-01" <= r["event_date"] < "2026-09-01"
                       and r["resolved"] == "True"], key=lambda r: (r["event_date"], r["fight_id"]))
    provenance = json.loads(raws["candidate_provenance"])
    audited = json.loads(raws["identity_outcome_audit"])
    _require(audited["warehouse_evidence"]["transaction"][1:] == ["on", "repeatable read"],
             "Warehouse evidence must come from a read-only repeatable-read transaction")
    from tools.audit_holdout_recovery import audit_adopted_identity
    recomputed = audit_adopted_identity(audited["warehouse_evidence"])
    _require(recomputed == audited["audit"], "Identity/outcome audit differs from captured warehouse evidence")
    _require(identities == recomputed["identity_exclusions"], "Identity/exclusion file differs from audited evidence")
    _require(recomputed["source_sha256"] == SOURCE_SHA256, "Identity audit source differs")
    by_id = {r["fight_id"]: r for r in provenance["rows"]}
    _require(len(selected) == 146 and len({r["fight_id"] for r in selected}) == 146,
             "Adopted source is not 146 unique fights")
    _require(set(by_id) == {r["fight_id"] for r in selected}, "Provenance population differs from source")
    if manifest["cohort_name"] == PRE_EVENT_COHORT:
        selected = [r for r in selected if r["pre_event_evidence"] == "database_scored_at_before_event"
                    and _timestamp(r["scored_at"]).date() < _date(r["event_date"])]
    _require([r["fight_id"] for r in predictions] == [r["fight_id"] for r in selected],
             "Frozen selection/order differs from adopted source")
    _require([r["fight_id"] for r in outcomes] == [r["fight_id"] for r in selected],
             "Outcome order differs from predictions")
    expected_prediction_fields = set(fields) - OUTCOME_COLUMNS | {"fighter_1_id", "fighter_2_id", "prediction_source"}
    _require(set(predictions[0]) == expected_prediction_fields, "Prediction schema differs from adopted source")
    _require(set(outcomes[0]) == set(fields) & OUTCOME_COLUMNS | {"fight_id"}, "Outcome schema differs from adopted source")
    for prediction, outcome, original in zip(predictions, outcomes, selected):
        fid = original["fight_id"]
        for field in fields:
            frozen = outcome if field in OUTCOME_COLUMNS else prediction
            _require(frozen[field] == original[field], f"Original snapshot field changed: {fid}/{field}")
        for side in ("fighter_1_id", "fighter_2_id"):
            _require(prediction[side] == by_id[fid][side], f"Original fighter ID differs: {fid}")
        _require(prediction["prediction_source"] == "adopted_recovered_git_report", "Unexpected prediction source")
    row_audit = {r["fight_id"]: r for r in recomputed["rows"]}
    unresolved = sorted(fid for fid in (r["fight_id"] for r in predictions)
                        if not row_audit[fid]["clean_identity_and_label"])
    _require(manifest["identity_audit"] == {"unresolved_fight_ids": unresolved,
                                           "clean_identity_and_label_count": len(predictions) - len(unresolved),
                                           "clean_evaluation_certified": not unresolved}, "Manifest identity audit differs")
    _require(not (manifest["cohort_name"] == PRE_EVENT_COHORT and unresolved),
             "Pre-event subset is BLOCKED by identity/label uncertainty")
    if manifest["cohort_name"] == PRE_EVENT_COHORT:
        parent = manifest.get("parent")
        _require(isinstance(parent, dict) and parent.get("cohort_name") == COHORT_NAME, "Missing exact parent relationship")
        parent_dir = directory.parent / COHORT_NAME
        _checked_file(directory.parent, {"path": f"{COHORT_NAME}/manifest.json", "sha256": parent["manifest_sha256"]})
        parent_result = validate_holdout(parent_dir)
        _require(parent_result["hashes"]["predictions"] == parent["predictions_sha256"]
                 and parent_result["hashes"]["outcomes"] == parent["outcomes_sha256"], "Parent hashes differ")
        parent_manifest = _read_manifest(parent_dir)
        _, parent_predictions = _read_predictions(parent_dir, parent_manifest)
        _, raw = _checked_file(parent_dir, parent_manifest["files"]["outcomes"])
        _, parent_outcomes = _csv_bytes(raw)
        subset = [r for r in parent_predictions if r["pre_event_evidence"] == "database_scored_at_before_event"
                  and _timestamp(r["scored_at"]).date() < _date(r["event_date"])]
        subset_ids = {r["fight_id"] for r in subset}
        _require(predictions == subset and outcomes == [r for r in parent_outcomes if r["fight_id"] in subset_ids],
                 "Pre-event records are not the exact parent subset")
        _require(parent_manifest["files"]["identity_exclusions"]["sha256"] == manifest["files"]["identity_exclusions"]["sha256"],
                 "Subset must retain full parent training exclusions")
    else:
        _require(manifest.get("parent") is None, "Historical population cannot have a parent")


def validate_holdout(holdout_dir: str | Path = DEFAULT_HOLDOUT_DIR) -> dict:
    """Validate bytes, identities, original records, joined outcomes and metadata."""
    directory = Path(holdout_dir)
    manifest = _read_manifest(directory)
    expected_metrics = EXPECTED_METRICS if manifest["cohort_name"] == COHORT_NAME else PRE_EVENT_METRICS
    required_metadata = {
        "selection_rule", "sources", "event_date_range", "scored_at_range",
        "proposed_training_cutoff", "model_artifact", "metrics", "created_at",
        "provenance_limitations", "prospectivity_audit", "metric_definitions", "identity_audit", "event_count",
    }
    _require(required_metadata.issubset(manifest),
             f"Missing manifest fields: {sorted(required_metadata - set(manifest))}")
    _require(isinstance(manifest["selection_rule"], str) and bool(manifest["selection_rule"].strip()),
             "Selection rule must be documented")
    _require(isinstance(manifest["sources"], list) and bool(manifest["sources"]), "Missing source provenance")
    _require(isinstance(manifest["provenance_limitations"], list), "Provenance limitations must be a list")
    for field in ("metrics", "event_date_range", "scored_at_range", "proposed_training_cutoff", "prospectivity_audit"):
        _require(isinstance(manifest[field], dict), f"Manifest {field} must be an object")
    for field, keys in (("scored_at_range", {"min", "max"}),
                        ("proposed_training_cutoff", {"earliest_relevant_scored_at", "event_date_exclusive_upper_bound", "rationale"})):
        _require(keys.issubset(manifest[field]), f"Missing required {field} fields")
    _timestamp(manifest["created_at"])
    _require(manifest["model_artifact"] == MODEL_ARTIFACT, "Unexpected model artifact")
    _require(manifest["metric_definitions"] == METRIC_DEFINITIONS, "Metric definitions differ from approved contract")

    files = manifest["files"]
    _require("outcomes" in files, "Manifest is missing outcomes file")
    _require(set(files) == {"predictions", "outcomes", "identity_exclusions", "readme"}, "Unexpected frozen file roles")
    hashes, paths = {}, []
    for role, entry in files.items():
        path, data = _checked_file(directory, entry)
        paths.append(path.resolve())
        hashes[role] = hashlib.sha256(data).hexdigest()
    _require(len(paths) == len(set(paths)), "Manifest file roles must use separate files")
    fields, predictions = _read_predictions(directory, manifest)
    identities = _read_identity_exclusions(directory, manifest)
    # IDs can be unavailable in older evidence, but their columns must exist and
    # missing IDs must be explicitly documented in provenance_limitations.
    _required_fields(fields, predictions, PREDICTION_FIELDS, frozenset({"fighter_1_id", "fighter_2_id"}))
    if any(not row[side] for row in predictions for side in ("fighter_1_id", "fighter_2_id")):
        _require(bool(manifest["provenance_limitations"]), "Missing fighter IDs require provenance limitations")
    _, outcome_bytes = _checked_file(directory, files["outcomes"])
    try:
        reader = csv.DictReader(outcome_bytes.decode("utf-8").splitlines())
        outcome_fields = reader.fieldnames or []
        outcomes = list(reader)
    except (UnicodeError, csv.Error) as exc:
        raise HoldoutError("Cannot parse outcomes CSV") from exc
    _require(len(outcome_fields) == len(set(outcome_fields)), "Duplicate outcome column names")
    _require(not any(None in row or None in row.values() for row in outcomes), "Malformed outcomes CSV row")
    _required_fields(outcome_fields, outcomes, OUTCOME_FIELDS, frozenset({"actual_label", "correct"}))
    _require(manifest.get("prediction_columns") == fields and manifest.get("outcome_columns") == outcome_fields,
             "Manifest column schemas differ")
    for role in ("predictions", "outcomes"):
        _require(files[role].get("row_count") == len(predictions), "Manifest file row counts differ")
    outcome_ids = [row["fight_id"] for row in outcomes]
    _require(len(outcome_ids) == len(set(outcome_ids)), "Duplicate outcome fight IDs")
    _require(set(outcome_ids) == {row["fight_id"] for row in predictions},
             "Prediction/outcome join is not one-to-one")
    by_id = {row["fight_id"]: row for row in outcomes}
    totals = {band: {"count": 0, "correct": 0} for band in expected_metrics}
    dates, timestamps = [], []
    audit = {"by_provenance": {}, "before_event_day": [], "on_event_day": [],
             "after_event_day": [], "catchup_or_retroactive": []}
    for row in predictions:
        fid = row["fight_id"]
        event_day, scored = _date(row["event_date"]), _timestamp(row["scored_at"])
        _require(date(2026, 4, 1) <= event_day < date(2026, 9, 1), "Event outside April–August 2026")
        dates.append(event_day)
        timestamps.append(scored)
        _probability(row["predicted_prob_f1"])
        p = _probability(row["calibrated_prob_f1"])
        _bool(row["is_uncertain"], "is_uncertain")
        _require(row["confidence_tier"] in {"high", "medium", "toss-up"}, "Invalid original confidence tier")
        _require(row["model_artifact"] == MODEL_ARTIFACT, f"Unexpected artifact for {fid}")
        outcome = by_id[fid]
        resolved = _bool(outcome["resolved"], "resolved")
        _require(resolved, f"Unresolved fight in required resolved cohort: {fid}")
        correct = (p >= Decimal(".5")) == bool(_label(outcome["actual_label"]))
        _require(_bool(outcome["correct"], "correct") == correct, f"Incorrect latent correctness for {fid}")
        uncertain = Decimal(".4") <= p <= Decimal(".6")
        bands = ["total", "uncertain" if uncertain else "actionable"]
        if p <= Decimal(".3") or p >= Decimal(".7"):
            bands.append("high_confidence")
        for band in bands:
            totals[band]["count"] += 1
            totals[band]["correct"] += int(correct)
        evidence = row["pre_event_evidence"]
        audit["by_provenance"][evidence] = audit["by_provenance"].get(evidence, 0) + 1
        timing = "before_event_day" if scored.date() < event_day else "on_event_day" if scored.date() == event_day else "after_event_day"
        audit[timing].append(fid)
        if "catchup" in evidence.lower() or "retroactive" in evidence.lower():
            audit["catchup_or_retroactive"].append(fid)
    if "strongest_57" in expected_metrics:
        strongest = strongest_predictions(predictions)
        totals["strongest_57"] = {"count": len(strongest), "correct": sum(
            (_probability(r["calibrated_prob_f1"]) >= Decimal(".5")) == bool(_label(by_id[r["fight_id"]]["actual_label"]))
            for r in strongest)}
        margins = sorted((abs(_probability(r["calibrated_prob_f1"]) - Decimal(".5")) for r in predictions), reverse=True)
        boundary = {"weakest_favorite_probability": str(Decimal(".5") + margins[56]),
                    "selection_boundary_tie": margins[56] == margins[57]}
    for key in ("before_event_day", "on_event_day", "after_event_day", "catchup_or_retroactive"):
        audit[key].sort()
    _require(manifest["prospectivity_audit"] == audit, "Manifest prospectivity audit differs from records")
    if audit["on_event_day"] or audit["after_event_day"] or audit["catchup_or_retroactive"]:
        _require(bool(manifest["provenance_limitations"]), "Prospectivity exceptions must be documented")
    timing_counts = {key: len(audit[key]) for key in ("before_event_day", "on_event_day", "after_event_day")}
    expected_timing = (108, 0, 38) if manifest["cohort_name"] == COHORT_NAME else (108, 0, 0)
    _require(tuple(timing_counts.values()) == expected_timing, "Population timing contract differs")
    _require(audit["by_provenance"] == ({"database_scored_at_before_event": 108, "catchup_scored_before_result_load": 38}
                                      if manifest["cohort_name"] == COHORT_NAME else {"database_scored_at_before_event": 108}),
             "Population provenance contract differs")
    _require(set(manifest["metrics"]) == set(expected_metrics), "Unexpected metric groups")
    for band, (count, correct) in expected_metrics.items():
        observed = totals[band]
        _require((observed["count"], observed["correct"]) == (count, correct),
                 f"{band} invariant failed: expected {count}/{correct}, got {observed['count']}/{observed['correct']}")
        observed["accuracy"] = correct / count
        _require(manifest["metrics"].get(band) == observed, f"Manifest {band} metrics differ")
    if "strongest_57" in expected_metrics:
        _require(boundary == {"weakest_favorite_probability": "0.6798", "selection_boundary_tie": False},
                 "Strongest-57 boundary differs from approved source")
        _require(manifest.get("strongest_57_boundary") == boundary, "Manifest strongest-57 boundary differs")
    earliest, latest = min(timestamps), max(timestamps)
    _require(manifest["event_date_range"] == {"min": min(dates).isoformat(), "max": max(dates).isoformat()},
             "Manifest event-date range differs")
    _require(_timestamp(manifest["scored_at_range"]["min"]) == earliest
             and _timestamp(manifest["scored_at_range"]["max"]) == latest, "Manifest scoring range differs")
    cutoff = manifest["proposed_training_cutoff"]
    _require(_timestamp(cutoff["earliest_relevant_scored_at"]) == earliest, "Cutoff evidence differs")
    _require(cutoff["event_date_exclusive_upper_bound"] == "2026-03-31"
             and _date(cutoff["event_date_exclusive_upper_bound"]) <= earliest.date(),
             "Training cutoff must exclude the earliest scoring day and later")
    _require(bool(cutoff.get("rationale")), "Training cutoff rationale is missing")
    _require(manifest["event_count"] == len({r["event_id"] for r in predictions}), "Manifest event count differs")
    _verify_source_and_identities(directory, manifest, predictions, outcomes, identities)
    return {"status": "VALID", "cohort_name": manifest["cohort_name"],
            "hashes": hashes, "metrics": totals, "prospectivity_audit": audit,
            "identity_audit": manifest["identity_audit"], "training_exclusion_count": len(identities["exclusion_ids"]),
            "parent_verified": manifest["cohort_name"] == PRE_EVENT_COHORT}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--holdout-dir", type=Path, default=DEFAULT_HOLDOUT_DIR)
    args = parser.parse_args()
    try:
        result = validate_holdout(args.holdout_dir)
    except (HoldoutError, KeyError, TypeError, AttributeError, ValueError) as exc:
        print(f"HOLDOUT VALIDATION FAILED: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

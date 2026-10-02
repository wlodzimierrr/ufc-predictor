"""Publish the two explicitly approved Phase 1 populations with exclusive writes.

Run once: PGCONNECT_TIMEOUT=5 python3 tools/freeze_phase1_holdouts.py
Never overwrite an existing frozen artifact. No models are loaded or trained.
"""

from __future__ import annotations

from datetime import datetime, timezone
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modeling.holdout import COHORT_NAME, PRE_EVENT_COHORT, OUTCOME_COLUMNS, POPULATIONS
from tools.audit_holdout_recovery import audit_adopted_identity, capture_adopted_warehouse_evidence, summarize
from tools.validate_prospective_holdout import (
    ROOT, SOURCE_PATH, SOURCE_SHA256, SOURCE_BLOB, EVIDENCE_DIR, MODEL_ARTIFACT,
    METRIC_DEFINITIONS, _date, _timestamp, validate_holdout,
)

AUDIT_PATH = f"{EVIDENCE_DIR}/phase1b-identity-outcome-audit.json"
LIMITATIONS = [
    "The recovered source's original analysis session and creation timestamp are unknown; adoption was explicitly authorized on 2026-10-01.",
    "Holdout outcomes were already inspected during recovery and reconciliation; this is not an unseen test set.",
    "Timestamp evidence alone does not prove complete historical feature/result/statistic availability; audit knowledge timestamps before training.",
    "Historical outcomes are retained even where current warehouse state differs; reviewed records corroborate historical labels but are mutable warehouse assertions, not independent official result verification.",
    "Eight catch-up review mappings have different fighter IDs without a non-name identity bridge. Their rows remain historical evidence and are not certified for clean model evaluation; none is in the pre-event subset.",
    "The 146-row historical population includes 38 predictions scored after event day; scored-before-result-load assertions do not prove absence of post-event knowledge leakage.",
]


def json_bytes(value) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")


def csv_bytes(fields, rows) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def exclusive_write(path: Path, data: bytes) -> None:
    """Exclusive creation applies even to failed/partial previous freezes."""
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(0o444)


def build_population(name, source_fields, selected, provenance, audited, created_at, source_entries, parent=None):
    prediction_fields = [f for f in source_fields if f not in OUTCOME_COLUMNS]
    prediction_fields += ["fighter_1_id", "fighter_2_id", "prediction_source"]
    outcome_fields = ["fight_id"] + [f for f in source_fields if f in OUTCOME_COLUMNS]
    predictions, outcomes = [], []
    for original in selected:
        row = {f: original[f] for f in prediction_fields if f in original}
        row.update({f: provenance[original["fight_id"]][f] for f in ("fighter_1_id", "fighter_2_id")})
        row["prediction_source"] = "adopted_recovered_git_report"
        predictions.append(row)
        outcomes.append({f: original[f] for f in outcome_fields})
    audit = {"by_provenance": {}, "before_event_day": [], "on_event_day": [],
             "after_event_day": [], "catchup_or_retroactive": []}
    for row in predictions:
        evidence = row["pre_event_evidence"]
        audit["by_provenance"][evidence] = audit["by_provenance"].get(evidence, 0) + 1
        scored, event = _timestamp(row["scored_at"]).date(), _date(row["event_date"])
        timing = "before_event_day" if scored < event else "on_event_day" if scored == event else "after_event_day"
        audit[timing].append(row["fight_id"])
        if "catchup" in evidence or "retroactive" in evidence:
            audit["catchup_or_retroactive"].append(row["fight_id"])
    for key in audit.keys() - {"by_provenance"}:
        audit[key].sort()
    metrics = summarize(selected)["metrics"]
    if name == PRE_EVENT_COHORT:
        del metrics["strongest_57"]
    for value in metrics.values():
        value["accuracy"] = value["correct"] / value["count"]
    row_audit = {r["fight_id"]: r for r in audited["rows"]}
    unresolved = sorted(r["fight_id"] for r in selected if not row_audit[r["fight_id"]]["clean_identity_and_label"])
    selection_rule = ("Read the exact adopted preserved source; keep 2026-04-01 <= event_date < 2026-09-01 and original resolved == True; no correctness/probability selection."
                      if name == COHORT_NAME else
                      "Select from frozen historical_2026_apr_aug: original pre_event_evidence == database_scored_at_before_event AND UTC scoring date strictly before event_date; preserve every selected record.")
    manifest = {
        "schema_version": 2, "status": "FROZEN", "cohort_name": name,
        "classification": POPULATIONS[name]["classification"],
        "created_at": created_at, "selection_rule": selection_rule, "sources": source_entries,
        "original_analysis_session": None, "original_report_created_at": None,
        "adoption_authorization_date": "2026-10-01", "row_order": ["event_date", "fight_id"],
        "serialization": "UTF-8, LF, csv.DictWriter with declared original field order; JSON sorted keys, indent=2, trailing LF; original scalar strings preserved",
        "prediction_columns": prediction_fields, "outcome_columns": outcome_fields,
        "total_row_count": len(selected), "unique_fight_count": len({r["fight_id"] for r in selected}),
        "event_count": len({r["event_id"] for r in selected}),
        "event_date_range": {"min": min(r["event_date"] for r in selected), "max": max(r["event_date"] for r in selected)},
        "scored_at_range": {"min": min(_timestamp(r["scored_at"]) for r in selected).isoformat(),
                            "max": max(_timestamp(r["scored_at"]) for r in selected).isoformat()},
        "proposed_training_cutoff": {"event_date_exclusive_upper_bound": "2026-03-31",
                                     "earliest_relevant_scored_at": "2026-03-31T12:09:04.077607+00:00",
                                     "rationale": "Exclude the earliest original scoring day and all later event dates; later historical availability audit is required before training."},
        "training_exclusion_count": len(audited["identity_exclusions"]["exclusion_ids"]),
        "model_artifact": MODEL_ARTIFACT, "metrics": metrics, "metric_definitions": METRIC_DEFINITIONS,
        "prospectivity_audit": audit, "provenance_limitations": LIMITATIONS,
        "identity_audit": {"unresolved_fight_ids": unresolved, "clean_identity_and_label_count": len(selected) - len(unresolved),
                           "clean_evaluation_certified": not unresolved},
        "parent": parent,
    }
    if name == COHORT_NAME:
        manifest["strongest_57_boundary"] = {"weakest_favorite_probability": "0.6798", "selection_boundary_tie": False}
    readme = f"""# {name}

Status: FROZEN (historical snapshot acceptance). Classification: `{manifest['classification']}`.

{selection_rule}

{len(selected)} unique fights across {manifest['event_count']} events, {manifest['event_date_range']['min']} through {manifest['event_date_range']['max']}.
UTC timing: {len(audit['before_event_day'])} before event day, {len(audit['on_event_day'])} on event day, {len(audit['after_event_day'])} after event day.
The pre-event subset is the primary population for later original-forecast comparison. Timestamp evidence alone does not establish complete historical feature availability.

`predictions.csv` preserves original prediction strings/precision/timestamps and contains no outcomes. `outcomes.csv` preserves the original resolved outcome snapshot in prediction orientation. Current warehouse outcomes are never substituted.
The manifest distinguishes inclusive threshold high confidence (p <= .30 or p >= .70) from strongest 57 (historical probability-margin ranking, fight_id tie-break).

Identity/label checks corroborate {len(selected) - len(unresolved)} rows; {len(unresolved)} remain uncertified for clean evaluation. A FROZEN historical snapshot is not blanket certification for clean model evaluation. Do not silently drop uncertain rows or treat the historical 146 as wholly prospective.
`identity-exclusions.json` contains IDs and identity evidence only: all 146 originals, 12 verified alternate bout IDs, and 8 precautionary unverified review targets. The subset carries the same full exclusions. Training uses `modeling.holdout` without opening outcomes or joined evidence.

Proposed later-training filter: `event_date < "2026-03-31"`, supported by `2026-03-31T12:09:04.077607+00:00`; audit historical result/statistic availability first.
Original report creation time and analysis session remain unknown. Outcomes were already inspected during recovery; these datasets are not unseen test sets.

Files were created exclusively and made read-only. Do not overwrite accepted artifacts. Validate offline with `python3 tools/validate_prospective_holdout.py --holdout-dir data/holdouts/{name}`.
See `docs/implementation-reports/phase1b-holdout-freeze-report.md` for the full audit and hashes.
"""
    artifacts = {"predictions.csv": csv_bytes(prediction_fields, predictions),
                 "outcomes.csv": csv_bytes(outcome_fields, outcomes),
                 "identity-exclusions.json": json_bytes(audited["identity_exclusions"]),
                 "README.md": readme.encode()}
    roles = {"predictions": "predictions.csv", "outcomes": "outcomes.csv",
             "identity_exclusions": "identity-exclusions.json", "readme": "README.md"}
    manifest["files"] = {role: {"path": filename, "sha256": hashlib.sha256(artifacts[filename]).hexdigest(),
                                "row_count": len(selected) if role in {"predictions", "outcomes"} else None}
                         for role, filename in roles.items()}
    artifacts["manifest.json"] = json_bytes(manifest)
    return artifacts, manifest


def main():
    destination = ROOT / "data/holdouts"
    # Fail before querying or writing if any accepted/partial publication exists.
    for name in (COHORT_NAME, PRE_EVENT_COHORT):
        if (destination / name).exists():
            raise FileExistsError(f"Refusing to overwrite frozen directory: {destination / name}")
    if (ROOT / AUDIT_PATH).exists():
        raise FileExistsError(f"Refusing to overwrite audit evidence: {ROOT / AUDIT_PATH}")
    created_at = datetime.now(timezone.utc).isoformat()
    raw = (ROOT / SOURCE_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != SOURCE_SHA256:
        raise ValueError("Adopted source checksum differs")
    reader = csv.DictReader(io.StringIO(raw.decode()))
    source_fields = reader.fieldnames
    selected = sorted([r for r in reader if "2026-04-01" <= r["event_date"] < "2026-09-01" and r["resolved"] == "True"],
                      key=lambda r: (r["event_date"], r["fight_id"]))
    warehouse_evidence = capture_adopted_warehouse_evidence()
    audited = audit_adopted_identity(warehouse_evidence)
    evidence = {"schema_version": 2, "created_at": created_at, "warehouse_evidence": warehouse_evidence, "audit": audited}
    exclusive_write(ROOT / AUDIT_PATH, json_bytes(evidence))
    provenance_path = f"{EVIDENCE_DIR}/candidate-provenance-audit.json"
    provenance = {r["fight_id"]: r for r in json.loads((ROOT / provenance_path).read_bytes())["rows"]}
    sources = [{"role": role, "path": path, "sha256": hashlib.sha256((ROOT / path).read_bytes()).hexdigest()}
               for role, path in (("recovered_report", SOURCE_PATH), ("candidate_provenance", provenance_path),
                                  ("identity_outcome_audit", AUDIT_PATH))]
    sources[0]["git_blob"] = SOURCE_BLOB
    historical, parent_manifest = build_population(COHORT_NAME, source_fields, selected, provenance, audited, created_at, sources)
    subset = [r for r in selected if r["pre_event_evidence"] == "database_scored_at_before_event"
              and _timestamp(r["scored_at"]).date() < _date(r["event_date"])]
    parent = {"cohort_name": COHORT_NAME, "manifest_sha256": hashlib.sha256(historical["manifest.json"]).hexdigest(),
              "predictions_sha256": parent_manifest["files"]["predictions"]["sha256"],
              "outcomes_sha256": parent_manifest["files"]["outcomes"]["sha256"]}
    pre_event, _ = build_population(PRE_EVENT_COHORT, source_fields, subset, provenance, audited, created_at, sources, parent)
    datasets = {COHORT_NAME: historical, PRE_EVENT_COHORT: pre_event}
    # Validate complete reviewable artifacts in staging before publishing datasets.
    with tempfile.TemporaryDirectory(prefix="phase1b-freeze-") as staging:
        for name, artifacts in datasets.items():
            directory = Path(staging) / name
            directory.mkdir()
            for filename, data in artifacts.items():
                exclusive_write(directory / filename, data)
        for name in datasets:
            validate_holdout(Path(staging) / name)
        for name, artifacts in datasets.items():
            directory = destination / name
            directory.mkdir(exist_ok=False)
            for filename, data in artifacts.items():
                exclusive_write(directory / filename, data)
    print(json.dumps({name: validate_holdout(destination / name) for name in datasets}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

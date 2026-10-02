"""Read-only inventory of Phase 1 evidence; never select or publish a cohort.

Run: python3 tools/audit_holdout_recovery.py [--warehouse]
Output is deterministic JSON for a given source state. Optional warehouse access
uses one read-only repeatable-read transaction. No model is loaded or scored.
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.validate_prospective_holdout import (
    EXPECTED_METRICS, SOURCE_PATH, SOURCE_SHA256, SOURCE_BLOB, strongest_predictions,
)

ROOT = Path(__file__).resolve().parents[1]
REPORT = "data/reports/pre_event_prediction_fights.csv"


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True, stderr=subprocess.DEVNULL)


def _true(value) -> bool:
    return str(value).lower() in {"true", "t", "1"}


def in_window(rows: list[dict]) -> list[dict]:
    return [r for r in rows if "2026-04-01" <= str(r.get("event_date", "")) < "2026-09-01"]


def summarize(rows: list[dict]) -> dict:
    window = in_window(rows)
    resolved = [r for r in window if _true(r.get("resolved"))]
    metrics = {key: {"count": 0, "correct": 0} for key in EXPECTED_METRICS}
    for row in resolved:
        p = Decimal(str(row["calibrated_prob_f1"]))
        uncertain = Decimal(".4") <= p <= Decimal(".6")
        bands = ["total", "uncertain" if uncertain else "actionable"]
        if p <= Decimal(".3") or p >= Decimal(".7"):
            bands.append("high_confidence")
        for band in bands:
            metrics[band]["count"] += 1
            metrics[band]["correct"] += int(_true(row.get("correct")))
    ranked = strongest_predictions(resolved)
    metrics["strongest_57"] = {"count": len(ranked), "correct": sum(_true(r.get("correct")) for r in ranked)}
    return {
        "window_rows": len(window), "unique_fights": len({r["fight_id"] for r in window}),
        "resolved_unique_fights": len({r["fight_id"] for r in resolved}), "metrics": metrics,
        "matches_all_invariants": all(
            (metrics[key]["count"], metrics[key]["correct"]) == pair
            for key, pair in EXPECTED_METRICS.items()
        ),
        "provenance": dict(sorted(Counter(r.get("pre_event_evidence", "unspecified") for r in window).items())),
        "event_counts": dict(sorted(Counter(str(r["event_date"]) for r in window).items())),
    }


def _csv(raw: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(raw))
    first = (reader.fieldnames or [""])[0]
    return [row for row in reader if row.get(first) != first]


def _record(source: str, selection: str, rows: list[dict], raw: bytes | None = None) -> dict:
    result = {"source": source, "selection_rule": selection, **summarize(rows)}
    if raw is not None:
        result["source_sha256"] = hashlib.sha256(raw).hexdigest()
    return result


def _prediction_labels(rows: list[dict], actuals: dict) -> list[dict]:
    result = []
    for original in rows:
        row = dict(original)
        actual = actuals.get(row["fight_id"], {})
        same_pair = (bool(row.get("fighter_1_id") and row.get("fighter_2_id"))
                     and {row["fighter_1_id"], row["fighter_2_id"]}
                     == {actual.get("fighter_1_id"), actual.get("fighter_2_id")})
        if "winner_fighter_id" in actual:
            winner = actual.get("winner_fighter_id")
            label = (1 if winner == row.get("fighter_1_id") else
                     0 if winner == row.get("fighter_2_id") else None) if actual.get("result_type") == "win" else None
        else:
            pair = (actual.get("fighter_1_outcome"), actual.get("fighter_2_outcome"))
            label = 1 if pair == ("W", "L") else 0 if pair == ("L", "W") else None
            if label is not None:
                winner = actual["fighter_1_id" if label else "fighter_2_id"]
                label = 1 if winner == row.get("fighter_1_id") else 0 if winner == row.get("fighter_2_id") else None
        if not same_pair:
            label = None
        row["actual_label"] = label
        row["resolved"] = label is not None
        row["correct"] = (Decimal(str(row["calibrated_prob_f1"])) >= Decimal(".5")) == label if label is not None else None
        result.append(row)
    return result


def bout_identity(original: dict, actual: dict | None) -> dict:
    """Compare event and fighter IDs; names never establish bout identity."""
    pair = [original.get("fighter_1_id"), original.get("fighter_2_id")]
    target = [actual.get("fighter_1_id"), actual.get("fighter_2_id")] if actual else [None, None]
    event_matches = bool(actual and original.get("event_id") == actual.get("event_id")
                         and original.get("event_date") == actual.get("event_date"))
    pair_matches = bool(all(pair + target) and set(pair) == set(target) and len(set(pair)) == 2)
    same = event_matches and pair_matches
    return {"event_matches": event_matches, "unordered_fighter_ids_match": pair_matches,
            "status": "verified_same_bout" if same else "unresolved_identity",
            "orientation_reversed": pair == target[::-1] if same else None}


def audit_adopted_identity(warehouse_evidence: dict) -> dict:
    """Reconcile only the adopted source against captured read-only evidence.

    No broad Git search, no identity inferred from names, no current label
    substituted into the historical report, and no warehouse writes.
    """
    raw = (ROOT / SOURCE_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != SOURCE_SHA256:
        raise ValueError("Adopted source checksum differs")
    source = [r for r in _csv(raw.decode()) if "2026-04-01" <= r["event_date"] < "2026-09-01"
              and r["resolved"] == "True"]
    source.sort(key=lambda r: (r["event_date"], r["fight_id"]))
    provenance = json.loads((ROOT / "docs/implementation-reports/phase1-holdout-evidence/candidate-provenance-audit.json").read_text())
    provenance = {r["fight_id"]: r for r in provenance["rows"]}
    fights = {r["fight_id"]: r for r in warehouse_evidence["fights"]}
    rows, identities = [], []
    for original in source:
        fid = original["fight_id"]
        p = provenance[fid]
        record = dict(original, fighter_1_id=p["fighter_1_id"], fighter_2_id=p["fighter_2_id"])
        history = [r for r in warehouse_evidence["predictions"] if r["fight_id"] == fid
                   and datetime.fromisoformat(r["scored_at"]) == datetime.fromisoformat(original["scored_at"])]
        match_fields = ("event_date", "fighter_1_id", "fighter_2_id", "fighter_1_name", "fighter_2_name",
                        "weight_class", "confidence_tier", "model_name", "model_artifact")
        matches = [r for r in history if all(str(r[k]) == record[k] for k in match_fields)
                   and all(Decimal(str(r[k])) == Decimal(record[k]) for k in ("predicted_prob_f1", "calibrated_prob_f1"))
                   and _true(r["is_uncertain"]) == _true(record["is_uncertain"])]
        prediction_matches = len(matches) == 1
        current = fights.get(fid)
        original_identity = bout_identity(record, current)
        mapped = p.get("reviewed_actual_fight_id") or fid
        actual = fights.get(mapped)
        mapping_identity = bout_identity(record, actual)
        label = int(Decimal(original["actual_label"]))
        winner = record["fighter_1_id"] if label else record["fighter_2_id"]
        reviews = [r for r in warehouse_evidence["reviews"] if r["fight_id"] == fid
                   and r["event_id"] == record["event_id"] and r["event_date"] == record["event_date"]
                   and r["fighter_1_id"] == record["fighter_1_id"] and r["fighter_2_id"] == record["fighter_2_id"]
                   and datetime.fromisoformat(r["scored_at"]) == datetime.fromisoformat(record["scored_at"])
                   and r["actual_label"] == label and (r.get("actual_fight_id") or fid) == mapped
                   and r["result_type"] == "win"]
        current_label = (1 if current["winner_fighter_id"] == record["fighter_1_id"] else
                         0 if current["winner_fighter_id"] == record["fighter_2_id"] else None
                         ) if current and current["result_type"] == "win" else None
        label_evidence = "reviewed_prediction_fights_original_orientation" if reviews else "current_fights_winner_fighter_id"
        label_matches = bool(reviews) or (original_identity["status"] == "verified_same_bout" and current_label == label)
        clean = (prediction_matches and label_matches and original_identity["status"] == "verified_same_bout"
                 and mapping_identity["status"] == "verified_same_bout")
        rows.append({"fight_id": fid, "event_id": record["event_id"], "event_date": record["event_date"],
                     "fighter_1_id": record["fighter_1_id"], "fighter_2_id": record["fighter_2_id"],
                     "source_prediction_record_matches": prediction_matches,
                     "original_bout_identity": original_identity, "reviewed_actual_fight_id": mapped,
                     "mapping_identity": mapping_identity, "historical_actual_label": original["actual_label"],
                     "historical_winner_fighter_id_in_original_orientation": winner,
                     "label_corroborated": label_matches, "label_evidence": label_evidence,
                     "reviewed_at": sorted(r["reviewed_at"] for r in reviews),
                     "current_original_result_type": current["result_type"] if current else None,
                     "current_original_actual_label": current_label,
                     "current_actual_result_type": actual["result_type"] if actual else None,
                     "current_actual_winner_fighter_id": actual["winner_fighter_id"] if actual else None,
                     "historical_current_outcome_discrepancy": current_label != label,
                     "opponent_replacement_established": False,
                     "clean_identity_and_label": clean})
        identity = {k: record[k] for k in ("fight_id", "event_id", "event_date", "fighter_1_id", "fighter_2_id")}
        identity["original_bout"] = {k: current.get(k) if current else None for k in
                                     ("fight_id", "event_id", "event_date", "fighter_1_id", "fighter_2_id")}
        identity["evidence"] = "Original prediction history and read-only fights/events snapshot; names are not identity evidence"
        identity["alternate_bout"] = None
        if mapped != fid:
            identity["alternate_bout"] = {k: actual.get(k) if actual else None for k in
                                          ("fight_id", "event_id", "event_date", "fighter_1_id", "fighter_2_id")}
            identity["alternate_bout"].update(mapping_identity)
            identity["alternate_bout"]["exclusion_basis"] = (
                "verified_alias" if mapping_identity["status"] == "verified_same_bout" else
                "precautionary_review_target_not_certified_as_same_bout")
        identities.append(identity)
    exclusions = sorted({r["fight_id"] for r in identities} |
                        {r["alternate_bout"]["fight_id"] for r in identities if r["alternate_bout"]})
    return {"source_git_blob": SOURCE_BLOB, "source_sha256": SOURCE_SHA256, "rows": rows,
            "summary": {"original_predictions_verified": sum(r["source_prediction_record_matches"] for r in rows),
                        "verified_alternate_bouts": sum(r["reviewed_actual_fight_id"] != r["fight_id"]
                                                        and r["mapping_identity"]["status"] == "verified_same_bout" for r in rows),
                        "unresolved_identity_ids": sorted(r["fight_id"] for r in rows if not r["clean_identity_and_label"]),
                        "orientation_reversed": sum(r["mapping_identity"]["orientation_reversed"] is True for r in rows),
                        "historical_current_discrepancies": sum(r["historical_current_outcome_discrepancy"] for r in rows)},
            "identity_exclusions": {"schema_version": 2, "source_git_blob": SOURCE_BLOB,
                                    "source_sha256": SOURCE_SHA256, "identities": identities,
                                    "exclusion_ids": exclusions}}


def capture_adopted_warehouse_evidence() -> dict:
    """Targeted, read-only audit of the 146 originals and 20 review targets."""
    from warehouse.db import get_connection
    provenance = json.loads((ROOT / "docs/implementation-reports/phase1-holdout-evidence/candidate-provenance-audit.json").read_text())
    ids = sorted({r["fight_id"] for r in provenance["rows"]} |
                 {r["reviewed_actual_fight_id"] for r in provenance["rows"] if r.get("reviewed_actual_fight_id")})
    queries = {
        "fights": "SELECT f.*, e.event_date, e.event_name FROM fights f JOIN events e USING(event_id) WHERE f.fight_id::text = ANY(%s) ORDER BY f.fight_id",
        "predictions": "SELECT * FROM predictions WHERE fight_id::text = ANY(%s) ORDER BY fight_id, scored_at",
        "reviews": "SELECT * FROM reviewed_prediction_fights WHERE fight_id::text = ANY(%s) ORDER BY fight_id, reviewed_at",
    }
    connection = get_connection()
    connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
    result = {"queries": queries, "query_fight_ids": ids}
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '15s'")
            cursor.execute("SHOW timezone")
            result["timezone"] = cursor.fetchone()[0]
            cursor.execute("SELECT transaction_timestamp(), current_setting('transaction_read_only'), current_setting('transaction_isolation')")
            result["transaction"] = cursor.fetchone()
            for key, query in queries.items():
                cursor.execute(query, (ids,))
                fields = [field[0] for field in cursor.description]
                result[key] = [dict(zip(fields, row)) for row in cursor.fetchall()]
            fighter_ids = sorted({str(r[side]) for key in ("fights", "predictions")
                                  for r in result[key] for side in ("fighter_1_id", "fighter_2_id")})
            query = "SELECT fighter_id, full_name, source_url, dob FROM fighters WHERE fighter_id::text = ANY(%s) ORDER BY fighter_id"
            cursor.execute(query, (fighter_ids,))
            fields = [field[0] for field in cursor.description]
            result["fighters"] = [dict(zip(fields, row)) for row in cursor.fetchall()]
            # Do not mutate queries while iterating over it above.
            result["queries"] = dict(queries, fighters=query)
            result["query_fighter_ids"] = fighter_ids
    finally:
        connection.rollback()
        connection.close()
    return json.loads(json.dumps(result, default=str))


def _deduplicate(rows: list[dict], earliest: bool = False) -> list[dict]:
    by_id = {}
    def scored_at(row):
        value = row["scored_at"]
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("Recovery requires timezone-aware scored_at")
        return parsed.astimezone(timezone.utc)
    for row in sorted(rows, key=scored_at, reverse=earliest):
        by_id[row["fight_id"]] = row
    return list(by_id.values())


def _warehouse() -> tuple[dict[str, list[dict]], dict]:
    from warehouse.db import get_connection
    queries = {
        "predictions": "SELECT p.*, f.event_id, e.event_name, f.result_type, f.winner_fighter_id FROM predictions p JOIN fights f USING(fight_id) JOIN events e USING(event_id) ORDER BY p.event_date, p.fight_id, p.scored_at",
        "pre_event_prediction_fights": "SELECT * FROM pre_event_prediction_fights ORDER BY event_date, fight_id",
        "reviewed_prediction_fights": "SELECT * FROM reviewed_prediction_fights ORDER BY event_date, fight_id",
        "reviewed_prediction_events": "SELECT * FROM reviewed_prediction_events ORDER BY event_date, event_name",
        "pre_event_prediction_events": "SELECT * FROM pre_event_prediction_events ORDER BY event_date, event_name",
    }
    conn = get_connection()
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    tables = {}
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '15s'")
            cursor.execute("SHOW timezone")
            database_timezone = cursor.fetchone()[0]
            for table, query in queries.items():
                cursor.execute(query)
                fields = [field[0] for field in cursor.description]
                tables[table] = [dict(zip(fields, row)) for row in cursor.fetchall()]
        return tables, {"access": "read-only repeatable-read", "timezone": database_timezone,
                        "queries": queries, "table_row_counts": {k: len(v) for k, v in tables.items()}}
    finally:
        conn.rollback()
        conn.close()


def audit(warehouse: bool = False) -> dict:
    candidates, examined = [], []
    for relative in (REPORT, "models/pre_event_prediction_fights.csv"):
        path = ROOT / relative
        if path.exists():
            raw = path.read_bytes()
            candidates.append(_record(relative, "April–August 2026; resolved rows", _csv(raw.decode()), raw))
    for commit in _git("log", "--all", "--format=%H", "--", REPORT).splitlines():
        raw = _git("show", f"{commit}:{REPORT}").encode()
        candidates.append(_record(f"git:{commit}:{REPORT}", "April–August 2026; resolved rows", _csv(raw.decode()), raw))
    fsck = _git("fsck", "--no-reflogs", "--unreachable")
    blobs = sorted(line.split()[2] for line in fsck.splitlines() if line.startswith("unreachable blob "))
    for oid in blobs:
        raw = subprocess.check_output(["git", "cat-file", "blob", oid], cwd=ROOT)
        header = raw.splitlines()[0] if raw else b""
        if b"calibrated_prob_f1" in header and b"fight_id" in header:
            candidates.append(_record(f"git-blob:{oid}", "April–August 2026; resolved rows", _csv(raw.decode()), raw))
    examined.append({"source": "git history, refs, reflogs, stash and unreachable objects",
                     "refs": _git("for-each-ref", "--format=%(refname) %(objectname)").splitlines(),
                     "stash": _git("stash", "list").splitlines(), "unreachable_blobs_inspected": len(blobs)})
    actuals = {}
    for row in _csv((ROOT / "data/fights.csv").read_text()):
        if row["fight_id"] not in actuals or row["scraped_at"] >= actuals[row["fight_id"]]["scraped_at"]:
            actuals[row["fight_id"]] = row
    saved = []
    for path in sorted((ROOT / "models/predictions").glob("*/predictions.csv")):
        raw = path.read_bytes()
        rows = _csv(raw.decode())
        examined.append({"source": str(path.relative_to(ROOT)), "rows": len(rows),
                         "window_rows": len(in_window(rows)), "sha256": hashlib.sha256(raw).hexdigest(),
                         "scored_at_values": sorted({r["scored_at"] for r in rows})})
        saved.extend(rows)
    saved = _prediction_labels(_deduplicate(saved), actuals)
    for timing, rows in [("all saved", saved),
                         ("scored before event day", [r for r in saved if r["scored_at"][:10] < r["event_date"]]),
                         ("scored on/after event day", [r for r in saved if r["scored_at"][:10] >= r["event_date"]])]:
        candidates.append(_record("models/predictions/*/predictions.csv + data/fights.csv", timing, rows))
    for path in sorted((ROOT / "data/reports").rglob("*.csv")) + [
        ROOT / "models/prediction_log.csv", ROOT / "models/pre_event_prediction_events.csv",
        ROOT / "models/backtests/past_event_predictions.csv", ROOT / "models/backtests/past_event_summary.csv",
    ]:
        raw = path.read_bytes()
        rows = _csv(raw.decode())
        examined.append({"source": str(path.relative_to(ROOT)), "rows": len(rows),
                         "window_rows": len(in_window(rows)), "sha256": hashlib.sha256(raw).hexdigest(),
                         "columns": list(rows[0]) if rows else []})
    for path in sorted((ROOT / "notebooks").glob("*.ipynb")):
        notebook = json.loads(path.read_text())
        examined.append({"source": str(path.relative_to(ROOT)),
                         "cells_with_outputs": sum(bool(c.get("outputs")) for c in notebook["cells"]),
                         "original_analysis_146_reference": any("146" in "".join(c.get("source", [])) for c in notebook["cells"])})
    database = None
    if warehouse:
        tables, database = _warehouse()
        for name in ("pre_event_prediction_fights", "reviewed_prediction_fights"):
            candidates.append(_record(f"warehouse:{name}", "April–August 2026; resolved rows",
                                      [dict(r, resolved=True) for r in tables[name]] if name.startswith("reviewed") else tables[name]))
        history = tables["predictions"]
        for timing, rows in [("all scoring timestamps", history),
                             ("scored before event day", [r for r in history if str(r["scored_at"])[:10] < str(r["event_date"])])]:
            for earliest in (True, False):
                selected = _deduplicate(rows, earliest)
                labeled = _prediction_labels(selected, {r["fight_id"]: r for r in selected})
                candidates.append(_record("warehouse:predictions + fights + events",
                                          timing + ("; earliest per fight" if earliest else "; latest per fight"), labeled))
        # A provenance-based reconciliation rule, not a search for target metrics:
        # restore explicitly reviewed results when current strict rows lost them.
        reconciled = {r["fight_id"]: r for r in tables["pre_event_prediction_fights"]}
        for row in tables["reviewed_prediction_fights"]:
            reconciled[row["fight_id"]] = dict(row, resolved=True)
        candidates.append(_record("warehouse:view with reviewed outcomes restored",
                                  "April–August 2026; reviewed rows take precedence by fight_id", list(reconciled.values())))
        candidates.append(_record("warehouse:view with reviewed outcomes restored",
                                  "April 1 through August 22; reviewed rows take precedence by fight_id",
                                  [r for r in reconciled.values() if str(r["event_date"]) <= "2026-08-22"]))
    return {"status": "EVIDENCE_ONLY", "required_invariants": EXPECTED_METRICS,
            "candidates": candidates, "examined_sources": examined, "warehouse": database}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", action="store_true")
    parser.add_argument("--adopted-source-only", action="store_true",
                        help="Audit the approved source and its identities only; skip forensic search")
    args = parser.parse_args()
    if args.adopted_source_only:
        if not args.warehouse:
            parser.error("--adopted-source-only requires --warehouse")
        evidence = capture_adopted_warehouse_evidence()
        result = {"warehouse_evidence": evidence, "audit": audit_adopted_identity(evidence)}
    else:
        result = audit(args.warehouse)
    print(json.dumps(result, default=str, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

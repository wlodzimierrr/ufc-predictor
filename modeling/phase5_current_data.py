"""Versioned Phase 5 current preparation; no model/prior fitting or scoring.

This is deliberately independent of the pinned Phase 3B loader and trainer.
Source results are independent contemporary observations, not frozen outcomes.
"""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
import importlib.metadata
import io
import json
import os
from pathlib import Path
import platform
from uuid import UUID

import numpy as np
import pandas as pd

from features.data_loader import WarehouseData
from features.replay import reconstruct_bouts
from modeling.holdout import assert_no_holdout_fights, load_holdout_fight_ids
from modeling.prospective_registry import registration_records
from modeling.refit_preflight import (
    ALGORITHM_VERSION, DEBUT_COLS, FEATURE_ORDER, FEATURE_VERSION, ROOT,
    PreflightError, json_bytes, sha256, validate_source, validate_training_frame,
)

VERSION = "phase5_current_candidate_v1"
OUTPUT_ROOT = ROOT / "data/experiments/phase5a_prospective_shadow"
META = ["fight_id", "event_id", "fighter_1_id", "fighter_2_id", "event_date",
        "weight_class", "label", "feature_version"]
TABLES = {"events": "event_id", "fighters": "fighter_id", "fights": "fight_id",
          "fight_stats_aggregate": "fight_stat_id"}
SCHEMA_QUERY = """SELECT table_schema, table_name, column_name, ordinal_position,
data_type, udt_name, is_nullable, column_default
FROM information_schema.columns WHERE table_schema = 'public'
AND table_name = ANY(%s) ORDER BY table_name, ordinal_position"""
REFERENCE_QUERY = """SELECT bf.*, f.event_id::text AS source_event_id,
f.fighter_1_id::text AS source_fighter_1_id, f.fighter_2_id::text AS source_fighter_2_id,
f.result_type AS source_result_type, f.winner_fighter_id::text AS source_winner_fighter_id,
e.event_date AS source_event_date, f.source_url AS source_fight_url,
f.scraped_at AS source_fight_scraped_at, e.scraped_at AS source_event_scraped_at
FROM public.bout_features bf LEFT JOIN public.fights f ON f.fight_id = bf.fight_id
LEFT JOIN public.events e ON e.event_id = f.event_id
WHERE bf.event_date < %s AND bf.label IS NOT NULL
AND NOT (bf.fight_id::text = ANY(%s)) ORDER BY bf.event_date, bf.fight_id"""
CODE_PATHS = ["modeling/phase5_current_data.py", "modeling/prospective_registry.py",
              "tools/prepare_phase5a_prospective_shadow.py", "features/data_loader.py",
              "tools/append_prospective_registry.py",
              "features/replay.py", "features/elo.py", "features/history.py",
              "features/snapshot.py", "features/pipeline.py", "features/career.py",
              "features/opponent.py", "features/rolling.py", "features/decay.py",
              "features/physical.py", "features/debut_prior.py", "features/forecast_replay.py",
              "modeling/data.py", "modeling/holdout.py", "modeling/refit_preflight.py",
              "modeling/score_upcoming.py", "modeling/calibrate.py"]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def encoded(value):
    """Lossless SQL scalars as JSON values; schema preserves numeric/date types."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    raise TypeError(f"Unsupported capture scalar: {type(value).__name__}")


def table_bytes(rows: list[dict]) -> bytes:
    return (json.dumps(rows, default=encoded, sort_keys=True, separators=(",", ":"),
                       allow_nan=False) + "\n").encode()


def validate_config(config: dict) -> None:
    canonical = json.loads((ROOT / "configs/phase5_prospective_shadow_v1.json").read_bytes())
    if config != canonical or config["contract_version"] != VERSION:
        raise PreflightError("Prospective configuration differs from preregistration")


def capture_warehouse(connection_factory, metadata: dict, exclusions: frozenset[str]) -> tuple[dict, dict]:
    """Single verified read-only repeatable-read transaction, always rollback/close.

    No local publication takes place here. Connection errors are never formatted
    because driver diagnostics can contain credentials or connection strings.
    """
    if metadata.get("feature_cols") != FEATURE_ORDER or metadata.get("feature_version") != FEATURE_VERSION:
        raise PreflightError("Unexpected production reference feature contract")
    val_date, test_date = metadata.get("val_date"), metadata.get("test_date")
    if not val_date or not test_date or date.fromisoformat(val_date) >= date.fromisoformat(test_date):
        raise PreflightError("Missing/invalid recorded production boundaries")
    if len(exclusions) != 166:
        raise PreflightError("All 166 exclusions are required before capture")
    receipt = {"capture_started_at": now(), "queries": [], "transaction_closed": False}
    conn = connection_factory()
    payloads = {}
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        with conn.cursor() as cur:
            def query(sql, params=None):
                started = now()
                cur.execute(sql, params) if params is not None else cur.execute(sql)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
                receipt["queries"].append({"sql": sql, "parameters": params,
                    "started_at": started, "finished_at": now(), "rows": len(rows),
                    "columns": columns})
                return [dict(zip(columns, row)) for row in rows]
            readonly = query("SHOW transaction_read_only")[0]["transaction_read_only"]
            receipt["transaction_read_only"] = readonly
            if readonly != "on":
                raise PreflightError("Read-only verification failed before any source read/publication")
            isolation = query("SHOW transaction_isolation")[0]["transaction_isolation"]
            if isolation != "repeatable read":
                raise PreflightError("Repeatable-read verification failed")
            receipt["transaction_isolation"] = isolation
            clock = query("SELECT transaction_timestamp() AS transaction_started_at, "
                          "clock_timestamp() AS database_capture_start, "
                          "current_setting('TimeZone') AS database_timezone, "
                          "pg_current_snapshot()::text AS transaction_snapshot")[0]
            receipt.update(clock)
            schemas = query(SCHEMA_QUERY, (list(TABLES) + ["bout_features"],))
            payloads["sources/schemas.json"] = table_bytes(schemas)
            if not all(any(s["table_name"] == t for s in schemas) for t in TABLES):
                raise PreflightError("Required source table/schema missing")
            for table, key in TABLES.items():
                rows = query(f'SELECT * FROM public."{table}" ORDER BY "{key}"')
                payloads[f"sources/{table}.json"] = table_bytes(rows)
            # Exclusions and the production test boundary precede label retrieval.
            reference = query(REFERENCE_QUERY, (date.fromisoformat(test_date), sorted(exclusions)))
            payloads["reference_bootstrap/stored_features.json"] = table_bytes(reference)
            receipt.update(query("SELECT clock_timestamp() AS database_capture_end")[0])
    finally:
        try:
            conn.rollback()
            receipt["rolled_back"] = True
        finally:
            conn.close()
            receipt["transaction_closed"] = True
            receipt["capture_finished_at"] = now()
    receipt = json.loads(table_bytes(receipt))
    payloads["source_capture_receipt.json"] = json_bytes(receipt)
    return payloads, receipt


def source_data(payloads: dict[str, bytes]) -> WarehouseData:
    rows = {name: json.loads(payloads[f"sources/{name}.json"]) for name in TABLES}
    schemas = json.loads(payloads["sources/schemas.json"])
    for table, values in rows.items():
        types = {s["column_name"]: s["data_type"] for s in schemas if s["table_name"] == table}
        for row in values:
            for key, value in row.items():
                if value is None:
                    continue
                if types.get(key) == "date":
                    row[key] = date.fromisoformat(value)
                elif types.get(key) in {"numeric", "double precision", "real"}:
                    row[key] = float(value)
                elif "timestamp" in types.get(key, ""):
                    row[key] = datetime.fromisoformat(value)
    return WarehouseData(events=rows["events"], fighters=rows["fighters"], fights=rows["fights"],
                         fight_stats=rows["fight_stats_aggregate"])


def structural_issues(data: WarehouseData, capture_date: date) -> list[dict]:
    """Record defects without deleting fights or repairing labels/values."""
    issues = []
    try:
        validate_source(data)
    except (ValueError, KeyError, TypeError) as exc:
        issues.append({"scope": "source", "reason": str(exc)})
    events = {e.get("event_id"): e for e in data.events}
    seen_pairs = set()
    for f in data.fights:
        pair = (f.get("event_id"), *sorted(str(f.get(k)) for k in ("fighter_1_id", "fighter_2_id")))
        # Early tournament rematches are legitimate only if the source has distinct
        # bouts; current fitting schema disallows ambiguous same-event pairs.
        if pair in seen_pairs:
            issues.append({"fight_id": f.get("fight_id"), "reason": "duplicate_event_participant_pair"})
        seen_pairs.add(pair)
        if f.get("result_type") in {"draw", "nc", "upcoming"} and f.get("winner_fighter_id") is not None:
            issues.append({"fight_id": f.get("fight_id"), "reason": "nonbinary_result_has_winner"})
        if type(f.get("is_title_fight")) is not bool:
            issues.append({"fight_id": f.get("fight_id"), "reason": "missing_or_invalid_stored_title_flag"})
        # Early bouts had longer rounds. A >300-second observed finish is not
        # invalid merely because modern rounds are five minutes.
        for key, maximum in (("finish_round", None), ("finish_time_seconds", None)):
            value = f.get(key)
            if value is not None and (type(value) is not int or value < (1 if key == "finish_round" else 0)
                                      or (maximum is not None and value > maximum)):
                issues.append({"fight_id": f.get("fight_id"), "reason": f"invalid_{key}"})
        e = events.get(f.get("event_id"))
        if e and f.get("result_type") in {"win", "draw", "nc"} and isinstance(e.get("event_date"), date):
            if e["event_date"] > capture_date:
                issues.append({"fight_id": f.get("fight_id"), "reason": "resolved_future_bout"})
    for fighter in data.fighters:
        for key in ("height_cm", "reach_cm", "weight_lbs"):
            value = fighter.get(key)
            if value is not None and (not np.isfinite(float(value)) or float(value) <= 0):
                issues.append({"fighter_id": fighter["fighter_id"], "reason": f"invalid_{key}"})
    for s in data.fight_stats:
        for key, value in s.items():
            if key in {"fight_stat_id", "fight_id", "fighter_id", "scraped_at", "source_url"}:
                continue
            if value is not None and (not isinstance(value, (int, float, Decimal)) or
                                      not np.isfinite(float(value)) or float(value) < 0):
                issues.append({"fight_stat_id": s.get("fight_stat_id"), "reason": f"invalid_stat_{key}"})
        for key in s:
            if key.endswith("_landed"):
                attempts = s.get(key.replace("_landed", "_attempted"))
                landed = s[key]
                if landed is not None and attempts is not None and landed > attempts:
                    issues.append({"fight_stat_id": s.get("fight_stat_id"), "reason": f"landed_exceeds_attempted_{key}"})
    return issues


def prepare_current(data: WarehouseData, cutoff: str) -> tuple[pd.DataFrame, dict, list[dict]]:
    data = deepcopy(data)
    issues = structural_issues(data, date.fromisoformat(cutoff))
    # Ambiguous distinct bout IDs still have well-formed source rows. Preserve
    # all of them in a diagnostic reconstruction, never a fit-ready dataset.
    # This does not coalesce identities, choose an alias or remove histories.
    reconstructable_block = bool(issues) and all(i["reason"] == "duplicate_event_participant_pair" for i in issues)
    exclusions = load_holdout_fight_ids()
    if len(exclusions) != 166:
        raise PreflightError("Exclusion contract changed")
    cutoff_date = date.fromisoformat(cutoff)
    events = {r["event_id"]: r for r in data.events}
    eligible, histories, records = [], [], []
    for fight in data.fights:
        event_date = events.get(fight.get("event_id"), {}).get("event_date")
        reasons = []
        if not isinstance(event_date, date):
            reasons.append("invalid_event_date_or_join")
        elif event_date >= cutoff_date:
            reasons.append("event_on_or_after_exclusive_cutoff")
        if fight["fight_id"] in exclusions:
            reasons.append("166_id_fitting_exclusion")
        result = fight.get("result_type")
        if result != "win":
            reasons.append(f"nonbinary_or_unresolved_{result}")
        reconstruction_eligible = not reasons
        if issues:
            reasons.append("structural_defects_block_entire_preparation")
        records.append({"fight_id": fight.get("fight_id"), "event_id": fight.get("event_id"),
            "event_date": event_date.isoformat() if isinstance(event_date, date) else None,
            "fighter_1_id": fight.get("fighter_1_id"), "fighter_2_id": fight.get("fighter_2_id"),
            "eligible": not reasons, "chronology_binary_exclusion_eligible": reconstruction_eligible,
            "reasons": reasons})
        # Independently captured excluded results may inform later history. No
        # exclusion label reaches the returned fitting targets.
        if isinstance(event_date, date) and event_date < cutoff_date and result in {"win", "draw", "nc"}:
            fight["scheduled_rounds"] = None
            fr, ft = fight.get("finish_round"), fight.get("finish_time_seconds")
            fight["elapsed_duration_seconds"] = (fr - 1) * 300 + ft if fr is not None and ft is not None else None
            histories.append(fight)
        if reconstruction_eligible:
            eligible.append(fight)
    if issues and not reconstructable_block:
        return pd.DataFrame(columns=META + FEATURE_ORDER), {"ready": False, "structural_issues": issues}, records
    historical = WarehouseData(fights=histories, fighter_by_id=data.fighter_by_id,
                               stats_by_fight_fighter=data.stats_by_fight_fighter)
    frame = pd.DataFrame(reconstruct_bouts(historical, eligible))
    if frame.empty:
        return pd.DataFrame(columns=META + FEATURE_ORDER), {"ready": False, "structural_issues": [{"reason": "no_eligible_targets"}]}, records
    for col in FEATURE_ORDER:
        frame[col] = np.nan if col in DEBUT_COLS else pd.to_numeric(frame[col], errors="raise").astype(float)
    frame["event_date"] = pd.to_datetime(frame.event_date)
    frame = frame[META + FEATURE_ORDER].sort_values(["event_date", "fight_id"]).reset_index(drop=True)
    validate_current_frame(frame, cutoff, permit_diagnostic_blocked=reconstructable_block)
    expected = {f["fight_id"]: f for f in eligible}
    for row in frame.to_dict("records"):
        f = expected[row["fight_id"]]
        if any(row[k] != f[k] for k in ("event_id", "fighter_1_id", "fighter_2_id")) or row["label"] != int(f["winner_fighter_id"] == f["fighter_1_id"]):
            raise PreflightError("Reconstruction changed identities/orientation/labels")
    excluded_history = [f["fight_id"] for f in histories if f["fight_id"] in exclusions]
    summary = {"ready": not issues, "structural_issues": issues, "source_fights": len(data.fights),
        "eligible_rows": len(frame), "event_count": int(frame.event_id.nunique()),
        "fitting_eligible_rows": 0 if issues else len(frame),
        "dataset_role": "blocked_diagnostic_reconstruction" if issues else "current_challenger_training_preparation",
        "history_identity_conflicts_repaired": False,
        "date_count": int(frame.event_date.nunique()),
        "earliest_included_event": frame.event_date.min().date().isoformat(),
        "latest_included_event": frame.event_date.max().date().isoformat(),
        "label_counts": {str(k): int(v) for k, v in frame.label.value_counts().sort_index().items()},
        "reason_counts_overlapping": dict(Counter(reason for r in records for reason in r["reasons"])),
        "fitting_exclusion_ids": sorted(exclusions), "fitting_exclusion_count": len(exclusions),
        "independent_excluded_results_in_history": sorted(excluded_history),
        "history_rows": len(histories), "draw_history_rows": sum(f["result_type"] == "draw" for f in histories),
        "nc_history_rows": sum(f["result_type"] == "nc" for f in histories),
        "resolved_fights_missing_participant_stats": sum(any((f["fight_id"], fid) not in data.stats_by_fight_fighter for fid in (f["fighter_1_id"], f["fighter_2_id"])) for f in histories),
        "both_debuting_rows": int(frame.both_debuting.eq(1).sum()),
        "missingness": {c: int(frame[c].isna().sum()) for c in FEATURE_ORDER},
        "excluded_labels_never_fitting_targets": True, "sparse_rows_preserved": True}
    return frame, summary, records


def validate_current_frame(frame: pd.DataFrame, cutoff: str, *, permit_diagnostic_blocked=False) -> None:
    """Strict fitting validation; diagnostic mode only tolerates the logged pair defect.

    Labels, numeric values, identities, chronology, schema and exclusions still
    pass full validation. The old preparation/trainer guards are unchanged.
    """
    assert_no_holdout_fights(frame)
    if list(frame.columns) != META + FEATURE_ORDER:
        raise PreflightError("Current exact ordered schema mismatch")
    for col in FEATURE_ORDER:
        values = pd.to_numeric(frame[col], errors="raise").to_numpy(dtype=float)
        if np.isinf(values).any():
            raise PreflightError("Infinite current feature: " + col)
    try:
        validate_training_frame(frame, event_cutoff=cutoff)
    except PreflightError as exc:
        if not permit_diagnostic_blocked or str(exc) != "Duplicate bout identity/self matchup" or frame.fighter_1_id.eq(frame.fighter_2_id).any():
            raise


def current_folds(frame: pd.DataFrame, cutoff: str, *, permit_diagnostic_blocked=False) -> dict:
    validate_current_frame(frame, cutoff, permit_diagnostic_blocked=permit_diagnostic_blocked)
    boundaries = [(pd.Timestamp(cutoff) - pd.DateOffset(months=m)).date().isoformat()
                  for m in (12, 9, 6, 3, 0)]
    folds, coverage = [], []
    for i, (start, end) in enumerate(zip(boundaries[:-1], boundaries[1:]), 1):
        train = frame[frame.event_date < pd.Timestamp(start)]
        prediction = frame[(frame.event_date >= pd.Timestamp(start)) & (frame.event_date < pd.Timestamp(end))]
        train_ids, pred_ids = sorted(train.fight_id), sorted(prediction.fight_id)
        if set(train.event_id) & set(prediction.event_id) or set(train.event_date) & set(prediction.event_date):
            raise PreflightError("OOF partitions split event/date")
        folds.append({"name": f"oof_{i}", "train_end_exclusive": start,
            "prediction_start_inclusive": start, "prediction_end_exclusive": end,
            "train_fight_ids": train_ids, "prediction_fight_ids": pred_ids,
            "train_rows": len(train_ids), "prediction_rows": len(pred_ids),
            "train_ids_sha256": sha256(json_bytes(train_ids)), "prediction_ids_sha256": sha256(json_bytes(pred_ids)),
            "preprocessing_fit_partition": "train_only", "boosting_rounds": 310,
            "early_stopping": False, "tuning": False,
            "ready": bool(train_ids and pred_ids) and not permit_diagnostic_blocked})
        coverage.extend(pred_ids)
    expected = sorted(frame.loc[frame.event_date >= pd.Timestamp(boundaries[0]), "fight_id"])
    if sorted(coverage) != expected or len(set(coverage)) != len(coverage):
        raise PreflightError("Missing/duplicate OOF memberships")
    return {"contract_version": VERSION, "boundaries": boundaries, "folds": folds,
        "membership_stage": "chronology_binary_166_exclusion_guard_before_structural_fitting_gate",
        "diagnostic_only": permit_diagnostic_blocked,
        "ready": all(f["ready"] for f in folds), "oof_rows": len(coverage),
        "oof_ids_sha256": sha256(json_bytes(expected)), "final_training_rows": len(frame),
        "final_training_ids_sha256": sha256(json_bytes(sorted(frame.fight_id)))}


def reference_preparation(payloads: dict, metadata: dict) -> dict[str, bytes]:
    rows = json.loads(payloads["reference_bootstrap/stored_features.json"])
    frame = pd.DataFrame(rows)
    exclusions = load_holdout_fight_ids()
    issues = []
    if frame.empty:
        issues.append({"reason": "no_reference_bootstrap_rows"})
    else:
        assert_no_holdout_fights(frame)
        if frame.fight_id.duplicated().any():
            issues.append({"reason": "duplicate_reference_identity"})
        for r in rows:
            reasons = []
            if r.get("label") not in (0, 1):
                reasons.append("invalid_binary_label")
            if not r["event_date"] < metadata["test_date"]:
                reasons.append("reference_date_restriction_failed")
            for key in ("fighter_1_id", "fighter_2_id", "event_date"):
                if r.get(key) != r.get("source_" + key):
                    reasons.append(f"source_join_mismatch_{key}")
            if r.get("source_result_type") != "win" or r.get("source_winner_fighter_id") not in (r.get("fighter_1_id"), r.get("fighter_2_id")) or r.get("label") != int(r.get("source_winner_fighter_id") == r.get("fighter_1_id")):
                reasons.append("source_label_mismatch")
            if r.get("feature_version") != metadata["feature_version"] or not r.get("computed_at"):
                reasons.append("missing_or_mismatched_feature_computation_metadata")
            for col in FEATURE_ORDER[:-3]:
                value = r.get(col)
                if col not in r:
                    reasons.append(f"missing_feature_column_{col}")
                elif value is not None:
                    try:
                        if not np.isfinite(float(value)):
                            reasons.append(f"nonfinite_{col}")
                    except (ValueError, TypeError):
                        reasons.append(f"nonnumeric_{col}")
            if reasons:
                issues.append({"fight_id": r["fight_id"], "reasons": reasons})
    preprocessing = [r for r in rows if r["event_date"] < metadata["val_date"]]
    calibration = [r for r in rows if metadata["val_date"] <= r["event_date"] < metadata["test_date"]]
    if not preprocessing or {r["label"] for r in calibration} != {0, 1}:
        issues.append({"reason": "missing_preprocessing_or_two_class_calibration"})
    manifest = {"contract_version": "production_derived_reference_v1", "ready": not issues,
        "structural_issues": issues, "val_date": metadata["val_date"], "test_date": metadata["test_date"],
        "feature_order": FEATURE_ORDER, "base_learner_unchanged": True,
        "legacy_stored_features_only": True, "historical_replay_certified": False,
        "preprocessing_rows": len(preprocessing), "calibration_rows": len(calibration),
        "original_recorded_train_rows": metadata["train_rows"], "original_recorded_val_rows": metadata["val_rows"],
        "preprocessing_ids_sha256": sha256(json_bytes(sorted(r["fight_id"] for r in preprocessing))),
        "calibration_ids_sha256": sha256(json_bytes(sorted(r["fight_id"] for r in calibration))),
        "exclusion_ids": sorted(exclusions),
        "recipe": "compute_debut_priors on pre-val eligible rows; apply to calibration; existing learner predict_proba in Phase5B; persist one Platt estimator",
        "fit_executed": False, "probabilities_computed": False,
        "inherited_limitations": ["current stored features not original archived calibration inputs",
            "no persisted production scoring-time Platt estimator", "legacy finish-round schedules may survive",
            "legacy within-date Elo/order and database rounding", "metadata/boolean/statistic defaults",
            "historical pre-bout availability unverified", "exclusions cannot undo legacy learner historical fitting",
            "future legacy feature pipeline must be separately pinned before scoring"]}
    return {"reference_bootstrap/preprocessing_rows.json": table_bytes(preprocessing),
            "reference_bootstrap/calibration_rows.json": table_bytes(calibration),
            "reference_bootstrap/manifest.json": json_bytes(manifest)}


def csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False, date_format="%Y-%m-%d", lineterminator="\n", float_format="%.17g").encode()


def build_payloads(source_payloads: dict[str, bytes], metadata: dict, config: dict,
                   capture_completed_at: str) -> dict[str, bytes]:
    validate_config(config)
    cutoff = datetime.fromisoformat(capture_completed_at).astimezone(timezone.utc).date().isoformat()
    data = source_data(source_payloads)
    frame, summary, eligibility_rows = prepare_current(data, cutoff)
    folds = current_folds(frame, cutoff, permit_diagnostic_blocked=not summary["ready"]) if not frame.empty else {"contract_version": VERSION, "ready": False, "folds": []}
    summary["ready"] = summary["ready"] and folds["ready"]
    manifest = {"contract_version": VERSION, "experiment_id": config["experiment_id"],
        "event_cutoff_exclusive": cutoff, "capture_completed_at": capture_completed_at,
        "source_mode": config["source_mode"], "certification_status": "retrospective_reconstruction_only",
        "per_bout_availability": "unverified", "feature_algorithm_version": ALGORITHM_VERSION,
        "feature_version": FEATURE_VERSION, "feature_order": FEATURE_ORDER,
        "orientation": "captured fighter_1 minus fighter_2; label 1 means fighter_1 wins",
        "summary": summary, "training_sha256": sha256(csv_bytes(frame)),
        "fitting_readiness_gate": "blocked rows/memberships are diagnostic only; Phase5B loader refuses them",
        "folds_sha256": sha256(json_bytes(folds)), "config_sha256": sha256(json_bytes(config)),
        "source_sha256": {n: sha256(b) for n, b in sorted(source_payloads.items())},
        "no_model_prior_or_calibration_fitting": True}
    future = []
    events = {e["event_id"]: e for e in data.events}
    profiles = {f["fighter_id"] for f in data.fighters}
    for f in sorted(data.fights, key=lambda r: r["fight_id"]):
        event = events.get(f.get("event_id"), {})
        d = event.get("event_date")
        if isinstance(d, date) and d.isoformat() >= cutoff:
            future.append({**{k: f.get(k) for k in ("fight_id", "event_id", "fighter_1_id", "fighter_2_id", "weight_class", "source_url")},
                "announced_event_date": d.isoformat(), "is_title_fight": None,
                "title_evidence": None, "warehouse_stored_title_flag": f.get("is_title_fight"),
                "source_scraped_at": encoded(f["scraped_at"]) if f.get("scraped_at") else None,
                "fighter_1_profile_present": f.get("fighter_1_id") in profiles,
                "fighter_2_profile_present": f.get("fighter_2_id") in profiles,
                "fighter_1_experience_status": "unverified", "fighter_2_experience_status": "unverified",
                "fighter_1_experience_evidence": None, "fighter_2_experience_evidence": None})
    registrations = registration_records(future, captured_at=capture_completed_at,
                                          source_sha256=sha256(source_payloads["sources/fights.json"]))
    payloads = {"training.csv": csv_bytes(frame), "training_manifest.json": json_bytes(manifest),
        "eligibility.json": json_bytes(eligibility_rows), "folds.json": json_bytes(folds),
        "effective_configuration.json": json_bytes(config),
        "feature_contract.json": json_bytes({"contract_version": VERSION, "algorithm": ALGORITHM_VERSION,
            "feature_version": FEATURE_VERSION, "ordered_features": FEATURE_ORDER,
            "scheduled_rounds": "unknown_for_every_target_and_history", "deferred_columns": DEBUT_COLS,
            "normalization_fit": "none_in_phase5a", "legitimate_nans_preserved": True}),
        "missingness.json": json_bytes({"eligible_rows": len(frame), "features": summary.get("missingness", {}),
            "limitations": ["contemporary mutable physical profiles", "unverified stored title defaults",
                "warehouse statistics can inherit default zeros", "source history coverage not independently certified",
                "unknown stance retains legacy corrected-algorithm semantics; future metadata gate applies"]}),
        "registry/considered_bouts.json": json_bytes(future),
        "registry/coverage.json": json_bytes({"scope": "all captured warehouse bouts with event_date >= exclusive cutoff",
            "considered_count": len(future), "registered_count": len(registrations),
            "data_blocked_count": sum(bool(r["eligibility_reasons"]) for r in registrations),
            "reason_counts_overlapping": dict(Counter(x for r in registrations for x in r["eligibility_reasons"])),
            "real_predictions": 0, "complete_paired_forecasts": 0, "registry_keys": [r["registry_key"] for r in registrations]})}
    previous = None
    for i, record in enumerate(registrations, 1):
        raw = json_bytes({"sequence": i, "previous_sha256": previous, "record": record})
        payloads[f"registry/records/{i:08d}.json"] = raw
        previous = sha256(raw)
    payloads["registry/tail_receipt.json"] = json_bytes({"sequence": len(registrations), "tail_sha256": previous})
    payloads.update(reference_preparation(source_payloads, metadata))
    freshness = {}
    for name in TABLES:
        rows = json.loads(source_payloads[f"sources/{name}.json"])
        observations = sorted(r["scraped_at"] for r in rows if r.get("scraped_at"))
        freshness[name] = {"rows": len(rows), "missing_scraped_at": sum(not r.get("scraped_at") for r in rows),
            "earliest_recorded_scrape": observations[0] if observations else None,
            "latest_recorded_scrape": observations[-1] if observations else None,
            "columns": sorted(rows[0]) if rows else []}
    payloads["source_manifest.json"] = json_bytes({"contract_version": VERSION,
        "capture_completed_at": capture_completed_at, "files": {n: {"sha256": sha256(b), "bytes": len(b)} for n,b in sorted(source_payloads.items())},
        "freshness": freshness, "database_schemas_file": "sources/schemas.json",
        "queries_and_clocks_file": "source_capture_receipt.json",
        "challenger_sources": ["sources/" + n + ".json" for n in TABLES],
        "reference_only": "reference_bootstrap/stored_features.json",
        "historical_availability": "unverified; no historical per-bout audit conducted"})
    return payloads


def versions() -> dict:
    return {"python": platform.python_version(), **{name: importlib.metadata.version(name) for name in
        ("numpy", "pandas", "scikit-learn", "xgboost", "psycopg2-binary", "joblib", "pytest")}}


def publish(destination: Path, payloads: dict[str, bytes]) -> str:
    """Exclusive directory and exact immutable bytes, checksummed before handoff."""
    destination = destination.resolve()
    if destination.is_relative_to(ROOT) and not destination.is_relative_to(OUTPUT_ROOT):
        raise PreflightError("Publication must stay in the separate Phase5A root")
    if destination.exists():
        raise PreflightError("Never overwrite an existing Phase 5 run")
    if any(Path(n).is_absolute() or ".." in Path(n).parts for n in payloads):
        raise PreflightError("Payload path escapes run")
    checksums = json_bytes({"files": {n: sha256(b) for n,b in sorted(payloads.items())}})
    destination.mkdir(parents=True, exist_ok=False)
    # INCOMPLETE persists on failure. Only fully verified runs are loadable.
    marker = destination / "INCOMPLETE"
    marker.write_bytes(b"Phase5A publication incomplete\n")
    for name, raw in {**payloads, "checksums.json": checksums}.items():
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(raw)
        path.chmod(0o444)
    verify_checksums(destination, expected_checksums_sha256=sha256(checksums), allow_incomplete=True)
    marker.unlink()
    return sha256(checksums)


def verify_checksums(directory: Path, *, expected_checksums_sha256: str, allow_incomplete=False) -> dict:
    if (directory / "INCOMPLETE").exists() and not allow_incomplete:
        raise PreflightError("Incomplete Phase5A run")
    raw = (directory / "checksums.json").read_bytes()
    if sha256(raw) != expected_checksums_sha256:
        raise PreflightError("Checksums root mismatch")
    checksums = json.loads(raw)["files"]
    actual = {str(p.relative_to(directory)) for p in directory.rglob("*") if p.is_file()}
    if actual - {"checksums.json", "INCOMPLETE"} != set(checksums):
        raise PreflightError("Run file list mismatch")
    for name, digest in checksums.items():
        path = (directory / name).resolve()
        if not path.is_relative_to(directory.resolve()) or sha256(path.read_bytes()) != digest:
            raise PreflightError("Run checksum tampering: " + name)
    return checksums


def load_current_preparation(directory: Path, *, expected_checksums_sha256: str,
                             expected_training_manifest_sha256: str) -> tuple[pd.DataFrame, dict, dict]:
    verify_checksums(directory, expected_checksums_sha256=expected_checksums_sha256)
    raw = (directory / "training_manifest.json").read_bytes()
    if sha256(raw) != expected_training_manifest_sha256:
        raise PreflightError("Current training manifest pin mismatch")
    manifest = json.loads(raw)
    if manifest.get("contract_version") != VERSION or not manifest["summary"]["ready"]:
        raise PreflightError("Blocked or incompatible current preparation")
    if manifest["feature_order"] != FEATURE_ORDER or manifest["feature_algorithm_version"] != ALGORITHM_VERSION:
        raise PreflightError("Current feature contract mismatch")
    if versions() != json.loads((directory / "package_versions.json").read_bytes()):
        raise PreflightError("Current preparation package versions changed")
    code = json.loads((directory / "code_versions.json").read_bytes())
    if set(code) != set(CODE_PATHS) or any(sha256((ROOT / path).read_bytes()) != digest for path,digest in code.items()):
        raise PreflightError("Current preparation code versions changed")
    config = json.loads((directory / "effective_configuration.json").read_bytes())
    validate_config(config)
    if sha256(json_bytes(config)) != manifest["config_sha256"]:
        raise PreflightError("Current configuration pin mismatch")
    cutoff_from_capture = datetime.fromisoformat(manifest["capture_completed_at"]).astimezone(timezone.utc).date().isoformat()
    if manifest["event_cutoff_exclusive"] != cutoff_from_capture:
        raise PreflightError("Exclusive cutoff must be capture completion UTC date")
    frame = pd.read_csv(directory / "training.csv", float_precision="round_trip")
    if list(frame.columns) != META + FEATURE_ORDER:
        raise PreflightError("Current exact ordered schema mismatch")
    frame["event_date"] = pd.to_datetime(frame.event_date)
    validate_training_frame(frame, event_cutoff=manifest["event_cutoff_exclusive"])
    if not frame[DEBUT_COLS + ["scheduled_rounds"]].isna().all().all():
        raise PreflightError("Schedules/debut preprocessing must remain unknown/deferred")
    folds = json.loads((directory / "folds.json").read_bytes())
    if folds != current_folds(frame, manifest["event_cutoff_exclusive"]):
        raise PreflightError("Current OOF membership changed")
    if sha256((directory / "training.csv").read_bytes()) != manifest["training_sha256"] or sha256(json_bytes(folds)) != manifest["folds_sha256"]:
        raise PreflightError("Current training/folds pin mismatch")
    return frame, manifest, folds

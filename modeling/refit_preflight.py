"""Outcome-free holdout exclusions, source checks, and refit data/fold preflight.

No model, calibration or performance code is imported or executed here.
The supported archive mode is retrospective, even when the source itself is
evidenced before the experiment's knowledge cutoff.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from features.data_loader import WarehouseData
from features.debut_prior import apply_debut_features, compute_debut_priors
from features.replay import index_source, reconstruct_bouts
from modeling.data import FEATURE_COLS_V2
from modeling.holdout import assert_no_holdout_fights, load_holdout_fight_ids
from warehouse.transform import transform_event, transform_fight, transform_fighter, transform_fight_stat

ROOT = Path(__file__).resolve().parents[1]
DEBUT_COLS = ["debut_prior_win_prob_f1", "debut_height_adv", "debut_reach_adv"]
FEATURE_ORDER = FEATURE_COLS_V2 + DEBUT_COLS
SCHEMA_VERSION = 1
FEATURE_VERSION = 2
ALGORITHM_VERSION = "v2_date_frozen_elo_schedule_unknown_v1"
SOURCE_MODE = "retrospective_git_pre_cutoff"
SOURCE_FILES = {"events": "data/events.csv", "fighters": "data/fighters.csv",
                "fights": "data/fights.csv", "fight_stats": "data/fight_stats.csv"}
DEV_WINDOWS = [("dev_2022", "2022-03-31", "2023-03-31"),
               ("dev_2023", "2023-03-31", "2024-03-31"),
               ("dev_2024", "2024-03-31", "2025-03-31")]
OOF_WINDOWS = [("oof_1", "2025-03-31", "2025-06-30"),
               ("oof_2", "2025-06-30", "2025-09-30"),
               ("oof_3", "2025-09-30", "2025-12-31"),
               ("oof_4", "2025-12-31", "2026-03-31")]


class PreflightError(ValueError):
    """Fail closed before any model can be fit."""


def json_bytes(value) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _instant(value: str) -> datetime:
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise PreflightError("Invalid knowledge cutoff") from exc
    if instant.tzinfo is None:
        raise PreflightError("Knowledge cutoff must include a timezone")
    return instant.astimezone(timezone.utc)


def classify_availability(*, observed_at: str | None, version_matches: bool,
                          required_by: str) -> str:
    """A late/missing observation is not proof of target leakage."""
    if observed_at is None:
        return "missing_availability_evidence"
    if not version_matches:
        return "unverified_source_version"
    return ("verified_by_required_time" if _instant(observed_at) <= _instant(required_by)
            else "observed_only_after_required_time")


def _unique(rows: list[dict], keys: tuple[str, ...], source: str) -> None:
    identities = []
    for row in rows:
        if any(row.get(k) is None or pd.isna(row[k]) or not str(row[k]).strip()
               or str(row[k]) != str(row[k]).strip() for k in keys):
            raise PreflightError(f"Missing/padded identifiers in {source}")
        identities.append(tuple(str(row[k]) for k in keys))
    if len(set(identities)) != len(identities):
        raise PreflightError(f"Duplicate identities in {source}")


def transform_archived_fight(raw: dict) -> dict:
    """Reject finish-round-as-schedule leakage in the historical CSV contract.

    num_rounds and finish_round both came from the post-event Round selector.
    There is no recoverable scheduled-round field in these CSVs. Actual elapsed
    time remains legitimate information for later fights' historical rates.
    """
    fight = transform_fight(raw)
    fight["scheduled_rounds"] = None
    elapsed = None
    if (fight["finish_round"] is not None and raw.get("finish_time_minute", "") != ""
            and raw.get("finish_time_second", "") != ""):
        seconds = int(raw["finish_time_minute"]) * 60 + int(raw["finish_time_second"])
        elapsed = (fight["finish_round"] - 1) * 300 + seconds
    fight["elapsed_duration_seconds"] = elapsed
    return fight


def load_git_source(*, source_ref: str, knowledge_cutoff: str,
                    repo: Path = ROOT) -> tuple[WarehouseData, dict, dict[str, bytes]]:
    """Read exact Git blobs without modifying the checkout or warehouse."""
    if not source_ref or source_ref.startswith("-"):
        raise PreflightError("An explicit Git source ref is required")
    commit = subprocess.check_output(["git", "rev-parse", "--verify", source_ref + "^{commit}"],
                                     cwd=repo, text=True).strip()
    observed_at = subprocess.check_output(["git", "show", "-s", "--format=%cI", commit],
                                          cwd=repo, text=True).strip()
    if classify_availability(observed_at=observed_at, version_matches=True,
                             required_by=knowledge_cutoff) != "verified_by_required_time":
        raise PreflightError("Git source commit is after the knowledge cutoff")
    raw_files, rows, provenance = {}, {}, {}
    for name, path in SOURCE_FILES.items():
        raw = subprocess.check_output(["git", "show", f"{commit}:{path}"], cwd=repo)
        raw_files[name + ".csv"] = raw
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
        if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise PreflightError(f"Invalid CSV schema: {path}")
        rows[name] = list(reader)
        if any(None in r or None in r.values() for r in rows[name]):
            raise PreflightError(f"Malformed CSV: {path}")
        provenance[name] = {"git_path": path, "sha256": sha256(raw), "rows": len(rows[name]),
                            "blob": subprocess.check_output(["git", "rev-parse", f"{commit}:{path}"],
                                                             cwd=repo, text=True).strip()}
    data = WarehouseData(events=[transform_event(r) for r in rows["events"]],
                         fighters=[transform_fighter(r) for r in rows["fighters"]],
                         fights=[transform_archived_fight(r) for r in rows["fights"]],
                         fight_stats=[transform_fight_stat(r) for r in rows["fight_stats"]])
    # The legacy transformer maps malformed outcomes to NC. Reject them here.
    if any((r.get("fighter_1_outcome"), r.get("fighter_2_outcome")) not in
           {("W", "L"), ("L", "W"), ("D", "D"), ("NC", "NC")} for r in rows["fights"]):
        raise PreflightError("Invalid raw result labels in archive")
    _unique(data.fight_stats, ("fight_stat_id",), "raw archive stats")
    _unique(data.fight_stats, ("fight_id", "fighter_id"), "raw archive stat pairs")
    fight_ids = {f["fight_id"] for f in data.fights}
    unused_stats = [s for s in data.fight_stats if s["fight_id"] not in fight_ids]
    # These rows have no corresponding archived bout and cannot enter a
    # history. Keep the exact raw file and report them, without dropping bouts
    # that have sparse statistics or inventing bout identities for the orphans.
    data.fight_stats = [s for s in data.fight_stats if s["fight_id"] in fight_ids]
    provenance = {"commit": commit, "commit_observation_evidence": observed_at,
                  "files": provenance, "knowledge_cutoff_availability": "git_version_before_cutoff",
                  "per_target_availability": "unverified",
                  "unused_statistics_without_archived_bout": {
                      "rows": len(unused_stats),
                      "fight_ids": sorted({s["fight_id"] for s in unused_stats}),
                      "reason": "cannot join to any archived bout; raw rows preserved"},
                  "scheduled_rounds": {"status": "unavailable_in_archive",
                      "num_rounds_equals_finish_round_rows": sum(r["num_rounds"] == r["finish_round"] for r in rows["fights"]),
                      "action": "num_rounds never consumed as schedule; unknown schedule/five-round experience kept NaN"},
                  "limitations": ["Git commit time is repository evidence, not a trusted timestamp service",
                                  "No pre-bout version history for mutable profiles/stat corrections",
                                  "Rows absent from this source are not silently sourced from today's warehouse"]}
    return data, provenance, raw_files


def validate_source(data: WarehouseData) -> None:
    for name, rows, keys in [("events", data.events, ("event_id",)),
                              ("fighters", data.fighters, ("fighter_id",)),
                              ("fights", data.fights, ("fight_id",)),
                              ("stats", data.fight_stats, ("fight_stat_id",)),
                              ("stat pairs", data.fight_stats, ("fight_id", "fighter_id"))]:
        _unique(rows, keys, name)
    events = {r["event_id"]: r for r in data.events}
    fighters = {r["fighter_id"] for r in data.fighters}
    for event in data.events:
        if not isinstance(event.get("event_date"), date):
            raise PreflightError("Missing/invalid source event date")
    for fight in data.fights:
        _unique([fight], ("fight_id", "event_id", "fighter_1_id", "fighter_2_id"), "fight")
        if fight["event_id"] not in events or not {fight["fighter_1_id"], fight["fighter_2_id"]} <= fighters:
            raise PreflightError("Fight references missing event/fighter")
        if fight["fighter_1_id"] == fight["fighter_2_id"]:
            raise PreflightError("Self matchup")
        result = fight.get("result_type")
        winner = fight.get("winner_fighter_id")
        if result not in {"win", "draw", "nc", "upcoming"} or (
                result == "win" and winner not in (fight["fighter_1_id"], fight["fighter_2_id"])):
            raise PreflightError("Invalid result/winner label")
    fights = {r["fight_id"]: r for r in data.fights}
    for stat in data.fight_stats:
        fight = fights.get(stat.get("fight_id"))
        if not fight or stat.get("fighter_id") not in (fight["fighter_1_id"], fight["fighter_2_id"]):
            raise PreflightError("Statistic references missing/wrong fight participant")
    index_source(data)


def validate_training_frame(df: pd.DataFrame, *, event_cutoff: str,
                            feature_order: list[str] = FEATURE_ORDER) -> None:
    if feature_order != FEATURE_ORDER or len(df.columns) != len(set(df.columns)):
        raise PreflightError("Unexpected feature order/schema")
    required = {"fight_id", "event_id", "fighter_1_id", "fighter_2_id", "event_date",
                "label", "feature_version", "weight_class", *feature_order}
    if not required <= set(df.columns):
        raise PreflightError(f"Missing training columns: {sorted(required - set(df.columns))}")
    _unique(df.to_dict("records"), ("fight_id",), "training frame")
    for key in ("event_id", "fighter_1_id", "fighter_2_id"):
        _unique([{key: v, "row": i} for i, v in enumerate(df[key])], (key, "row"), key)
        if df[key].isna().any():
            raise PreflightError(f"Null {key}")
    if df.empty or not df["label"].isin([0, 1]).all():
        raise PreflightError("Empty training frame or invalid binary labels")
    dates = pd.to_datetime(df["event_date"], errors="raise")
    if dates.isna().any() or not (dates < pd.Timestamp(event_cutoff)).all():
        raise PreflightError("Training events violate exclusive cutoff")
    if not df["feature_version"].eq(FEATURE_VERSION).all():
        raise PreflightError("Unexpected feature version")
    if df.groupby("event_id")["event_date"].nunique().gt(1).any():
        raise PreflightError("Event identity spans multiple dates")
    pairs = df.apply(lambda r: (str(r.event_id), *sorted((str(r.fighter_1_id), str(r.fighter_2_id)))), axis=1)
    if pairs.duplicated().any() or df.fighter_1_id.eq(df.fighter_2_id).any():
        raise PreflightError("Duplicate bout identity/self matchup")
    for col in feature_order:
        try:
            values = pd.to_numeric(df[col], errors="raise").to_numpy(dtype=float)
        except (ValueError, TypeError) as exc:
            raise PreflightError(f"Non-numeric feature: {col}") from exc
        if np.isinf(values).any():
            raise PreflightError(f"Infinite feature: {col}")
    assert_no_holdout_fights(df)


def prepare_training_frame(data: WarehouseData, *, event_cutoff: str,
                           knowledge_cutoff: str, source_mode: str) -> tuple[pd.DataFrame, dict]:
    if source_mode != SOURCE_MODE:
        raise PreflightError("Strict replay unavailable: no complete per-target source-version evidence; "
                             "choose a reviewed retrospective source mode explicitly")
    cutoff = date.fromisoformat(event_cutoff)
    if cutoff > _instant(knowledge_cutoff).date():
        raise PreflightError("Event cutoff extends beyond knowledge cutoff")
    validate_source(data)
    exclusions = load_holdout_fight_ids()
    reasons = {"event_on_or_after_cutoff": 0, "holdout_id": 0, "draw": 0, "nc": 0, "upcoming": 0}
    targets, histories = [], []
    for fight in data.fights:
        # Both historical input results and fitting labels are bounded here.
        if fight["event_date"] >= cutoff:
            reasons["event_on_or_after_cutoff"] += 1
        elif fight["fight_id"] in exclusions:
            reasons["holdout_id"] += 1
        else:
            histories.append(fight)
            if fight["result_type"] == "win":
                targets.append(fight)
            else:
                reasons[fight["result_type"]] += 1
    historical = WarehouseData(fights=histories, fighter_by_id=data.fighter_by_id,
                               stats_by_fight_fighter=data.stats_by_fight_fighter)
    df = pd.DataFrame(reconstruct_bouts(historical, targets))
    if df.empty:
        raise PreflightError("No eligible labels")
    df["event_date"] = pd.to_datetime(df["event_date"])
    for col in FEATURE_COLS_V2:
        df[col] = pd.to_numeric(df[col], errors="raise").astype(float)
    # No global normalization is fit. All three slots remain deferred.
    for col in DEBUT_COLS:
        df[col] = np.nan
    meta = ["fight_id", "event_id", "fighter_1_id", "fighter_2_id", "event_date",
            "weight_class", "label", "feature_version"]
    df = df[meta + FEATURE_ORDER].sort_values(["event_date", "fight_id"]).reset_index(drop=True)
    validate_training_frame(df, event_cutoff=event_cutoff)
    return df, {"source_fights": len(data.fights), "eligible_rows": len(df), "excluded_row_reasons": reasons,
                "exclusion_priority": list(reasons), "holdout_exclusion_count": len(exclusions),
                "event_count": int(df.event_id.nunique()), "date_count": int(df.event_date.nunique()),
                "min_event_date": df.event_date.min().date().isoformat(),
                "max_event_date": df.event_date.max().date().isoformat(),
                "label_counts": {str(k): int(v) for k, v in df.label.value_counts().sort_index().items()},
                "both_debuting_rows": int(df.both_debuting.eq(1).sum()),
                "missingness": {c: int(df[c].isna().sum()) for c in FEATURE_ORDER},
                "deferred_preprocessing_columns": DEBUT_COLS,
                "sparse_rows_preserved": True}


def split_window(df: pd.DataFrame, start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if pd.Timestamp(start) >= pd.Timestamp(end):
        raise PreflightError("Invalid fold window")
    train = df[df.event_date < pd.Timestamp(start)].copy()
    valid = df[(df.event_date >= pd.Timestamp(start)) & (df.event_date < pd.Timestamp(end))].copy()
    if train.empty or valid.empty or train.event_date.max() >= valid.event_date.min():
        raise PreflightError("Empty or non-temporal fold")
    if set(train.event_id) & set(valid.event_id) or set(train.event_date) & set(valid.event_date):
        raise PreflightError("Event/date split between fold partitions")
    assert_no_holdout_fights(train)
    assert_no_holdout_fights(valid)
    return train, valid


def prepare_fold_inputs(df: pd.DataFrame, start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Phase 3B preprocessing only: derive normalization from this fold's train."""
    train, valid = split_window(df, start, end)
    priors = compute_debut_priors(train)
    return apply_debut_features(train, priors), apply_debut_features(valid, priors), priors


def fold_manifest(df: pd.DataFrame, *, event_cutoff: str) -> dict:
    validate_training_frame(df, event_cutoff=event_cutoff)
    if event_cutoff != OOF_WINDOWS[-1][2]:
        raise PreflightError("This fold design requires the March 31 2026 exclusive cutoff")
    folds, coverage = [], []
    for phase, windows in (("development", DEV_WINDOWS), ("calibration_oof", OOF_WINDOWS)):
        for name, start, end in windows:
            train, valid = split_window(df, start, end)
            tr_ids, va_ids = sorted(train.fight_id), sorted(valid.fight_id)
            folds.append({"name": name, "phase": phase, "train_end_exclusive": start,
                          "prediction_start_inclusive": start, "prediction_end_exclusive": end,
                          "train_rows": len(train), "prediction_rows": len(valid),
                          "train_fight_ids": tr_ids, "prediction_fight_ids": va_ids,
                          "train_ids_sha256": sha256(json_bytes(tr_ids)),
                          "prediction_ids_sha256": sha256(json_bytes(va_ids)),
                          "preprocessing_fit_partition": "train_only",
                          "selection_allowed": phase == "development"})
            if phase == "calibration_oof":
                coverage.extend(va_ids)
    expected = set(df.loc[df.event_date >= pd.Timestamp(OOF_WINDOWS[0][1]), "fight_id"])
    if len(coverage) != len(set(coverage)) or set(coverage) != expected:
        raise PreflightError("OOF coverage has missing/duplicate identities")
    return {"schema_version": SCHEMA_VERSION, "folds": folds, "oof_rows": len(coverage),
            "oof_start": OOF_WINDOWS[0][1], "oof_end_exclusive": event_cutoff,
            "final_training_rows": len(df), "final_training_ids_sha256": sha256(json_bytes(sorted(df.fight_id))),
            "selection_data_end_exclusive": DEV_WINDOWS[-1][2],
            "no_event_or_date_split": True, "complete_eligible_oof_coverage": True}


def publish_snapshot(df: pd.DataFrame, *, destination: Path, manifest: dict,
                     folds: dict, source_files: dict[str, bytes]) -> dict:
    """Exclusive publication of immutable deterministic data; never under models."""
    destination = destination.resolve()
    if destination.is_relative_to(ROOT / "models") or destination.is_relative_to(ROOT / "data/holdouts"):
        raise PreflightError("Snapshot destination is protected")
    if (manifest.get("schema_version") != SCHEMA_VERSION or
            manifest.get("feature_version") != FEATURE_VERSION or
            manifest.get("feature_algorithm_version") != ALGORITHM_VERSION or
            manifest.get("feature_order") != FEATURE_ORDER):
        raise PreflightError("Unexpected snapshot schema/feature version")
    validate_training_frame(df, event_cutoff=manifest["event_cutoff_exclusive"])
    raw = df.to_csv(index=False, date_format="%Y-%m-%d", lineterminator="\n", float_format="%.17g").encode()
    payloads = {"training.csv": raw, "folds.json": json_bytes(folds),
                **{"sources/" + k: v for k, v in source_files.items()}}
    manifest = {**manifest, "files": {k: {"sha256": sha256(v), "bytes": len(v)}
                                        for k, v in sorted(payloads.items())}}
    payloads["manifest.json"] = json_bytes(manifest)
    destination.mkdir(parents=True, exist_ok=False)
    for name, payload in payloads.items():
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as f:
            f.write(payload)
        path.chmod(0o444)
    return {"manifest_sha256": sha256(payloads["manifest.json"]), **manifest["files"]}


def validate_refit_config(config: dict) -> None:
    """Fail closed on temporal design drift, without authorizing any fitting."""
    expected = {"schema_version": SCHEMA_VERSION, "feature_version": FEATURE_VERSION,
                "feature_algorithm_version": ALGORITHM_VERSION,
                "event_cutoff_exclusive": "2026-03-31",
                "knowledge_cutoff": "2026-03-31T12:09:04.077607+00:00",
                "source_mode": SOURCE_MODE, "decision_policy_version": "probability_band_v1",
                "holdout_exclusion_count": 166, "phase3b_mode_approval_required": True}
    if any(config.get(k) != v for k, v in expected.items()):
        raise PreflightError("Unexpected refit configuration version/cutoff/source mode")
    for section, key, windows in (("development", "validation_windows", DEV_WINDOWS),
                                   ("oof", "prediction_windows", OOF_WINDOWS)):
        if config.get(section, {}).get(key) != [[s, e] for _, s, e in windows]:
            raise PreflightError("Unexpected fold memberships")
    if config.get("oof", {}).get("early_stopping") is not False:
        raise PreflightError("OOF windows must not select their own rounds")
    final = config.get("final_refit", {})
    if final.get("early_stopping") is not False or final.get("retain_legacy_validation_test_reservation") is not False:
        raise PreflightError("Final refit must use all eligible rows with fixed rounds")


def load_prepared_snapshot(directory: Path, *, expected_manifest_sha256: str,
                           source_mode: str) -> tuple[pd.DataFrame, dict, dict]:
    """Phase 3B input guard: validate exact files before callers may fit anything."""
    if (directory / "REJECTED.json").exists():
        raise PreflightError("Rejected preliminary snapshot cannot be used")
    raw = (directory / "manifest.json").read_bytes()
    if sha256(raw) != expected_manifest_sha256:
        raise PreflightError("Snapshot manifest checksum mismatch")
    manifest = json.loads(raw)
    if (manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("feature_version") != FEATURE_VERSION
            or manifest.get("feature_algorithm_version") != ALGORITHM_VERSION
            or manifest.get("feature_order") != FEATURE_ORDER):
        raise PreflightError("Unexpected snapshot schema/feature version")
    if source_mode != SOURCE_MODE or manifest.get("source_mode") != source_mode:
        raise PreflightError("Missing/unapproved source mode; strict replay is unavailable")
    if manifest.get("certification_status") != "chronological_retrospective_only":
        raise PreflightError("Unexpected availability classification")
    if (manifest.get("event_cutoff_exclusive") != "2026-03-31" or
            manifest.get("knowledge_cutoff") != "2026-03-31T12:09:04.077607+00:00"):
        raise PreflightError("Unexpected experiment cutoffs")
    files = manifest.get("files", {})
    if not {"training.csv", "folds.json", *("sources/" + name + ".csv" for name in SOURCE_FILES)} <= files.keys():
        raise PreflightError("Missing snapshot inputs")
    for name, entry in files.items():
        path = (directory / name).resolve()
        if not path.is_relative_to(directory.resolve()) or sha256(path.read_bytes()) != entry["sha256"]:
            raise PreflightError("Snapshot file path/checksum mismatch")
    frame = pd.read_csv(directory / "training.csv", float_precision="round_trip")
    if list(frame.columns[-len(FEATURE_ORDER):]) != FEATURE_ORDER:
        raise PreflightError("Unexpected stored feature order")
    frame["event_date"] = pd.to_datetime(frame.event_date)
    validate_training_frame(frame, event_cutoff=manifest["event_cutoff_exclusive"])
    folds = json.loads((directory / "folds.json").read_bytes())
    if folds != fold_manifest(frame, event_cutoff=manifest["event_cutoff_exclusive"]):
        raise PreflightError("Fold manifest differs from eligible training membership")
    return frame, manifest, folds


def validate_oof_inputs(oof: pd.DataFrame, training: pd.DataFrame, folds: dict) -> None:
    """Guard future calibration inputs without fitting a calibrator or scoring."""
    required = {"fight_id", "fold", "label", "raw_prob_f1", "fighter_1_id", "fighter_2_id"}
    if not required <= set(oof.columns):
        raise PreflightError("Missing OOF identifiers/orientation/probability columns")
    _unique(oof.to_dict("records"), ("fight_id",), "OOF predictions")
    assert_no_holdout_fights(oof)
    expected = {fid: f["name"] for f in folds["folds"] if f["phase"] == "calibration_oof"
                for fid in f["prediction_fight_ids"]}
    if set(oof.fight_id) != set(expected):
        raise PreflightError("OOF identities do not exactly cover eligible calibration rows")
    indexed = training.set_index("fight_id")
    for row in oof.to_dict("records"):
        if expected[row["fight_id"]] != row["fold"]:
            raise PreflightError("OOF fold assignment differs")
        if any(row[k] != indexed.at[row["fight_id"], k] for k in ("label", "fighter_1_id", "fighter_2_id")):
            raise PreflightError("OOF label/orientation differs from training snapshot")
    probabilities = pd.to_numeric(oof.raw_prob_f1, errors="raise").to_numpy(dtype=float)
    if not np.isfinite(probabilities).all() or ((probabilities < 0) | (probabilities > 1)).any():
        raise PreflightError("Invalid OOF probabilities")
    if set(oof.label) != {0, 1}:
        raise PreflightError("Calibration requires both label classes")

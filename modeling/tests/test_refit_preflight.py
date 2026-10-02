"""Data/fold guards only. No production mutations or learned model fitting."""

from copy import deepcopy
from datetime import date
import json
from pathlib import Path
import subprocess
import sys
import tomllib
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from features.tests.test_replay import synthetic_source
from modeling.data import load_bout_data
from modeling.holdout import load_holdout_fight_ids
from modeling.refit_preflight import (
    ALGORITHM_VERSION, DEV_WINDOWS, FEATURE_ORDER, FEATURE_VERSION, OOF_WINDOWS,
    ROOT, SCHEMA_VERSION, SOURCE_MODE, PreflightError, classify_availability,
    fold_manifest, json_bytes, load_prepared_snapshot, prepare_fold_inputs,
    prepare_training_frame, publish_snapshot, sha256, split_window,
    validate_refit_config, validate_source, validate_training_frame, validate_oof_inputs, transform_archived_fight,
)

CUTOFF = "2026-03-31"
KNOWLEDGE = "2026-03-31T12:09:04.077607+00:00"


def prepare(data=None):
    return prepare_training_frame(data or synthetic_source(), event_cutoff=CUTOFF,
                                   knowledge_cutoff=KNOWLEDGE, source_mode=SOURCE_MODE)


def temporal_frame():
    df, _ = prepare()
    rows = []
    days = ["2020-01-01", "2022-04-01", "2023-04-01", "2024-04-01",
            "2025-04-01", "2025-07-01", "2025-10-01", "2026-01-01", "2026-03-30"]
    for i, day in enumerate(days):
        for j in range(2):
            r = df.iloc[-1].copy()
            r["fight_id"], r["event_id"] = f"row-{i}-{j}", f"event-{i}"
            r["fighter_1_id"], r["fighter_2_id"] = f"a-{i}-{j}", f"b-{i}-{j}"
            r["event_date"] = pd.Timestamp(day)
            r["label"] = j
            r["both_debuting"] = 1
            r["diff_height_cm"], r["diff_reach_cm"] = i + j, i - j
            rows.append(r)
    return pd.DataFrame(rows).reset_index(drop=True)


def minimal_manifest():
    return {"schema_version": SCHEMA_VERSION, "feature_version": FEATURE_VERSION,
            "feature_algorithm_version": ALGORITHM_VERSION, "feature_order": FEATURE_ORDER,
            "event_cutoff_exclusive": CUTOFF, "knowledge_cutoff": KNOWLEDGE,
            "source_mode": SOURCE_MODE, "certification_status": "chronological_retrospective_only"}


def test_cutoff_exclusive_and_no_sparse_debut_rows_dropped():
    data = synthetic_source()
    data.events[-1]["event_date"] = date(2026, 3, 31)
    df, summary = prepare(data)
    assert "fight-7-0" not in set(df.fight_id)
    assert summary["excluded_row_reasons"]["event_on_or_after_cutoff"] == 1
    assert df.both_debuting.eq(1).any()
    assert df[FEATURE_ORDER].isna().any().any()
    assert list(df.columns[-50:]) == FEATURE_ORDER
    data.events[-1]["event_date"] = date(2026, 3, 30)
    assert "fight-7-0" in set(prepare(data)[0].fight_id)


def test_sql_cutoff_and_exclusions_before_fetch_preserves_raw_invalid_label():
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cols = ["fight_id", "fighter_1_id", "fighter_2_id", "event_date", "weight_class", "label",
            "both_debuting", "feature_version", "computed_at"]
    cur.description = [(c,) for c in cols]
    cur.fetchall.return_value = [("a", "f1", "f2", date(2026, 3, 30), "lightweight", 0.5, None, 2, None)]
    df = load_bout_data(conn, ["both_debuting"], event_cutoff=CUTOFF,
                        excluded_fight_ids=frozenset({"alternate", "original"}), include_metadata=True)
    sql, args = cur.execute.call_args.args
    assert "event_date < %s" in sql
    assert "fight_id::text = ANY(%s)" in sql
    assert args == (date(2026, 3, 31), ["alternate", "original"])
    assert df.label.iloc[0] == 0.5
    assert df.both_debuting.isna().all()


def test_legacy_loader_defaults_remain_unbounded():
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.description = [(c,) for c in ("fight_id", "event_date", "label", "diff_elo")]
    cur.fetchall.return_value = [("id", date(2026, 4, 1), 1, None)]
    df = load_bout_data(conn, ["diff_elo"])
    assert len(cur.execute.call_args.args) == 1
    assert "event_date <" not in cur.execute.call_args.args[0]
    assert len(df) == 1


@pytest.mark.parametrize("id_type", ["original", "alternate"])
def test_all_original_and_alternate_holdout_exclusions(id_type):
    evidence = json.loads((ROOT / "data/holdouts/historical_2026_apr_aug/identity-exclusions.json").read_bytes())
    identity = evidence["identities"][0]
    fid = identity["fight_id"] if id_type == "original" else next(
        r["alternate_bout"]["fight_id"] for r in evidence["identities"] if r.get("alternate_bout"))
    data = synthetic_source()
    data.fights[-1]["fight_id"] = fid
    # Remove associated old stat rows: synthetic held-out bout needs no stats.
    data.fight_stats = [s for s in data.fight_stats if s["fight_id"] != "fight-7-0"]
    df, report = prepare(data)
    assert fid not in set(df.fight_id)
    assert report["excluded_row_reasons"]["holdout_id"] == 1
    assert report["holdout_exclusion_count"] == 166


def test_preparation_never_opens_holdout_outcomes_or_joined_evidence(monkeypatch):
    original = Path.open
    def checked(path, *a, **kw):
        if path.name == "outcomes.csv" or "phase1-holdout-evidence" in str(path) or path.name == "pre_event_prediction_fights.csv":
            raise AssertionError(f"Forbidden outcome read: {path}")
        return original(path, *a, **kw)
    monkeypatch.setattr(Path, "open", checked)
    prepare()
    fold_manifest(temporal_frame(), event_cutoff=CUTOFF)


@pytest.mark.parametrize("change,message", [
    (lambda f: f.__setitem__("label", 0.5), "labels"),
    (lambda f: f.__setitem__("fighter_1_id", None), "identifiers|Null"),
    (lambda f: f.__setitem__("fight_id", None), "identifiers"),
    (lambda f: f.__setitem__("event_id", ""), "identifiers"),
    (lambda f: f.__setitem__("feature_version", 3), "version"),
    (lambda f: f.__setitem__("diff_elo", np.inf), "Infinite"),
    (lambda f: f.__setitem__("diff_elo", "corrupt"), "Non-numeric"),
    (lambda f: f.__setitem__("event_date", pd.Timestamp(CUTOFF)), "cutoff"),
])
def test_frame_rejects_invalid_input(change, message):
    frame = temporal_frame()
    change(frame)
    with pytest.raises(PreflightError, match=message):
        validate_training_frame(frame, event_cutoff=CUTOFF)


def test_duplicate_bout_identity_rejected_even_with_different_fight_id():
    frame = temporal_frame()
    frame.loc[1, ["fighter_1_id", "fighter_2_id"]] = frame.loc[0, ["fighter_2_id", "fighter_1_id"]].to_numpy()
    with pytest.raises(PreflightError, match="Duplicate bout"):
        validate_training_frame(frame, event_cutoff=CUTOFF)


def test_duplicate_source_identifiers_and_bad_winner_rejected():
    data = synthetic_source()
    data.fights.append(deepcopy(data.fights[0]))
    with pytest.raises(PreflightError, match="Duplicate"):
        validate_source(data)
    data = synthetic_source()
    data.fights[0]["winner_fighter_id"] = "outsider"
    with pytest.raises(PreflightError, match="winner"):
        validate_source(data)


def test_unexpected_feature_order_rejected():
    with pytest.raises(PreflightError, match="order"):
        validate_training_frame(temporal_frame(), event_cutoff=CUTOFF, feature_order=list(reversed(FEATURE_ORDER)))


def test_temporal_folds_complete_coverage_dates_and_events_unsplit():
    frame = temporal_frame()
    manifest = fold_manifest(frame, event_cutoff=CUTOFF)
    oof_ids = []
    for entry in manifest["folds"]:
        train, valid = split_window(frame, entry["prediction_start_inclusive"], entry["prediction_end_exclusive"])
        assert train.event_date.max() < valid.event_date.min()
        assert set(train.event_id).isdisjoint(valid.event_id)
        assert valid.groupby("event_id").size().eq(2).all()
        if entry["phase"] == "calibration_oof":
            oof_ids.extend(entry["prediction_fight_ids"])
            assert entry["selection_allowed"] is False
    assert len(oof_ids) == len(set(oof_ids)) == 10
    assert set(oof_ids) == set(frame.loc[frame.event_date >= "2025-03-31", "fight_id"])
    assert manifest == fold_manifest(frame.sample(frac=1, random_state=42), event_cutoff=CUTOFF)


def test_event_identity_across_dates_and_empty_folds_fail():
    frame = temporal_frame()
    frame.loc[2, "event_id"] = frame.loc[0, "event_id"]
    with pytest.raises(PreflightError, match="multiple dates"):
        fold_manifest(frame, event_cutoff=CUTOFF)
    with pytest.raises(PreflightError, match="Empty"):
        split_window(temporal_frame(), "2030-01-01", "2031-01-01")


def test_fold_priors_do_not_use_validation_rows():
    frame = temporal_frame()
    before_train, before_valid, before_prior = prepare_fold_inputs(frame, "2025-03-31", "2025-06-30")
    changed = frame.copy()
    changed.loc[changed.event_date >= "2025-03-31", "diff_height_cm"] = 10000
    changed.loc[changed.event_date >= "2025-03-31", "label"] = 1
    after_train, _, after_prior = prepare_fold_inputs(changed, "2025-03-31", "2025-06-30")
    assert before_prior == after_prior
    pd.testing.assert_frame_equal(before_train, after_train)
    assert before_valid.debut_height_adv.notna().all()
    assert frame.debut_height_adv.isna().all()
    assert prepare_fold_inputs(frame, "2025-09-30", "2025-12-31")[2] != before_prior


@pytest.mark.parametrize("observed,matches,classification", [
    (None, False, "missing_availability_evidence"),
    ("2026-03-01T00:00:00+00:00", False, "unverified_source_version"),
    ("2026-03-01T00:00:00+00:00", True, "verified_by_required_time"),
    ("2026-08-01T00:00:00+00:00", True, "observed_only_after_required_time"),
])
def test_availability_classification(observed, matches, classification):
    assert classify_availability(observed_at=observed, version_matches=matches, required_by=KNOWLEDGE) == classification


def test_strict_mode_fails_before_any_source_or_fit(monkeypatch):
    with pytest.raises(PreflightError, match="Strict replay unavailable"):
        prepare_training_frame(None, event_cutoff=CUTOFF, knowledge_cutoff=KNOWLEDGE, source_mode="strict_replay")
    result = subprocess.run([sys.executable, "tools/prepare_xgb_refit_data.py", "--event-cutoff", CUTOFF,
                             "--knowledge-cutoff", KNOWLEDGE, "--source-mode", "strict_replay",
                             "--source-ref", "missing-ref", "--output", "/tmp/phase3a-unpublished-strict-test"],
                            cwd=ROOT, text=True, capture_output=True)
    assert result.returncode == 2
    assert "Strict replay unavailable" in result.stderr
    assert "missing-ref" not in result.stderr


def test_deterministic_checksummed_snapshot_and_tamper_detection(tmp_path):
    frame = temporal_frame()
    folds = fold_manifest(frame, event_cutoff=CUTOFF)
    sources = {name + ".csv": b"example\n" for name in ("events", "fighters", "fights", "fight_stats")}
    a = publish_snapshot(frame, destination=tmp_path / "a", manifest=minimal_manifest(), folds=folds, source_files=sources)
    b = publish_snapshot(frame, destination=tmp_path / "b", manifest=minimal_manifest(), folds=folds, source_files=sources)
    assert a == b
    loaded, _, _ = load_prepared_snapshot(tmp_path / "a", expected_manifest_sha256=a["manifest_sha256"], source_mode=SOURCE_MODE)
    assert len(loaded) == len(frame)
    with pytest.raises(FileExistsError):
        publish_snapshot(frame, destination=tmp_path / "a", manifest=minimal_manifest(), folds=folds, source_files=sources)
    path = tmp_path / "a/training.csv"
    path.chmod(0o644)
    path.write_bytes(path.read_bytes() + b"corrupt\n")
    with pytest.raises(PreflightError, match="checksum"):
        load_prepared_snapshot(tmp_path / "a", expected_manifest_sha256=a["manifest_sha256"], source_mode=SOURCE_MODE)


def test_draft_config_and_reject_oof_early_stopping():
    config = tomllib.loads((ROOT / "configs/xgb_refit_pre_april_2026.toml").read_text())
    validate_refit_config(config)
    config["oof"]["early_stopping"] = True
    with pytest.raises(PreflightError, match="OOF"):
        validate_refit_config(config)


def test_oof_calibration_input_guard_no_fit():
    frame = temporal_frame()
    folds = fold_manifest(frame, event_cutoff=CUTOFF)
    assignment = {fid: f["name"] for f in folds["folds"] if f["phase"] == "calibration_oof"
                  for fid in f["prediction_fight_ids"]}
    oof = frame[frame.fight_id.isin(assignment)][["fight_id", "fighter_1_id", "fighter_2_id", "label"]].copy()
    oof["fold"] = oof.fight_id.map(assignment)
    # Fixed synthetic values exercise input validation, not candidate scoring.
    oof["raw_prob_f1"] = 0.5
    validate_oof_inputs(oof, frame, folds)
    bad = oof.copy()
    bad.loc[bad.index[0], "fold"] = "oof_4"
    with pytest.raises(PreflightError, match="assignment"):
        validate_oof_inputs(bad, frame, folds)
    bad = oof.iloc[1:].copy()
    with pytest.raises(PreflightError, match="cover"):
        validate_oof_inputs(bad, frame, folds)
    bad = oof.copy()
    bad.loc[bad.index[0], "fighter_1_id"] = "changed"
    with pytest.raises(PreflightError, match="orientation"):
        validate_oof_inputs(bad, frame, folds)


def test_archived_finish_round_never_enters_target_scheduled_rounds():
    raw = {"fight_id": "bout", "event_id": "event", "fighter_1_id": "a", "fighter_2_id": "b",
           "fighter_1_outcome": "W", "fighter_2_outcome": "L", "scraped_at": "2026-03-01T00:00:00+00:00",
           "num_rounds": "1", "finish_round": "1", "finish_time_minute": "2", "finish_time_second": "0",
           "primary_finish_method": "ko/tko", "bout_type": "Lightweight Bout"}
    first = transform_archived_fight(raw)
    assert first["scheduled_rounds"] is None
    assert first["elapsed_duration_seconds"] == 120
    raw.update(num_rounds="5", finish_round="5", finish_time_minute="5")
    second = transform_archived_fight(raw)
    assert second["scheduled_rounds"] is None
    assert second["elapsed_duration_seconds"] == 1500


def test_unknown_schedule_preserves_prior_observed_duration_and_uncertainty():
    from features.history import build_fighter_index, get_history
    from features.career import compute_career_features
    from features.physical import compute_physical_features
    data = synthetic_source()
    for f in data.fights:
        f["scheduled_rounds"] = None
        f["elapsed_duration_seconds"] = 900
    validate_source(data)
    history = get_history(build_fighter_index(data), "a", date(2020, 5, 1))
    career = compute_career_features(history)
    assert career["total_cage_time_seconds"] == 900 * len(history)
    assert career["career_sig_strikes_landed_per_min"] > 0
    assert compute_physical_features(data.fighter_by_id["a"], history, date(2020, 5, 1))["five_round_experience"] is None

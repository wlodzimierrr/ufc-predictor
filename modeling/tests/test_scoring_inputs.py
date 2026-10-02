"""Phase 4A guards, synthetic adversarial replay and outcome-free real inputs."""

from copy import deepcopy
from datetime import date
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from features.forecast_replay import DATE_SEMANTICS, reconstruct_forecast, utc_instant
from features.replay import index_source
from features.snapshot import build_fighter_snapshot
from features.tests.test_replay import synthetic_source
from modeling.scoring_inputs import (
    CANDIDATE, DEBUT_COLS, FEATURE_METADATA, FEATURE_ORDER, HOLDOUT, IDENTITY_COLUMNS,
    POLICY, ROOT, TARGET_COLUMNS, PreflightError, Snapshot, assess_inputs,
    comparison_protocol, construct_features, csv_bytes, decode_snapshot,
    discover_snapshots, json_bytes, load_targets, parse_profile, parse_target_flags,
    prepare_payload, publish, resolve_blob, select_capture, select_snapshot,
    sha256, source_structure, url_id, validate_feature_frame,
    validated_loader_reference, verify_run_checksums, verify_training_contract,
)


def features(row):
    return {k: row[k] for k in FEATURE_ORDER}


def target(data=None):
    data = data or synthetic_source()
    return deepcopy(data.fight_by_id["fight-7-0"])


@pytest.fixture(scope="module")
def real_payload():
    return prepare_payload()


def test_exact_108_original_order_orientation_and_instants():
    targets, validation = load_targets()
    original = pd.read_csv(HOLDOUT / "predictions.csv", usecols=IDENTITY_COLUMNS + ["scored_at"], dtype=str)
    assert len(targets) == len(original) == 108
    assert validation["events"] == 13
    for col in IDENTITY_COLUMNS:
        assert [t[col] for t in targets] == original[col].tolist()
    assert [t["scored_at_original"] for t in targets] == original.scored_at.tolist()
    assert list(pd.to_datetime([t["scored_at"] for t in targets], utc=True)) == list(pd.to_datetime(original.scored_at, utc=True))
    assert set(targets[0]) == set(TARGET_COLUMNS)
    assert not any("prob" in col or "correct" in col or "winner" in col or "confidence" in col for col in TARGET_COLUMNS)


def snapshot(commit, available, coherent=True):
    return Snapshot({"commit": commit, "availability_utc": available, "coherent": coherent}, None, {})


def test_source_before_after_scoring_and_deterministic_ties():
    a = snapshot("a", "2026-03-01T00:00:00Z")
    b = snapshot("b", "2026-03-01T00:00:00+00:00")
    later = snapshot("c", "2026-03-01T00:00:01Z")
    bad = snapshot("d", "2026-03-01T00:00:00Z", False)
    assert select_snapshot([later, bad, a, b], "2026-03-01T01:00:00+01:00") is b
    assert select_snapshot([b, later, a, bad], "2026-03-01T00:00:00Z") is b
    assert select_snapshot([later], "2026-03-01T00:00:00Z") is None
    assert select_snapshot([a], "2026-03-02T00:00:00Z") is a  # stale remains eligible


def test_real_later_archives_are_inspected_and_rejected_without_union(real_payload):
    payload, validation = real_payload
    sources = json.loads(payload["source-manifest.json"])["actual_scoring_sources"]
    historical_commits = {
        "0a13162ea60e0a2ede49d8d8d10b30a81683b717",
        "1f477d3ddc87b123d0099025b669e728e7881a34",
        "8bb0552fc2fa4f91e1e769c41dc999ac61b2f14b",
        "e23dd7cf3572c41776197c1990a09f17c9d96cc9",
        "4af6c2f63ebcf63e13fbdbbdcf3bce59243334e4",
        "6e5c0cd6afcb360c58036eaa1c489e71f1d4bc81",
    }
    source_commits = {s["commit"] for s in sources}
    assert len(source_commits) == len(sources)
    assert historical_commits <= source_commits
    by = {s["commit"][:7]: s for s in sources}
    assert by["1f477d3"]["coherent"]
    assert by["e23dd7c"]["structure"]["events"]["duplicate_identity_keys"] == 8
    assert by["e23dd7c"]["structure"]["fights"]["duplicate_identity_keys"] == 71
    assert by["4af6c2f"]["structure"]["fights"]["duplicate_identity_keys"] == 12
    assert by["8bb0552"]["structure"]["missing_event_references"] == 186
    assert not by["6e5c0cd"]["coherent"]
    assignments = json.loads(payload["source-selection.json"])["assignments"]
    assert {a["source_commit"] for a in assignments} == {by["1f477d3"]["commit"]}
    # Committing refreshed CSVs adds archives, but cannot make their versions
    # available to the already-frozen historical forecasts.
    last_scored_at = max(utc_instant(a["scored_at"]) for a in assignments)
    for source in sources:
        if source["commit"] not in historical_commits:
            assert utc_instant(source["availability_utc"]) > last_scored_at
            assert source["commit"] not in {a["source_commit"] for a in assignments}
    assert validation["gap_counts"] == {"unknown_target_title_status": 95, "missing_target_profile": 1,
                                        "unresolved_experience_for_supplemented_profile": 4}
    assert validation["forecasts_with_essential_gaps"] == 98


def test_immutable_git_blob_resolver_hashes_every_component():
    sources = discover_snapshots()
    for source in sources:
        for entry in source.record["files"].values():
            assert sha256(resolve_blob(entry)) == entry["sha256"]
            bad = {**entry, "sha256": "0" * 64}
            with pytest.raises(PreflightError, match="hash differs"):
                resolve_blob(bad)


def capture(fetched_at="2026-03-01T00:00:00Z", body_hash="abc", job="a"):
    url = "http://ufcstats.com/fighter-details/synthetic"
    return {"source_url": url, "fetched_at": fetched_at, "content_hash": body_hash,
            "http_status": "200", "fetch_status": "fetched", "storage_path": "data/raw/ufcstats/fighters/x.html", "job_run_id": job}


def test_raw_capture_requires_exact_surviving_body_and_time():
    a, b = capture(), capture(job="b")
    fid = url_id(a["source_url"])
    selected, status = select_capture([b, a], "abc", identity=fid, scored_at=a["fetched_at"])
    assert selected is b and status == "exact_body_available_by_scoring"
    selected, status = select_capture([a], "overwritten", identity=fid, scored_at=a["fetched_at"])
    assert selected is None and status == "body_missing_or_overwritten"
    assert select_capture([a], "abc", identity=fid, scored_at="2026-02-28T23:59:59Z")[0] is None
    assert select_capture([a], "abc", identity="wrong", scored_at=a["fetched_at"])[0] is None
    assert select_capture([{**a, "http_status": "500"}], "abc", identity=fid, scored_at=a["fetched_at"])[0] is None


@pytest.mark.parametrize("change", ["target", "future", "same_scoring_day", "opponent", "schedule"])
def test_synthetic_target_future_same_scoring_day_and_schedule_invariance(change):
    data = synthetic_source()
    matchup = target(data)
    scored_at = "2020-06-02T00:30:00+02:00"  # UTC June 1, excluded in entirety
    before, lineage = reconstruct_forecast(data, matchup, scored_at)
    changed = deepcopy(data)
    if change == "target":
        matchup.update(winner_fighter_id="b", result_type="win", finish_round=1, finish_time_seconds=1, label=0)
        # Even a corrupt source date cannot put the target result into histories.
        changed.fight_by_id[matchup["fight_id"]]["event_date"] = date(2019, 1, 1)
    for f in changed.fights:
        affected = f["event_date"] >= date(2020, 6, 1)
        if affected or change == "schedule":
            f["scheduled_rounds"] = 5
        if affected and change != "schedule":
            f.update(winner_fighter_id=f["fighter_2_id"], finish_round=1, finish_method="submission")
    for stat in changed.fight_stats:
        if changed.fight_by_id[stat["fight_id"]]["event_date"] >= date(2020, 6, 1):
            stat.update(sig_strikes_landed=1000000, takedowns_landed=200000)
    # Do not reindex event dates: this adversarial fixture changes the fight map.
    changed.stats_by_fight_fighter = {(s["fight_id"], s["fighter_id"]): s for s in changed.fight_stats}
    after, _ = reconstruct_forecast(changed, matchup, scored_at)
    assert features(before) == features(after)
    assert before["history_date_cutoff_exclusive"] == "2020-06-01"
    assert before["feature_reference_date"] == "2020-08-01"
    assert before["scheduled_rounds"] is None
    assert all(before[c] is None for c in DEBUT_COLS)
    assert "label" not in before and "winner_fighter_id" not in before
    assert all("fight-5" not in fid for f in lineage["fighters"] for fid in f["archived_prior_fight_ids"])


def test_distinct_cutoff_and_reference_date_reuse_training_snapshot(monkeypatch):
    data = synthetic_source()
    calls = []
    def checked(fighter, history, reference, elos, index, fid, fight_id):
        calls.append((reference, deepcopy(history), index))
        assert all(h.event_date < date(2020, 6, 1) for values in index.values() for h in values)
        return build_fighter_snapshot(fighter, history, reference, elos, index, fid, fight_id)
    monkeypatch.setattr("features.forecast_replay.build_fighter_snapshot", checked)
    row, _ = reconstruct_forecast(data, target(data), "2020-06-01T12:00:00Z")
    assert all(c[0] == date(2020, 8, 1) for c in calls)
    assert row["diff_age"] == 0
    assert row["history_date_cutoff_exclusive"] != row["feature_reference_date"]
    assert all(h.scheduled_rounds is None for c in calls for h in c[1])


def test_event_date_clock_changes_activity_without_admitting_intervening_results():
    data = synthetic_source()
    early = target(data)
    late = {**early, "event_date": date(2024, 8, 1)}
    before, one = reconstruct_forecast(data, early, "2020-06-01T12:00:00Z")
    after, two = reconstruct_forecast(data, late, "2020-06-01T12:00:00Z")
    assert before["diff_elo"] == after["diff_elo"]
    assert before["diff_fights_per_year_last3"] != after["diff_fights_per_year_last3"]
    assert one["fighters"] == two["fighters"]


def test_unknown_title_and_missing_profile_do_not_become_false_or_debut():
    data = synthetic_source()
    matchup = target(data)
    with pytest.raises(ValueError, match="title"):
        reconstruct_forecast(data, {**matchup, "is_title_fight": None}, "2020-06-01T12:00:00Z")
    data.fighter_by_id.pop("a")
    with pytest.raises(ValueError, match="profile"):
        reconstruct_forecast(data, matchup, "2020-06-01T12:00:00Z")


def synthetic_frame():
    data = synthetic_source()
    matchup = target(data)
    row, _ = reconstruct_forecast(data, matchup, "2020-06-01T12:00:00Z")
    row.update(event_date="2020-08-01", source_commit="synthetic")
    targets = [{**{k: row[k] for k in IDENTITY_COLUMNS + ["scored_at", "weight_class"]}, "fight_id": f"probe-{i}"} for i in range(108)]
    rows = [{**{k: row[k] for k in FEATURE_METADATA + FEATURE_ORDER}, "fight_id": t["fight_id"]} for t in targets]
    frame = pd.DataFrame(rows, columns=FEATURE_METADATA + FEATURE_ORDER)
    for col in FEATURE_ORDER:
        frame[col] = pd.to_numeric(frame[col], errors="raise").astype(float)
    return targets, frame


@pytest.mark.parametrize("change", ["order", "coverage", "orientation", "timestamp", "schedule", "debut", "infinite", "reference", "extra_outcome"])
def test_feature_validator_rejects_contract_changes(change):
    targets, frame = synthetic_frame()
    validate_feature_frame(frame, targets)
    if change == "order":
        columns = list(frame.columns)
        columns[-1], columns[-2] = columns[-2], columns[-1]
        frame = frame[columns]
    elif change == "coverage":
        frame = frame.iloc[:-1]
    elif change == "orientation":
        frame.loc[0, "fighter_1_id"] = "wrong"
    elif change == "timestamp":
        frame.loc[0, "scored_at"] = "2020-06-02T12:00:00Z"
    elif change == "schedule":
        frame.loc[0, "scheduled_rounds"] = 3
    elif change == "debut":
        frame.loc[0, DEBUT_COLS[0]] = .5
    elif change == "infinite":
        frame.loc[0, "diff_elo"] = np.inf
    elif change == "reference":
        frame.loc[0, "feature_reference_date"] = "2020-06-01"
    else:
        frame["actual_label"] = 1
    with pytest.raises(PreflightError):
        validate_feature_frame(frame, targets)


def test_reconstruction_all_108_gate_precedes_feature_build(monkeypatch):
    def fail(*a, **k):
        pytest.fail("feature construction entered for incomplete cohort")
    monkeypatch.setattr("modeling.scoring_inputs.reconstruct_forecast", fail)
    with pytest.raises(PreflightError, match="108-row"):
        construct_features([{}] * 108, [{"gaps": [{"kind": "essential"}]}] * 108, [], {})


def test_complete_synthetic_108_rebuild_feature_order_nan_and_original_metadata():
    targets, _ = synthetic_frame()
    data = synthetic_source()
    source = snapshot("synthetic", "2020-01-01T00:00:00Z")
    source.data = data
    assignments = [{"fight_id": t["fight_id"], "source_commit": "synthetic", "gaps": []} for t in targets]
    supplements = {t["fight_id"]: {"profiles": {}, "title": False} for t in targets}
    first, lineage = construct_features(targets, assignments, [source], supplements)
    second, again = construct_features(targets, assignments, [source], supplements)
    assert first == second and lineage == again
    frame = pd.read_csv(io.BytesIO(first), float_precision="round_trip")
    validate_feature_frame(frame, targets)
    assert frame[DEBUT_COLS + ["scheduled_rounds"]].isna().all().all()
    assert len(lineage["forecast_lineage"]) == 108
    forbidden = {"actual_label", "label", "correct", "raw_prob_f1", "calibrated_prob_f1", "winner_fighter_id"}
    assert not forbidden.intersection(frame.columns)


def test_existing_holdout_exclusion_guard_remains_outcome_free():
    from modeling.holdout import HoldoutError, assert_no_holdout_fights, load_holdout_fight_ids
    exclusions = load_holdout_fight_ids(HOLDOUT)
    assert len(exclusions) == 166
    with pytest.raises(HoldoutError):
        assert_no_holdout_fights(pd.DataFrame({"fight_id": [load_targets()[0][0]["fight_id"]]}), HOLDOUT)
    assert_no_holdout_fights(pd.DataFrame({"fight_id": ["synthetic-fit-identity"]}), HOLDOUT)


def test_honest_separation_of_training_and_scoring_sources(real_payload):
    payload, validation = real_payload
    manifest = json.loads(payload["source-manifest.json"])
    receipt = json.loads(payload["compatibility.json"])
    assert manifest["training_contract_is_scoring_source"] is False
    assert receipt["training_manifest_role"].startswith("reference contract only")
    assert receipt["scoring_source_manifest_sha256"] == sha256(payload["source-manifest.json"])
    assert receipt["input_provenance_kind"] == "new_forecast_reconstruction_from_actual_scoring_sources"
    assert receipt["loader_reference_supplied"] is False and receipt["real_vectors_validated"] is False
    assert receipt["training_contract_reference"]["source_mode"] != manifest["source_policy"]["mode"]
    assert "features.csv" not in payload and "INCOMPLETE.json" in payload


def test_blocked_run_rebuild_and_checksums_refuse_scoring_and_overwrite(real_payload, tmp_path, monkeypatch):
    payload, _ = real_payload
    monkeypatch.setattr("modeling.scoring_inputs.OUTPUT_ROOT", tmp_path)
    run = tmp_path / "blocked"
    first = publish(run, payload)
    assert verify_run_checksums(run) == first
    rebuilt, _ = prepare_payload(frozen_run=run)
    assert rebuilt == payload
    with pytest.raises(PreflightError, match="Blocked/incomplete"):
        validated_loader_reference(run)
    with pytest.raises(PreflightError, match="overwrite"):
        publish(run, payload)
    (run / "targets.csv").chmod(0o644)
    with (run / "targets.csv").open("ab") as f:
        f.write(b"changed")
    with pytest.raises(PreflightError, match="checksum differs"):
        verify_run_checksums(run)


def test_no_outcome_access_db_fit_priors_real_scoring_or_real_feature_construction(monkeypatch):
    import psycopg2
    import xgboost
    from sklearn.linear_model import LogisticRegression
    from modeling.xgb_candidate_bundle import CandidateBundle
    import builtins
    original_open = builtins.open
    original_path_open = Path.open
    def fail(*a, **k):
        pytest.fail("forbidden fit, score, database or real feature operation")
    def check(path):
        p = Path(path)
        if "holdouts" in p.parts and p.name not in {"manifest.json", "predictions.csv", "identity-exclusions.json"}:
            pytest.fail(f"frozen outcome access: {p}")
        if "identity-outcome" in p.name:
            pytest.fail(f"joined audit access: {p}")
    def checked_open(path, *args, **kwargs):
        if isinstance(path, (str, Path)):
            check(path)
        return original_open(path, *args, **kwargs)
    def checked_path_open(path, *args, **kwargs):
        check(path)
        return original_path_open(path, *args, **kwargs)
    monkeypatch.setattr(builtins, "open", checked_open)
    monkeypatch.setattr(Path, "open", checked_path_open)
    monkeypatch.setattr(psycopg2, "connect", fail)
    monkeypatch.setattr(xgboost.XGBClassifier, "fit", fail)
    monkeypatch.setattr(xgboost.XGBClassifier, "predict_proba", fail)
    monkeypatch.setattr(LogisticRegression, "fit", fail)
    monkeypatch.setattr(CandidateBundle, "predict", fail)
    monkeypatch.setattr("features.debut_prior.compute_debut_priors", fail)
    monkeypatch.setattr("features.debut_prior.apply_debut_features", fail)
    monkeypatch.setattr("modeling.scoring_inputs.reconstruct_forecast", fail)
    payload, validation = prepare_payload()
    assert validation["real_feature_rows_constructed"] == 0
    assert "features.csv" not in payload
    # Check the existing loader with the saved candidate, still without scoring.
    bundle = CandidateBundle(CANDIDATE)
    assert bundle.metadata["training_rows"] == 8400


def test_fixed_protocol_requires_prediction_lock_before_outcomes(real_payload):
    protocol = json.loads(real_payload[0]["comparison-protocol.json"])
    assert len(protocol["fight_ids_in_order"]) == 108
    assert protocol["bootstrap"]["event_count"] == 13
    assert protocol["bootstrap"]["seed"] == 20261002
    assert protocol["bootstrap"]["replicates"] == 10000
    assert protocol["primary_metrics"]["log_loss_clip"] == [1e-8, 1 - 1e-8]
    assert protocol["calibration_bins"]["edges"] == [i / 10 for i in range(11)]
    assert "before any Phase 4B outcome read" in protocol["prediction_lock"]
    assert protocol["promotion_authorized"] is False


def test_raw_profile_and_explicit_flags_do_not_parse_result_fields():
    fighter_urls = ["http://ufcstats.com/fighter-details/a", "http://ufcstats.com/fighter-details/b"]
    event_url, fight_url = "http://ufcstats.com/event-details/e", "http://ufcstats.com/fight-details/f"
    target = {"fight_id": url_id(fight_url), "event_id": url_id(event_url), "fighter_1_id": url_id(fighter_urls[0]),
              "fighter_2_id": url_id(fighter_urls[1]), "weight_class": "lightweight"}
    links = ''.join(f'<a href={u}>synthetic</a>' for u in fighter_urls + [event_url])
    raw = (links + '<i class="b-fight-details__fight-title">Lightweight Bout</i><p>Winner: invented</p>').encode()
    assert parse_target_flags(raw, {"source_url": fight_url}, target) is False
    assert parse_target_flags(links.encode(), {"source_url": fight_url}, target) is None
    assert parse_target_flags(raw, {"source_url": fight_url}, {**target, "fighter_1_id": "wrong"}) is None
    body = ''.join(f'<li class="b-list__box-list-item"><i>{k}:</i>{v}</li>' for k, v in [
        ("Height", "5' 10\""), ("Reach", '71"'), ("DOB", "Jun 24, 1994"), ("STANCE", "Orthodox")]).encode()
    profile = parse_profile(body, {"source_url": fighter_urls[0]}, target["fighter_1_id"])
    assert profile["reach_cm"] == 180
    assert profile["height_cm"] == float(5 * 12.0 * 2.54 + 10 * 2.54)
    assert profile["dob"] == date(1994, 6, 24)

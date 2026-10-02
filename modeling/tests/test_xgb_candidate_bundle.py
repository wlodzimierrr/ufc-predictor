"""Real offline save/load parity plus malformed candidate/input checks."""

from copy import deepcopy
import json
from pathlib import Path
import shutil

import joblib
import numpy as np
import pandas as pd
import pytest

from modeling.refit_preflight import DEBUT_COLS, FEATURE_ORDER, PreflightError, sha256
from modeling.train_xgb_candidate import (
    _fold_inputs, fit_calibrator, guarded_fit, preflight, write_checksums, write_json,
)
from modeling.xgb_candidate_bundle import CandidateBundle, publish_candidate
from modeling.xgb_candidate_contract import (
    ALGORITHM_VERSION, CALIBRATION_CONTRACT, CONFIG_SHA256, EVENT_CUTOFF, FEATURE_VERSION,
    FOLDS_SHA256, LABEL_MAPPING, MANIFEST_SHA256, ORIENTATION, REQUIRED_COMPONENTS,
    ROOT, SNAPSHOT, SOURCE_COMMIT, SOURCE_MODE, TRAINING_SHA256, calibrate, digest,
    feature_provenance, fit_partition_priors, membership, package_versions, preprocess_partition,
)


@pytest.fixture(scope="module")
def offline_bundle(tmp_path_factory):
    root = tmp_path_factory.mktemp("candidate-offline")
    config, whole, _, folds, _ = preflight(ROOT / "configs/xgb_refit_pre_april_2026.toml",
        snapshot=SNAPSHOT, manifest_sha256=MANIFEST_SHA256, source_mode=SOURCE_MODE)
    settings = {"parameters": {**config["base_parameters"], "max_depth": 3,
                               "min_child_weight": 20, "reg_lambda": 1.}, "rounds": 2}
    selection = {"settings_rounds_sha256": digest(settings), "frozen_settings": settings, "selected_rounds": 2}
    priors, pieces = {}, []
    for fold in folds["folds"][3:]:
        _, valid, prior = _fold_inputs(whole, fold, root)
        priors[fold["name"]] = prior
        out = valid[["fight_id", "event_id", "event_date", "fighter_1_id", "fighter_2_id", "label"]].copy()
        # Synthetic calibration inputs deliberately independent of labels.
        out["raw_prob_f1"] = np.linspace(.2, .8, len(valid))
        out["fold"] = fold["name"]
        out["settings_rounds_sha256"] = digest(settings)
        out["training_ids_sha256"] = fold["train_ids_sha256"]
        out["prior_sha256"] = digest(prior)
        pieces.append(out)
    oof = pd.concat(pieces).sort_values(["event_date", "fight_id"]).reset_index(drop=True)
    oof.to_csv(root / "oof.csv", index=False, date_format="%Y-%m-%d", float_format="%.17g")
    oof = pd.read_csv(root / "oof.csv", float_precision="round_trip", parse_dates=["event_date"])
    calibrator, details = fit_calibrator(oof, whole, folds, selection, priors)
    prior = fit_partition_priors(whole, whole, expected_ids=sorted(whole.fight_id), end=EVENT_CUTOFF, name="final")
    processed = preprocess_partition(whole, whole, expected_ids=sorted(whole.fight_id), prior_record=prior)
    model = guarded_fit(whole, processed, fold=None, parameters=settings["parameters"], rounds=2)
    for name in REQUIRED_COMPONENTS - {"checksums.json", "base_learner.json", "calibrator.joblib", "oof.csv"}:
        write_json(root / name, {})
    model.save_model(root / "base_learner.json")
    joblib.dump(calibrator, root / "calibrator.joblib")
    write_json(root / "final_debut_priors.json", prior)
    shutil.copyfile(SNAPSHOT / "manifest.json", root / "source_manifest.json")
    shutil.copyfile(SNAPSHOT / "folds.json", root / "folds.json")
    shutil.copyfile(ROOT / "configs/xgb_refit_pre_april_2026.toml", root / "effective_configuration.toml")
    write_json(root / "package_versions.json", package_versions())
    write_json(root / "selection.json", selection)
    write_json(root / "training_manifest.json", {"training_rows": 8400,
        "training_fight_ids": sorted(whole.fight_id), "training_ids_sha256": membership(whole),
        "no_early_stopping": True, "no_legacy_reservation": True, "rounds": 2})
    write_json(root / "oof_manifest.json", {"rows": 459, "oof_csv_sha256": sha256((root / "oof.csv").read_bytes()),
                                          "settings_rounds_sha256": digest(settings)})
    write_json(root / "metadata.json", {"schema_version": 1, "status": "COMPLETED",
        "feature_version": FEATURE_VERSION, "feature_algorithm_version": ALGORITHM_VERSION,
        "feature_order": FEATURE_ORDER, "label_mapping": LABEL_MAPPING, "orientation": ORIENTATION,
        "decision_policy_version": "probability_band_v1", "source_mode": SOURCE_MODE,
        "event_cutoff_exclusive": EVENT_CUTOFF, "knowledge_cutoff": config["knowledge_cutoff"],
        "preparation_manifest_sha256": MANIFEST_SHA256, "training_csv_sha256": TRAINING_SHA256,
        "folds_sha256": FOLDS_SHA256, "configuration_sha256": CONFIG_SHA256,
        "source_commit": SOURCE_COMMIT, "training_rows": 8400, "actual_training_endpoint": "2026-03-07",
        "input_feature_provenance": feature_provenance(), "selected_settings_sha256": digest(settings),
        "base_parameters": settings["parameters"], "final_prior_sha256": digest(prior),
        "calibration_contract": CALIBRATION_CONTRACT, "calibration": details})
    write_checksums(root)
    return root, whole, model, processed, calibrator


def test_real_bundle_round_trip_preserves_nan_orientation_and_precision(offline_bundle):
    root, whole, model, processed, calibrator = offline_bundle
    before = whole.copy()
    bundle = CandidateBundle(root)
    output = bundle.predict(whole, provenance=feature_provenance())
    raw = model.predict_proba(processed[FEATURE_ORDER], iteration_range=(0, 2))[:, 1].astype(float)
    assert np.array_equal(output.raw_prob_f1, raw)
    assert np.array_equal(output.calibrated_prob_f1, calibrate(calibrator, raw))
    assert output.fighter_1_id.equals(whole.fighter_1_id)
    assert output.calibrated_prob_f1.dtype == np.float64
    assert output.calibrated_prob_f2.equals(1 - output.calibrated_prob_f1)
    assert whole.equals(before)  # input NaNs and deferred slots untouched


@pytest.mark.parametrize("component", ["base_learner.json", "calibrator.joblib", "final_debut_priors.json",
                                      "source_manifest.json", "folds.json", "oof.csv", "metadata.json", "checksums.json"])
def test_missing_component_fails(offline_bundle, tmp_path, component):
    source = offline_bundle[0]
    copy = tmp_path / "bundle"
    shutil.copytree(source, copy)
    (copy / component).unlink()
    with pytest.raises(PreflightError):
        CandidateBundle(copy)


@pytest.mark.parametrize("component", ["base_learner.json", "calibrator.joblib", "final_debut_priors.json", "oof.csv"])
def test_tampered_component_fails_before_estimator_loading(offline_bundle, tmp_path, monkeypatch, component):
    copy = tmp_path / "bundle"
    shutil.copytree(offline_bundle[0], copy)
    with (copy / component).open("ab") as f:
        f.write(b"tampered")
    monkeypatch.setattr(joblib, "load", lambda *a, **k: pytest.fail("estimator loaded before checksum validation"))
    with pytest.raises(PreflightError, match="checksum mismatch"):
        CandidateBundle(copy)


@pytest.mark.parametrize("field,value", [("feature_algorithm_version", "legacy"), ("feature_order", ["diff_elo"]),
                                      ("orientation", "p_means_fighter_2"), ("status", "INCOMPLETE")])
def test_incompatible_metadata_fails_even_with_consistent_checksums(offline_bundle, tmp_path, field, value):
    copy = tmp_path / "bundle"
    shutil.copytree(offline_bundle[0], copy)
    m = json.loads((copy / "metadata.json").read_bytes())
    m[field] = value
    write_json(copy / "metadata.json", m)
    write_checksums(copy)
    with pytest.raises(PreflightError):
        CandidateBundle(copy)


@pytest.mark.parametrize("change", ["missing", "order", "provenance", "version", "schedule", "debut", "infinite"])
def test_incompatible_input_fails(offline_bundle, change):
    bundle = CandidateBundle(offline_bundle[0])
    frame = offline_bundle[1].iloc[:2].copy()
    provenance = feature_provenance()
    if change == "missing":
        frame = frame.drop(columns=["diff_age"])
    elif change == "order":
        columns = list(frame.columns)
        a, b = columns.index("diff_age"), columns.index("diff_elo")
        columns[a], columns[b] = columns[b], columns[a]
        frame = frame[columns]
    elif change == "provenance":
        provenance["feature_algorithm_version"] = "legacy_warehouse_v2"
    elif change == "version":
        frame["feature_version"] = 1
    elif change == "schedule":
        frame["scheduled_rounds"] = 3
    elif change == "debut":
        frame[DEBUT_COLS[0]] = .5
    else:
        frame["diff_age"] = np.inf
    with pytest.raises(PreflightError):
        bundle.predict(frame, provenance=provenance)


def test_full_precision_no_pick_boundaries_use_frozen_calibrator(offline_bundle, monkeypatch):
    bundle = CandidateBundle(offline_bundle[0])
    frame = offline_bundle[1].iloc[:6].copy()
    probabilities = np.array([np.nextafter(.4, 0.), .4, np.nextafter(.4, 1.),
                              np.nextafter(.6, 0.), .6, np.nextafter(.6, 1.)])
    monkeypatch.setattr("modeling.xgb_candidate_bundle.calibrate", lambda estimator, raw: probabilities)
    output = bundle.predict(frame, provenance=feature_provenance())
    assert np.array_equal(output.calibrated_prob_f1, probabilities)
    assert output.decision_status.tolist() == ["pick", "no_pick", "no_pick", "no_pick", "no_pick", "pick"]
    assert output.pick_label.iloc[0] == 0 and output.pick_label.iloc[-1] == 1
    assert output.pick_label.iloc[1:5].isna().all()


def test_loader_never_refits_priors_or_calibrator(offline_bundle, monkeypatch):
    def fail(*a, **k):
        pytest.fail("inference attempted a preprocessing/calibration fit")
    monkeypatch.setattr("features.debut_prior.compute_debut_priors", fail)
    monkeypatch.setattr("sklearn.linear_model.LogisticRegression.fit", fail)
    monkeypatch.setattr("psycopg2.connect", fail)
    bundle = CandidateBundle(offline_bundle[0])
    bundle.predict(offline_bundle[1].iloc[:2], provenance=feature_provenance())


def test_unlisted_files_and_path_escape_fail(offline_bundle, tmp_path):
    copy = tmp_path / "bundle"
    shutil.copytree(offline_bundle[0], copy)
    (copy / "unexpected.json").write_text('{}')
    with pytest.raises(PreflightError, match="unlisted"):
        CandidateBundle(copy)
    (copy / "unexpected.json").unlink()
    (copy / "base_learner.json").unlink()
    (copy / "base_learner.json").symlink_to(offline_bundle[0] / "base_learner.json")
    with pytest.raises(PreflightError, match="escapes"):
        CandidateBundle(copy)


def test_publication_refuses_non_staging_bundle(offline_bundle):
    with pytest.raises(PreflightError, match="incomplete run"):
        publish_candidate(offline_bundle[0])

"""Offline candidate lifecycle guards and learned-estimator contracts."""

from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import MagicMock
import warnings

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression

from features.debut_prior import compute_debut_priors
from modeling.holdout import HoldoutError, load_holdout_fight_ids
from modeling.refit_preflight import DEBUT_COLS, FEATURE_ORDER, PreflightError, fold_manifest, split_window
from modeling.tests.test_refit_preflight import temporal_frame
from modeling.train_xgb_candidate import (
    _fold_inputs, fit_calibrator, guarded_fit, preflight, validate_oof_provenance,
)
from modeling.xgb_candidate_bundle import check_calibrator, verify_checksums
from modeling.xgb_candidate_contract import (
    CALIBRATION_CONTRACT, CONFIG_SHA256, EVENT_CUTOFF, MANIFEST_SHA256, ROOT,
    SNAPSHOT, SOURCE_MODE, calibrate, digest, fit_partition_priors, guard_partition,
    log_odds, preprocess_partition, predict_best_iteration, select_development,
)


@pytest.fixture
def temporal():
    frame = temporal_frame()
    frame["scheduled_rounds"] = np.nan
    return frame


@pytest.mark.parametrize("change", ["missing", "duplicate", "label", "orientation", "feature", "future", "date"])
def test_partition_guard_rejects_changes_before_fit(temporal, monkeypatch, change):
    folds = fold_manifest(temporal, event_cutoff=EVENT_CUTOFF)
    fold = folds["folds"][0]
    train, valid = split_window(temporal, fold["prediction_start_inclusive"], fold["prediction_end_exclusive"])
    if change == "missing":
        train = train.iloc[:-1]
    elif change == "duplicate":
        train = pd.concat([train, train.iloc[:1]])
    elif change == "label":
        train.loc[train.index[0], "label"] = 1 - train.label.iloc[0]
    elif change == "orientation":
        train.loc[train.index[0], "fighter_1_id"] = "wrong-fighter"
    elif change == "feature":
        train.loc[train.index[0], "diff_elo"] += 1
    elif change == "future":
        train = pd.concat([train, valid.iloc[:1]])
    else:
        train.loc[train.index[0], "event_date"] = pd.Timestamp("2022-04-01")
    constructor = MagicMock()
    monkeypatch.setattr("modeling.train_xgb_candidate.XGBClassifier", constructor)
    with pytest.raises(PreflightError):
        guarded_fit(temporal, train, fold=fold, parameters={}, rounds=2, validation=valid)
    constructor.assert_not_called()


def test_holdout_and_early_stopping_label_guards(temporal, monkeypatch):
    fold = fold_manifest(temporal, event_cutoff=EVENT_CUTOFF)["folds"][0]
    train, valid = split_window(temporal, fold["prediction_start_inclusive"], fold["prediction_end_exclusive"])
    valid.loc[valid.index[0], "fight_id"] = sorted(load_holdout_fight_ids())[0]
    constructor = MagicMock()
    monkeypatch.setattr("modeling.train_xgb_candidate.XGBClassifier", constructor)
    with pytest.raises(HoldoutError):
        guarded_fit(temporal, train, fold=fold, parameters={}, rounds=2, validation=valid)
    constructor.assert_not_called()
    with pytest.raises(HoldoutError):
        fit_partition_priors(temporal, valid, expected_ids=sorted(valid.fight_id), end=EVENT_CUTOFF, name="bad")


def test_fold_and_final_prior_isolation_and_native_nan(temporal, tmp_path):
    fold = fold_manifest(temporal, event_cutoff=EVENT_CUTOFF)["folds"][0]
    train, valid, prior = _fold_inputs(temporal, fold, tmp_path)
    original = temporal[temporal.event_date < pd.Timestamp(fold["train_end_exclusive"])]
    assert prior["priors"] == compute_debut_priors(original)
    changed = temporal.copy()
    changed.loc[changed.event_date >= pd.Timestamp(fold["train_end_exclusive"]), "diff_height_cm"] = 99999
    _, _, isolated = _fold_inputs(changed, fold, tmp_path / "changed")
    assert isolated == prior
    final = fit_partition_priors(temporal, temporal, expected_ids=sorted(temporal.fight_id), end=EVENT_CUTOFF, name="final")
    assert final["priors"] == compute_debut_priors(temporal)
    assert final["training_ids_sha256"] != prior["training_ids_sha256"]
    assert final["priors"]["global_height_std"] != prior["priors"]["global_height_std"]
    assert train.debut_prior_win_prob_f1.eq(.5).all()
    assert valid.debut_height_adv.notna().all()
    assert train.scheduled_rounds.isna().all()


def result(params, count, loss, iteration):
    return {"grid_parameters": dict(zip(("max_depth", "min_child_weight", "reg_lambda"), params)),
            "prediction_rows": count, "metrics": {"log_loss": loss}, "best_iteration": iteration}


def test_selection_is_row_weighted_ties_lexicographic_and_rounds_half_up():
    rows = [result((3, 20, .1), 100, .5, 4), result((3, 20, .1), 1, .9, 9),
            result((4, 20, .1), 100, .55, 8), result((4, 20, .1), 1, .6, 8)]
    selected = select_development(rows)
    assert selected["selected_parameters"]["max_depth"] == 3
    assert selected["selected_rounds"] == 8  # median(5,10)=7.5 => 8
    assert selected["winning_weighted_log_loss"] == pytest.approx((100 * .5 + .9) / 101)
    tied = select_development([result((6, 100, 5.), 1, .5, 10), result((3, 50, 1.), 1, .5, 10)])
    assert tied["selected_parameters"] == {"max_depth": 3, "min_child_weight": 50, "reg_lambda": 1.}


def test_best_iteration_prediction_is_explicit():
    model = MagicMock()
    model.best_iteration = 4
    model.predict_proba.return_value = np.array([[.2, .8]])
    frame = pd.DataFrame([{c: 0. for c in FEATURE_ORDER}])
    assert predict_best_iteration(model, frame)[0] == .8
    assert model.predict_proba.call_args.kwargs == {"iteration_range": (0, 5)}


@pytest.mark.parametrize("stage", ["oof", "final"])
def test_frozen_fits_have_no_eval_set_or_early_stopping(temporal, monkeypatch, stage):
    folds = fold_manifest(temporal, event_cutoff=EVENT_CUTOFF)
    fold = folds["folds"][3] if stage == "oof" else None
    train = temporal if fold is None else split_window(temporal, fold["train_end_exclusive"], fold["prediction_end_exclusive"])[0]
    model = MagicMock()
    model.get_booster.return_value.num_boosted_rounds.return_value = 17
    constructor = MagicMock(return_value=model)
    monkeypatch.setattr("modeling.train_xgb_candidate.XGBClassifier", constructor)
    guarded_fit(temporal, train, fold=fold, parameters={"max_depth": 3}, rounds=17)
    assert constructor.call_args.kwargs == {"max_depth": 3, "n_estimators": 17}
    assert model.fit.call_args.kwargs == {"verbose": False}
    if fold:
        valid = split_window(temporal, fold["train_end_exclusive"], fold["prediction_end_exclusive"])[1]
        with pytest.raises(PreflightError, match="early-stopping"):
            guarded_fit(temporal, train, fold=fold, parameters={}, rounds=17, validation=valid)
    with pytest.raises(PreflightError):
        guarded_fit(temporal, train, fold=fold, parameters={"early_stopping_rounds": 50}, rounds=17)


@pytest.fixture
def oof_inputs(temporal, tmp_path):
    folds = fold_manifest(temporal, event_cutoff=EVENT_CUTOFF)
    settings = {"parameters": {"max_depth": 3}, "rounds": 12}
    selection = {"settings_rounds_sha256": digest(settings)}
    records, priors = [], {}
    for fold in folds["folds"][3:]:
        _, valid, prior = _fold_inputs(temporal, fold, tmp_path)
        priors[fold["name"]] = prior
        for row in valid.to_dict("records"):
            records.append({**{k: row[k] for k in ("fight_id", "event_id", "event_date", "fighter_1_id", "fighter_2_id", "label")},
                "fold": fold["name"], "raw_prob_f1": .25 if row["label"] == 0 else .6,
                "settings_rounds_sha256": selection["settings_rounds_sha256"],
                "training_ids_sha256": fold["train_ids_sha256"], "prior_sha256": digest(prior)})
    return pd.DataFrame(records), temporal, folds, selection, priors


@pytest.mark.parametrize("field", ["missing", "duplicate", "label", "fighter_1_id", "fold", "raw_prob_f1",
                                    "settings_rounds_sha256", "training_ids_sha256", "prior_sha256", "event_id", "event_date"])
def test_oof_rejects_bad_coverage_orientation_and_provenance_before_calibration(oof_inputs, monkeypatch, field):
    oof, whole, folds, selection, priors = oof_inputs
    if field == "missing":
        oof = oof.iloc[:-1]
    elif field == "duplicate":
        oof = pd.concat([oof, oof.iloc[:1]])
    else:
        value = 1 - oof.label.iloc[0] if field == "label" else (
            np.nan if field == "raw_prob_f1" else pd.Timestamp("2024-01-01") if field == "event_date" else "altered")
        oof.loc[oof.index[0], field] = value
    constructor = MagicMock()
    monkeypatch.setattr("modeling.train_xgb_candidate.LogisticRegression", constructor)
    with pytest.raises(PreflightError):
        fit_calibrator(oof, whole, folds, selection, priors)
    constructor.assert_not_called()


def test_calibrator_saved_estimator_clipping_and_both_classes(oof_inputs, tmp_path):
    model, details = fit_calibrator(*oof_inputs)
    check_calibrator(model, CALIBRATION_CONTRACT)
    p = np.array([0., 1., 1e-12, .4, .6])
    assert np.isfinite(log_odds(p)).all()
    assert log_odds(p)[0, 0] == log_odds(p)[2, 0]
    assert details["converged"] and details["iterations"] < 1000
    assert len(details["calibration_input_sha256"]) == 64
    assert "calibration_fit_diagnostics" in details
    path = tmp_path / "calibrator.joblib"
    joblib.dump(model, path)
    assert np.array_equal(calibrate(model, p), calibrate(joblib.load(path), p))
    bad = deepcopy(CALIBRATION_CONTRACT)
    bad["clip_epsilon"] = 1e-6
    with pytest.raises(PreflightError):
        check_calibrator(model, bad)
    model.classes_ = np.array([1, 0])
    with pytest.raises(PreflightError):
        calibrate(model, p)
    oof, whole, folds, selection, priors = oof_inputs
    single = oof.copy()
    single["label"] = 1
    whole = whole.copy()
    whole.loc[whole.fight_id.isin(single.fight_id), "label"] = 1
    with pytest.raises(PreflightError, match="both label classes"):
        fit_calibrator(single, whole, folds, selection, priors)


def test_convergence_warning_is_reported_without_changing_settings(oof_inputs, monkeypatch):
    original = LogisticRegression.fit
    def warned(self, x, y):
        fitted = original(self, x, y)
        warnings.warn("forced convergence diagnostic", ConvergenceWarning)
        return fitted
    monkeypatch.setattr(LogisticRegression, "fit", warned)
    model, details = fit_calibrator(*oof_inputs)
    assert not details["converged"]
    assert details["warnings"][0]["category"] == "ConvergenceWarning"
    assert model.C == 1e10 and model.max_iter == 1000


def test_training_path_does_not_open_warehouse_or_frozen_outcomes(monkeypatch, tmp_path):
    import psycopg2
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: pytest.fail("warehouse access"))
    original = Path.open
    accesses = []
    def checked(path, *args, **kwargs):
        accesses.append(str(path))
        if "holdouts" in path.parts and path.name not in {"manifest.json", "predictions.csv", "identity-exclusions.json"}:
            pytest.fail(f"frozen outcome/evidence access: {path}")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "open", checked)
    _, whole, _, folds, _ = preflight(ROOT / "configs/xgb_refit_pre_april_2026.toml",
        snapshot=SNAPSHOT, manifest_sha256=MANIFEST_SHA256, source_mode=SOURCE_MODE)
    fold = folds["folds"][0]
    train, valid, _ = _fold_inputs(whole, fold, tmp_path)
    model = guarded_fit(whole, train, fold=fold, parameters={"n_jobs": 1, "tree_method": "hist"}, rounds=2, validation=valid)
    predict_best_iteration(model, valid)
    assert any("holdouts" in p for p in accesses)


def test_changed_configuration_fails_without_snapshot_load(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    path.write_bytes((ROOT / "configs/xgb_refit_pre_april_2026.toml").read_bytes() + b'\n')
    loader = MagicMock()
    monkeypatch.setattr("modeling.train_xgb_candidate.load_prepared_snapshot", loader)
    with pytest.raises(PreflightError, match="Configuration"):
        preflight(path, snapshot=SNAPSHOT, manifest_sha256=MANIFEST_SHA256, source_mode=SOURCE_MODE)
    loader.assert_not_called()


def test_checksum_manifest_requires_complete_components(tmp_path):
    (tmp_path / "checksums.json").write_text(json.dumps({"schema_version": 1, "self_excluded": "checksums.json", "files": {}}))
    with pytest.raises(PreflightError, match="Required components"):
        verify_checksums(tmp_path)

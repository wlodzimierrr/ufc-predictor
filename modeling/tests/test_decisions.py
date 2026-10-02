"""Fixed-policy tests using probabilities/fixtures, never production scoring."""

import csv
import json
from pathlib import Path
from unittest.mock import MagicMock, Mock

import numpy as np
import pandas as pd
import pytest

from modeling.decisions import (
    DECISION_FIELDS, DECISION_POLICY_VERSION, attach_decisions, decide_prediction,
    decision_metrics, write_prediction_csv,
)
from modeling.uncertainty import confidence_tier, flag_uncertain


@pytest.mark.parametrize("p,label,tier", [
    (0, 0, "high"), (.30, 0, "high"), (.40, None, "toss-up"),
    (.50, None, "toss-up"), (.60, None, "toss-up"), (.70, 1, "high"),
    (1, 1, "high"), (.35, 0, "medium"), (.65, 1, "medium"),
    (np.nextafter(.40, 0), 0, "medium"), (np.nextafter(.40, 1), None, "toss-up"),
    (np.nextafter(.60, 0), None, "toss-up"), (np.nextafter(.60, 1), 1, "medium"),
    (.39999, 0, "medium"), (.60001, 1, "medium"),
])
def test_boundaries(p, label, tier):
    d = decide_prediction(p, "One", "Two")
    assert d["pick_label"] == label
    assert d["decision_status"] == ("no_pick" if label is None else "pick")
    assert d["is_actionable"] == (label is not None)
    assert d["pick_winner_name"] == (None if label is None else ["Two", "One"][label])
    assert d["uncertainty_reasons"] == (["probability_band"] if label is None else [])
    assert d["decision_policy_version"] == DECISION_POLICY_VERSION
    assert confidence_tier([p]).tolist() == [tier]
    assert flag_uncertain([p]).tolist() == [label is None]


@pytest.mark.parametrize("p", [None, "bad", np.nan, np.inf, -np.inf, -.001, 1.001])
def test_invalid_input_cannot_become_pick(p):
    for call in (lambda: decide_prediction(p), lambda: confidence_tier([p]), lambda: flag_uncertain([p])):
        with pytest.raises(ValueError, match=r"finite.*\[0, 1\]"):
            call()


def fixture_frame(probabilities=(.2, .4, .5, .6, .8), labels=(0, 1, 0, None, 1)):
    frame = pd.DataFrame({
        "fight_id": [f"fight-{i}" for i in range(len(probabilities))],
        "event_id": "event", "event_name": "Test Card", "event_date": "2026-08-01",
        "fighter_1_id": "one", "fighter_2_id": "two",
        "fighter_1_name": "One", "fighter_2_name": "Two", "weight_class": None,
        "predicted_prob_f1": probabilities, "calibrated_prob_f1": probabilities,
        "actual_label": labels, "scored_at": "2026-07-31T12:00:00+00:00",
        "model_name": "fixture", "model_artifact": "fixture-artifact",
        "pre_event_evidence": "database_scored_at_before_event",
    })
    frame["confidence_tier"] = confidence_tier(probabilities)
    frame["is_uncertain"] = flag_uncertain(probabilities)
    frame["predicted_label"] = (frame["calibrated_prob_f1"] >= .5).astype(int)
    frame["resolved"] = frame["actual_label"].notna()
    frame["correct"] = frame["predicted_label"].eq(frame["actual_label"]).where(frame["resolved"], None)
    return frame


def test_no_inversion_mutation_or_latent_contract_change():
    source = fixture_frame()
    original = source.copy(deep=True)
    result = attach_decisions(source)
    pd.testing.assert_frame_equal(source, original)
    pd.testing.assert_frame_equal(result[source.columns], original)
    assert result["decision_origin"].eq("derived_from_legacy_probability").all()
    assert result.loc[result["is_uncertain"], "pick_correct"].isna().all()


def test_empty_input():
    result = attach_decisions(pd.DataFrame())
    assert result.empty and set(DECISION_FIELDS) <= set(result.columns)
    assert confidence_tier([]).size == flag_uncertain([]).size == 0
    metrics = decision_metrics(pd.DataFrame())
    assert metrics["total_count"] == 0
    assert metrics["actionable_accuracy"] is metrics["no_pick_share"] is None


def test_json_cli_empty_and_progress_isolated(monkeypatch, tmp_path, capsys):
    import predict

    monkeypatch.setattr(predict, "get_connection", lambda: MagicMock())
    monkeypatch.setattr(predict, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(predict, "upsert", lambda *args, **kwargs: 1)
    monkeypatch.setattr("sys.argv", ["predict.py", "--format", "json"])
    def score(*args):
        print("fixture scoring progress")
        return fixture_frame((.5,), (None,))
    monkeypatch.setattr(predict, "score_upcoming", score)
    predict.main()
    output = capsys.readouterr()
    assert json.loads(output.out)[0]["decision_status"] == "no_pick"
    assert "fixture scoring progress" in output.err
    monkeypatch.setattr(predict, "score_upcoming", lambda *args: pd.DataFrame())
    predict.main()
    assert json.loads(capsys.readouterr().out) == []


def test_json_csv_database_nulls_and_precision(tmp_path, capsys):
    from predict import _database_rows, _output_json

    probabilities = (np.nextafter(.40, 0), .5, np.nextafter(.60, 1))
    source = fixture_frame(probabilities, (0, 1, None))
    result = attach_decisions(source, origin="recorded_at_scoring")
    _output_json(result)
    output = capsys.readouterr().out
    records = json.loads(output)
    assert "NaN" not in output
    assert records[1]["pick_label"] is records[1]["pick_winner_name"] is None
    assert records[1]["weight_class"] is None
    assert [r["calibrated_prob_f1"] for r in records] == list(probabilities)
    assert records[0]["pick_label"] == 0 and records[2]["pick_label"] == 1
    path = tmp_path / "predictions.csv"
    write_prediction_csv(result, path)
    with path.open() as handle:
        csv_rows = list(csv.DictReader(handle))
    assert csv_rows[1]["pick_label"] == csv_rows[1]["pick_winner_name"] == ""
    assert json.loads(csv_rows[1]["uncertainty_reasons"]) == ["probability_band"]
    reloaded = pd.read_csv(path, float_precision="round_trip")
    assert reloaded["calibrated_prob_f1"].tolist() == list(probabilities)
    assert attach_decisions(reloaded)["decision_origin"].eq("recorded_at_scoring").all()
    rows = _database_rows(result)
    assert rows[1]["pick_label"] is rows[1]["pick_winner_name"] is None
    assert rows[0]["calibrated_prob_f1_full"] == probabilities[0]
    assert rows[2]["calibrated_prob_f1_full"] == probabilities[2]


def test_card_no_pick_shows_both_probabilities(capsys):
    from modeling.score_upcoming import _print_card

    _print_card(fixture_frame((.55,), (1,)))
    output = capsys.readouterr().out
    assert "NO PICK" in output and "One: 55.0% | Two: 45.0%" in output
    assert "PICK: One" not in output


def test_scoring_integration_isolated(tmp_path, monkeypatch):
    import modeling.score_upcoming as scorer

    upcoming = tmp_path / "models/upcoming"
    upcoming.mkdir(parents=True)
    source = fixture_frame((.2, .5, .8), (0, 1, 1))
    source["feature"] = [1, 2, 3]
    source.to_csv(upcoming / "upcoming_features.csv", index=False)
    model = Mock()
    model.predict_proba.return_value = np.array([[.8, .2], [.5, .5], [.2, .8]])
    monkeypatch.setattr(scorer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(scorer, "_load_production_info", lambda: {"artifact_path": "fixture", "selected_model": "fixture"})
    monkeypatch.setattr(scorer, "load_model", lambda _: (model, {"feature_cols": ["feature"]}))
    monkeypatch.setattr(scorer, "_fit_calibrator", lambda *args: (np.array([.2, .8]), np.array([0, 1])))
    calibrated = np.array([.25, .55, .75])
    monkeypatch.setattr(scorer, "calibrate_platt", lambda *args: calibrated)
    monkeypatch.setattr(scorer, "_get_fighter_names", lambda *args: {"one": "One", "two": "Two"})
    result = scorer.score_upcoming(None)
    assert result["predicted_prob_f1"].tolist() == [.2, .5, .8]
    assert result["calibrated_prob_f1"].tolist() == calibrated.tolist()
    assert result["pick_label"].tolist() == [0, pd.NA, 1]
    assert result["decision_origin"].eq("recorded_at_scoring").all()
    (upcoming / "upcoming_features.csv").unlink()
    assert scorer.score_upcoming(None).empty


def test_metrics_denominators_and_all_no_pick():
    metrics = decision_metrics(fixture_frame())
    assert metrics["total_count"] == 5 and metrics["resolved_count"] == 4
    assert metrics["latent_correct_count"] == 2 and metrics["latent_accuracy"] == .5
    assert metrics["actionable_count"] == metrics["actionable_resolved_count"] == 2
    assert metrics["actionable_correct_count"] == 2 and metrics["actionable_accuracy"] == 1
    assert metrics["actionable_coverage"] == .4 and metrics["no_pick_share"] == .6
    assert metrics["threshold_high_count"] == 2 and metrics["threshold_high_accuracy"] == 1
    no_pick = decision_metrics(fixture_frame((.4, .5, .6), (0, 1, None)))
    assert no_pick["actionable_count"] == 0 and no_pick["actionable_accuracy"] is None
    assert no_pick["log_loss"] is not None and no_pick["brier_score"] is not None
    pending = decision_metrics(fixture_frame((.2, .5, .8), (None, None, None)))
    assert pending["actionable_count"] == 2 and pending["actionable_correct_count"] == 0
    assert pending["latent_accuracy"] is pending["actionable_accuracy"] is pending["log_loss"] is None


def test_legacy_event_reporting_provenance_and_pending():
    from modeling.build_pre_event_prediction_log import _build_event_log

    source = fixture_frame()
    source.loc[4, "pre_event_evidence"] = "catchup_scored_before_result_load"
    result = _build_event_log(source)
    assert set(result["pre_event_evidence"]) == {"database_scored_at_before_event", "catchup_scored_before_result_load"}
    assert result["total_count"].sum() == 5 and result["n_predicted_fights"].sum() == 4
    no_pick = _build_event_log(fixture_frame((.4, .5), (0, 1))).iloc[0]
    assert no_pick["actionable_count"] == 0 and pd.isna(no_pick["actionable_accuracy"])
    pending = _build_event_log(fixture_frame((.2,), (None,))).iloc[0]
    assert pending["n_predicted_fights"] == 0 and pd.isna(pending["accuracy"])


def test_legacy_saved_report_files_are_only_read(tmp_path, monkeypatch):
    import modeling.build_pre_event_prediction_log as report

    source = fixture_frame((np.nextafter(.4,0),.5,.8), (0,None,1))
    source["event_date"] = "2099-08-01"
    source["scored_at"] = "2099-07-31T12:00:00+00:00"
    folder = tmp_path / "2099-08-01"
    folder.mkdir()
    path = folder / "predictions.csv"
    source.to_csv(path, index=False)
    before = path.read_bytes()
    actuals = source[["fight_id", "event_id", "fighter_1_id", "fighter_2_id", "actual_label", "resolved"]].copy()
    actuals["scraped_at"] = pd.Timestamp("2099-08-02", tz="UTC")
    monkeypatch.setattr(report, "_load_actuals", lambda: actuals)
    monkeypatch.setattr(report, "_load_fighter_names", lambda: {})
    monkeypatch.setattr(report, "PREDICTION_LOG", tmp_path / "missing-log.csv")
    fights, events = report.build_logs(tmp_path, source="csv")
    assert len(fights) == 3 and fights["decision_origin"].eq("derived_from_legacy_probability").all()
    assert fights.iloc[0]["pick_label"] == 0
    assert pd.isna(fights.iloc[1]["pick_correct"])
    assert events.iloc[0]["actionable_accuracy"] == 1
    assert path.read_bytes() == before


def test_legacy_and_pending_summary_only_logs(tmp_path, monkeypatch):
    import modeling.build_pre_event_prediction_log as report

    path = tmp_path / "prediction_log.csv"
    pd.DataFrame([
        {"event_name":"Legacy", "event_date":"2026-08-01", "n_fights":2,
         "accuracy":.5, "log_loss":.7, "brier_score":.25},
        {"event_name":"Pending", "event_date":"2026-08-02", "n_fights":0,
         "accuracy":None, "log_loss":None, "brier_score":None},
    ]).to_csv(path, index=False)
    before = path.read_bytes()
    monkeypatch.setattr(report, "PREDICTION_LOG", path)
    result = report._load_reviewed_event_log(pd.DataFrame())
    assert len(result) == 2 and result["actionable_accuracy"].isna().all()
    assert result["decision_policy_version"].isna().all()
    assert path.read_bytes() == before


@pytest.mark.parametrize("labels", [(1, None), (None, None)])
def test_post_event_review_isolated_single_class_and_unresolved(monkeypatch, labels):
    import modeling.post_event_review as review

    source = fixture_frame((.5, .6), labels)
    source["event_date_str"] = "2026-08-01"
    source["retroactive"] = False
    monkeypatch.setattr(review, "get_connection", lambda: Mock())
    monkeypatch.setattr(review, "_find_predictions", lambda *args: source)
    monkeypatch.setattr(review, "_get_actual_results", lambda *args: {
        f"fight-{i}": {"label": label} for i, label in enumerate(labels)
    })
    saved = []
    monkeypatch.setattr(review, "_append_to_log", saved.append)
    monkeypatch.setattr(review, "_upsert_review_summary", lambda *args: None)
    reviewed = []
    monkeypatch.setattr(review, "_upsert_review_fights", lambda _, frame, __: reviewed.append(frame))
    monkeypatch.setattr(review, "_print_rolling_trend", lambda: None)
    summary = review.review_event("Test Card")
    assert summary["total_count"] == 2 and summary["resolved_count"] == sum(label is not None for label in labels)
    assert summary["no_pick_count"] == 2 and summary["actionable_accuracy"] is None
    assert reviewed[0]["pick_correct"].isna().all()
    if labels[0] is not None:
        assert summary["accuracy"] == 1 and summary["log_loss"] is not None
    else:
        assert summary["accuracy"] is summary["log_loss"] is None


@pytest.mark.parametrize("population,counts", [
    ("historical_2026_apr_aug", (55, 91, 44)),
    ("pre_event_2026_apr_aug", (46, 62, 23)),
])
def test_frozen_prediction_only_counts(population, counts):
    path = Path(__file__).resolve().parents[2] / "data/holdouts" / population / "predictions.csv"
    # No outcomes file is opened or joined for this policy check.
    predictions = pd.read_csv(path, float_precision="round_trip")
    result = attach_decisions(predictions)
    actual = (int((~result["is_actionable"]).sum()), int(result["is_actionable"].sum()),
              int((confidence_tier(predictions["calibrated_prob_f1"]) == "high").sum()))
    assert actual == counts

"""Recovery diagnostics must preserve orientation and exact band boundaries."""

from tools.audit_holdout_recovery import bout_identity, _deduplicate, _prediction_labels, summarize


def test_outcome_join_uses_prediction_fighter_orientation():
    predictions = [{"fight_id": "test-only", "fighter_1_id": "a", "fighter_2_id": "b",
                    "calibrated_prob_f1": "0.75"}]
    actuals = {"test-only": {"fighter_1_id": "b", "fighter_2_id": "a",
                             "fighter_1_outcome": "L", "fighter_2_outcome": "W"}}
    row = _prediction_labels(predictions, actuals)[0]
    assert row["actual_label"] == 1
    assert row["correct"] is True
    assert "actual_label" not in predictions[0]
    actuals["test-only"]["fighter_2_id"] = "replacement"
    assert _prediction_labels(predictions, actuals)[0]["resolved"] is False


def test_recovery_reports_discrepancy_without_selecting_subset():
    rows = [{"fight_id": str(i), "event_date": "2026-04-04", "resolved": True,
             "correct": i % 2 == 0, "calibrated_prob_f1": p}
            for i, p in enumerate(("0.30", "0.40", "0.60", "0.70"))]
    result = summarize(rows)
    assert result["metrics"]["total"] == {"count": 4, "correct": 2}
    assert result["metrics"]["uncertain"] == {"count": 2, "correct": 1}
    assert result["metrics"]["high_confidence"] == {"count": 2, "correct": 1}
    assert result["matches_all_invariants"] is False
    assert len(rows) == 4


def test_recovery_preserves_earliest_and_latest_record_choices():
    rows = [{"fight_id": "test-only", "scored_at": "2026-03-31T00:00:00+00:00"},
            {"fight_id": "test-only", "scored_at": "2026-04-01T00:00:00+00:00"}]
    assert _deduplicate(rows, earliest=True) == [rows[0]]
    assert _deduplicate(rows) == [rows[1]]


def test_recovery_orders_timestamps_by_instant():
    rows = [{"fight_id": "test-only", "scored_at": "2026-04-01T01:30:00+02:00"},
            {"fight_id": "test-only", "scored_at": "2026-03-31 23:45:00+00:00"}]
    assert _deduplicate(rows, earliest=True) == [rows[0]]
    assert _deduplicate(rows) == [rows[1]]


def test_identity_requires_same_event_and_ids_and_records_orientation():
    original = {"event_id": "event-a", "event_date": "2026-07-18", "fighter_1_id": "a", "fighter_2_id": "b"}
    actual = dict(original, fighter_1_id="b", fighter_2_id="a")
    assert bout_identity(original, actual)["orientation_reversed"] is True
    actual["event_id"] = "event-b"
    assert bout_identity(original, actual)["status"] == "unresolved_identity"
    actual.update(event_id="event-a", fighter_2_id="replacement", fighter_2_name="Same name does not prove identity")
    assert bout_identity(original, actual)["unordered_fighter_ids_match"] is False
    assert bout_identity(original, actual)["orientation_reversed"] is None


def test_changed_opponent_is_unresolved_even_when_winner_id_matches():
    predictions = [{"fight_id": "bout", "fighter_1_id": "a", "fighter_2_id": "b", "calibrated_prob_f1": ".75"}]
    actuals = {"bout": {"fighter_1_id": "a", "fighter_2_id": "replacement", "winner_fighter_id": "a", "result_type": "win"}}
    assert _prediction_labels(predictions, actuals)[0]["resolved"] is False

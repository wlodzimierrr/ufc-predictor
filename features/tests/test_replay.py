"""Synthetic adversarial tests; no warehouse or frozen outcomes are read."""

from copy import deepcopy
from datetime import date

import pytest

from features.data_loader import WarehouseData
from features.elo import compute_all_elos
from features.replay import index_source, reconstruct_at_scored_at, reconstruct_bouts
from modeling.data import FEATURE_COLS_V2


def synthetic_source() -> WarehouseData:
    fights = []
    events = []
    for i in range(8):
        event_date = date(2020, i + 1, 1)
        events.append({"event_id": f"event-{i}", "event_date": event_date})
        pairs = [("a", "b"), ("b", "c")] if i == 5 else [("a", "b")]
        if i == 2:
            pairs = [("b", "c")]
        for j, (a, b) in enumerate(pairs):
            fights.append({"fight_id": f"fight-{i}-{j}", "event_id": f"event-{i}",
                           "event_date": event_date, "fighter_1_id": a, "fighter_2_id": b,
                           "result_type": "win", "winner_fighter_id": a if i % 2 else b,
                           "scheduled_rounds": 3, "is_title_fight": False, "weight_class": "lightweight",
                           "finish_method": "decision", "finish_round": 3, "finish_time_seconds": 300})
    fighters = [{"fighter_id": f, "height_cm": 170 + i * 5, "reach_cm": 175 + i * 5,
                 "stance": "orthodox" if i % 2 else "southpaw", "dob": date(1990, 1, 1)}
                for i, f in enumerate(("a", "b", "c"))]
    stats = [{"fight_stat_id": fight["fight_id"] + fid, "fight_id": fight["fight_id"], "fighter_id": fid,
              "sig_strikes_landed": 30 + i * 10, "sig_strikes_attempted": 100,
              "takedowns_landed": 1 + i % 2, "takedowns_attempted": 5, "control_time_seconds": 60}
             for i, fight in enumerate(fights) for fid in (fight["fighter_1_id"], fight["fighter_2_id"])]
    data = WarehouseData(events=events, fighters=fighters, fights=fights, fight_stats=stats)
    index_source(data)
    return data


def features(row):
    return {k: row[k] for k in FEATURE_COLS_V2}


def test_target_future_results_and_stats_cannot_change_earlier_features():
    data = synthetic_source()
    target = data.fight_by_id["fight-5-0"]
    before = features(reconstruct_bouts(data, [target])[0])
    changed = deepcopy(data)
    for f in changed.fights:
        if f["event_date"] >= target["event_date"]:
            f["winner_fighter_id"] = f["fighter_2_id"]
            f["finish_method"] = "submission"
            f["finish_round"] = 1
    for stat in changed.fight_stats:
        if changed.fight_by_id[stat["fight_id"]]["event_date"] >= target["event_date"]:
            stat["sig_strikes_landed"] = 10000
            stat["takedowns_landed"] = 200
    index_source(changed)
    assert before == features(reconstruct_bouts(changed, [changed.fight_by_id[target["fight_id"]]])[0])


def test_opponents_future_history_cannot_change_prior_strength():
    data = synthetic_source()
    target = data.fight_by_id["fight-4-0"]
    before = features(reconstruct_bouts(data, [target])[0])
    changed = deepcopy(data)
    # Opponent c appears in b's prior history, but c's later results must not
    # affect b's historical opponent baseline or opponent Elo.
    f = changed.fight_by_id["fight-5-1"]
    f["winner_fighter_id"] = "c"
    changed.stats_by_fight_fighter[(f["fight_id"], "c")]["sig_strikes_landed"] = 9999
    assert before == features(reconstruct_bouts(changed, [target])[0])


def test_same_day_elo_frozen_and_input_order_independent():
    data = synthetic_source()
    elos = compute_all_elos(data.fights)
    assert elos["fight-5-0"]["b"] == elos["fight-5-1"]["b"]
    assert elos == compute_all_elos(list(reversed(data.fights)))
    changed = deepcopy(data.fights)
    changed[5]["winner_fighter_id"] = changed[5]["fighter_2_id"]
    assert compute_all_elos(changed)["fight-5-1"] == elos["fight-5-1"]


def test_scored_at_caps_elo_and_histories_before_intervening_results():
    data = synthetic_source()
    matchup = data.fight_by_id["fight-7-0"]
    early = reconstruct_at_scored_at(data, matchup, "2020-05-15T12:00:00+00:00")
    changed = deepcopy(data)
    for fight in changed.fights:
        if fight["event_date"] >= date(2020, 5, 15):
            fight["winner_fighter_id"] = fight["fighter_2_id"]
    for stat in changed.fight_stats:
        if changed.fight_by_id[stat["fight_id"]]["event_date"] >= date(2020, 5, 15):
            stat["sig_strikes_landed"] = 99999
    index_source(changed)
    assert features(early) == features(reconstruct_at_scored_at(changed, matchup, early["scored_at"]))
    late = reconstruct_at_scored_at(data, matchup, "2020-07-15T12:00:00+00:00")
    assert features(early) != features(late)
    assert early["availability_certification"] == "unverified_at_scored_at"


def test_scoring_day_results_excluded_with_timezone_conversion():
    data = synthetic_source()
    target = data.fight_by_id["fight-7-0"]
    row = reconstruct_at_scored_at(data, target, "2020-06-02T00:30:00+02:00")
    assert row["history_date_cutoff_exclusive"] == "2020-06-01"
    changed = deepcopy(data)
    changed.fight_by_id["fight-5-0"]["winner_fighter_id"] = "b"
    assert features(row) == features(reconstruct_at_scored_at(changed, target, row["scored_at"]))


def test_current_mutable_profile_is_explicitly_not_made_historical_by_filter():
    data = synthetic_source()
    matchup = data.fight_by_id["fight-4-0"]
    before = reconstruct_at_scored_at(data, matchup, "2020-04-15T12:00:00+00:00")
    data.fighter_by_id["a"]["reach_cm"] = 230
    after = reconstruct_at_scored_at(data, matchup, before["scored_at"])
    assert before["diff_reach_cm"] != after["diff_reach_cm"]
    assert after["availability_certification"] == "unverified_at_scored_at"


def test_scored_at_requires_timezone():
    data = synthetic_source()
    with pytest.raises(ValueError, match="timezone"):
        reconstruct_at_scored_at(data, data.fights[-1], "2020-05-15T12:00:00")

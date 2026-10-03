"""Synthetic, offline admission tests; frozen source records are never loaded."""

from copy import deepcopy
import datetime
from itertools import product
from unittest.mock import patch

import pytest

from warehouse import strict_fight_outcomes_v2 as strict
from warehouse.transform import transform_fight as legacy_transform_fight

MISSING = object()
RESOLVED = [
    ("W", "L", "win", "fighter_1_id"),
    ("L", "W", "win", "fighter_2_id"),
    ("D", "D", "draw", None),
    ("NC", "NC", "nc", None),
]


def fight_row(status="completed", first="W", second="L", **fields):
    row = {
        "fight_id": "synthetic-fight",
        "event_id": "synthetic-event",
        "fighter_1_id": "synthetic-fighter-one",
        "fighter_2_id": "synthetic-fighter-two",
        "fighter_1_outcome": first,
        "fighter_2_outcome": second,
        "bout_type": "  Interim Women's Bantamweight Title Bout  ",
        "num_rounds": "5",
        "primary_finish_method": "submission",
        "secondary_finish_method": "  rear naked choke  ",
        "finish_round": "2",
        "finish_time_minute": "4",
        "finish_time_second": "12",
        "referee": "  Synthetic Referee  ",
        "scraped_at": "2026-08-16 19:52:20 UTC",
        "url": "https://synthetic.invalid/fight",
        "unused": {"nested": ["preserved"]},
    }
    if status is not MISSING:
        row["event_status"] = status
    row.update(fields)
    return row


@pytest.mark.parametrize("status", ["completed", MISSING, None, "", " \t\n"])
@pytest.mark.parametrize("first,second,result,winner_key", RESOLVED)
def test_resolved_mappings_and_historical_missing_status(status, first, second, result, winner_key):
    row = fight_row(status, first, second)
    before = deepcopy(row)
    transformed = strict.transform_fight(row)
    assert transformed["result_type"] == result
    assert transformed["winner_fighter_id"] == (row[winner_key] if winner_key else None)
    assert transformed == legacy_transform_fight(row)
    assert row == before


@pytest.mark.parametrize("first,second", list(product([None, "", " \t\n"], repeat=2)))
def test_explicit_upcoming_with_null_or_blank_outcomes(first, second):
    row = fight_row(" \tupcoming\n", first, second)
    before = deepcopy(row)
    result = strict.transform_fight(row)
    assert result["result_type"] == "upcoming"
    assert result["winner_fighter_id"] is None
    assert row == before


def test_omitted_outcome_keys_require_explicit_upcoming():
    row = fight_row("upcoming")
    del row["fighter_1_outcome"], row["fighter_2_outcome"]
    assert strict.transform_fight(row)["result_type"] == "upcoming"
    del row["event_status"]
    with pytest.raises(strict.FightOutcomeValidationError) as caught:
        strict.transform_fight(row)
    assert caught.value.reason == "missing_status_without_outcomes"


@pytest.mark.parametrize("first,second,result,winner_key", RESOLVED)
def test_whitespace_does_not_change_resolved_orientation(first, second, result, winner_key):
    row = fight_row(" completed\t", f" \t{first}\n", f"\n{second} ")
    transformed = strict.transform_fight(row)
    assert transformed["result_type"] == result
    assert transformed["winner_fighter_id"] == (row[winner_key] if winner_key else None)


INVALID_PAIRS = []
for first, second in product(["", "W", "L", "D", "NC"], repeat=2):
    if (first, second) in {(a, b) for a, b, *_ in RESOLVED}:
        continue
    if not first and not second:
        reason = "completed_without_outcomes"
    elif not first or not second:
        reason = "partial_outcome_pair"
    else:
        reason = "contradictory_outcome_pair"
    INVALID_PAIRS.append((first, second, reason))


@pytest.mark.parametrize("status", ["completed", MISSING])
@pytest.mark.parametrize("first,second,reason", INVALID_PAIRS)
def test_all_empty_partial_and_contradictory_known_pairs(status, first, second, reason):
    if status is MISSING and reason == "completed_without_outcomes":
        reason = "missing_status_without_outcomes"
    row = fight_row(status, first, second)
    before = deepcopy(row)
    with patch.object(strict, "_legacy_transform_fight") as legacy:
        with pytest.raises(strict.FightOutcomeValidationError) as caught:
            strict.transform_fight(row)
        legacy.assert_not_called()
    assert caught.value.reason == reason
    assert caught.value.fight_id == row["fight_id"]
    assert reason in str(caught.value)
    assert row["fight_id"] in str(caught.value)
    assert row == before


@pytest.mark.parametrize("status", [None, "", " \t", "completed", "canceled", " canceled "])
@pytest.mark.parametrize("first,second", list(product([None, "", " \t"], repeat=2)))
def test_empty_outcomes_never_resolve_without_upcoming_status(status, first, second):
    reason = ("canceled_status" if status and status.strip() == "canceled"
              else "completed_without_outcomes" if status == "completed"
              else "missing_status_without_outcomes")
    with pytest.raises(strict.FightOutcomeValidationError) as caught:
        strict.transform_fight(fight_row(status, first, second))
    assert caught.value.reason == reason


@pytest.mark.parametrize("status,reason", [("upcoming", "upcoming_with_outcomes"),
                                         ("canceled", "canceled_status")])
@pytest.mark.parametrize("first,second", [
    (first, second) for first, second in product(["", "W", "L", "D", "NC"], repeat=2)
    if first or second
])
def test_status_outcome_contradictions(status, reason, first, second):
    with pytest.raises(strict.FightOutcomeValidationError) as caught:
        strict.transform_fight(fight_row(status, first, second))
    assert caught.value.reason == reason


@pytest.mark.parametrize("status", ["unknown", "scheduled", "cancelled", "finished", "COMPLETED", 7])
@pytest.mark.parametrize("first,second", [("", "")] + [(a, b) for a, b, *_ in RESOLVED])
def test_unsupported_statuses_do_not_default_to_completed(status, first, second):
    with pytest.raises(strict.FightOutcomeValidationError) as caught:
        strict.transform_fight(fight_row(status, first, second))
    assert caught.value.reason == "unsupported_event_status"


@pytest.mark.parametrize("token", ["win", "loss", "draw", "nc", "N/C", "--", "w", "l", "d", 7])
@pytest.mark.parametrize("status", ["completed", "upcoming", MISSING])
@pytest.mark.parametrize("side", [1, 2])
def test_unsupported_outcome_tokens(status, side, token):
    row = fight_row(status, "W", "L")
    row[f"fighter_{side}_outcome"] = token
    with pytest.raises(strict.FightOutcomeValidationError) as caught:
        strict.transform_fight(row)
    assert caught.value.reason == "unsupported_outcome_token"


@pytest.mark.parametrize("method", ["overturned", "decision", "submission", "ko/tko"])
def test_finish_method_cannot_supply_missing_outcomes(method):
    # Invalid unrelated fields also prove outcome admission runs first.
    row = fight_row("completed", "", "", primary_finish_method=method, num_rounds="invalid")
    with patch.object(strict, "_legacy_transform_fight") as legacy:
        with pytest.raises(strict.FightOutcomeValidationError) as caught:
            strict.transform_fight(row)
        legacy.assert_not_called()
    assert caught.value.reason == "completed_without_outcomes"


@pytest.mark.parametrize("method", ["decision", "ko/tko", "submission", "tko - doctor's stoppage",
                                   "overturned", "could not continue", "dq", "other", "", None])
def test_non_outcome_fields_match_the_preserved_transform(method):
    row = fight_row(primary_finish_method=method)
    before = deepcopy(row)
    actual = strict.transform_fight(row)
    assert actual == legacy_transform_fight(row)
    assert actual["weight_class"] == "women_bantamweight"
    assert actual["is_title_fight"] is True
    assert actual["is_interim_title"] is True
    assert actual["scheduled_rounds"] == 5
    assert actual["finish_round"] == 2
    assert actual["finish_time_seconds"] == 252
    assert actual["finish_detail"] == "rear naked choke"
    assert actual["referee"] == "Synthetic Referee"
    assert actual["scraped_at"] == datetime.datetime(2026, 8, 16, 19, 52, 20,
                                                   tzinfo=datetime.timezone.utc)
    assert row == before


def test_delegation_uses_a_copy_of_the_admitted_row():
    row = fight_row()
    before = deepcopy(row)
    with patch.object(strict, "_legacy_transform_fight", wraps=legacy_transform_fight) as legacy:
        strict.transform_fight(row)
    delegated = legacy.call_args.args[0]
    assert delegated == row
    assert delegated is not row
    assert row == before


@pytest.mark.parametrize("fight_id,scraped_at", [
    ("3c34cdee-2aa5-5cef-b467-3f5ed89b0b0f", "2026-08-16 19:52:20 UTC"),
    ("0ed0563e-a80c-5aa5-a2c4-8c0e818a7273", "2026-08-16 19:52:28 UTC"),
])
def test_synthetic_cancelled_empty_diagnostic_shapes(fight_id, scraped_at):
    # Shape/identity/clock reproduction only; no frozen source or capture reads.
    row = fight_row("canceled", "", "", fight_id=fight_id, scraped_at=scraped_at,
                    primary_finish_method="", secondary_finish_method="", num_rounds="",
                    finish_round="", finish_time_minute="", finish_time_second="")
    before = deepcopy(row)
    assert legacy_transform_fight(row)["result_type"] == "nc"
    with pytest.raises(strict.FightOutcomeValidationError) as caught:
        strict.transform_fight(row)
    assert caught.value.reason == "canceled_status"
    assert caught.value.fight_id == fight_id
    assert row == before

"""Fighter fight-history index.

Builds a per-fighter chronological index over WarehouseData so feature modules
can get a fighter's prior fights at any cutoff date in O(log n) time.

Usage:
    data = load_all_data(conn)
    index = build_fighter_index(data)
    prior = get_history(index, fighter_id, cutoff_date)
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from datetime import date
from typing import Any

from features.data_loader import WarehouseData


Row = dict[str, Any]


@dataclass
class FightHistory:
    """One historical fight entry, from the perspective of one fighter.

    Attributes:
        fight_id:        UUID string
        event_id:        UUID string
        event_date:      date of the event
        result_type:     'win' | 'draw' | 'nc'
        won:             True if this fighter won
        finish_method:   e.g. 'decision', 'ko_tko', 'submission', ...
        finish_round:    round the fight ended (None for decision)
        finish_time_seconds: seconds into the finish round
        scheduled_rounds: 3 or 5
        weight_class:    weight class string or None
        is_title_fight:  bool
        fighter_stats:   stat row for this fighter (or None if missing)
        opponent_stats:  stat row for the opponent (or None if missing)
        opponent_id:     UUID string
    """

    fight_id: str
    event_id: str
    event_date: date
    result_type: str
    won: bool
    finish_method: str | None
    finish_round: int | None
    finish_time_seconds: int | None
    scheduled_rounds: int | None
    weight_class: str | None
    is_title_fight: bool
    fighter_stats: Row | None
    opponent_stats: Row | None
    opponent_id: str


def build_fighter_index(data: WarehouseData) -> dict[str, list[FightHistory]]:
    """Build {fighter_id -> [FightHistory, ...]} sorted by event_date ascending.

    Each fight produces two entries — one for each fighter — so both can access
    their own stats and their opponent's stats.

    Fights without an event_date (broken FK) are skipped with a warning.

    Args:
        data: WarehouseData returned by load_all_data().

    Returns:
        dict mapping fighter_id (str) to a list of FightHistory objects sorted
        chronologically (oldest first).
    """
    index: dict[str, list[FightHistory]] = {}

    for fight in data.fights:
        event_date = fight.get("event_date")
        if event_date is None:
            continue

        f1_id = fight["fighter_1_id"]
        f2_id = fight["fighter_2_id"]
        fight_id = fight["fight_id"]

        f1_stats = data.stats_by_fight_fighter.get((fight_id, f1_id))
        f2_stats = data.stats_by_fight_fighter.get((fight_id, f2_id))

        winner_id = fight.get("winner_fighter_id")

        for fighter_id, opponent_id, my_stats, opp_stats in [
            (f1_id, f2_id, f1_stats, f2_stats),
            (f2_id, f1_id, f2_stats, f1_stats),
        ]:
            entry = FightHistory(
                fight_id=fight_id,
                event_id=fight["event_id"],
                event_date=event_date,
                result_type=fight["result_type"],
                won=(winner_id == fighter_id),
                finish_method=fight.get("finish_method"),
                finish_round=fight.get("finish_round"),
                finish_time_seconds=fight.get("finish_time_seconds"),
                scheduled_rounds=fight.get("scheduled_rounds"),
                weight_class=fight.get("weight_class"),
                is_title_fight=bool(fight.get("is_title_fight")),
                fighter_stats=my_stats,
                opponent_stats=opp_stats,
                opponent_id=opponent_id,
            )
            index.setdefault(fighter_id, []).append(entry)

    # Sort each fighter's history by event_date ascending
    for fights_list in index.values():
        fights_list.sort(key=lambda h: h.event_date)

    return index


def get_history(
    index: dict[str, list[FightHistory]],
    fighter_id: str,
    cutoff_date: date,
) -> list[FightHistory]:
    """Return all fights for fighter_id with event_date < cutoff_date.

    The cutoff_date is exclusive: fights on the same date are excluded.
    This implements the leakage prevention rule from docs/feature-catalog.md.

    Args:
        index:       result of build_fighter_index().
        fighter_id:  UUID string.
        cutoff_date: the event_date of the target fight.

    Returns:
        List of FightHistory sorted oldest-first, all before cutoff_date.
        Empty list if the fighter is debuting or not found.
    """
    fights_list = index.get(fighter_id)
    if not fights_list:
        return []

    # Binary search: find first index where event_date >= cutoff_date
    dates = [h.event_date for h in fights_list]
    cutoff_idx = bisect.bisect_left(dates, cutoff_date)
    return fights_list[:cutoff_idx]

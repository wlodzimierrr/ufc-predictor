"""Fighter snapshot builder.

Orchestrates all feature modules into a single flat dict representing
everything known about one fighter before one fight.

Usage:
    snapshot = build_fighter_snapshot(
        fighter, history, cutoff_date,
        elo_data, fighter_index,
        fighter_id=fid, fight_id=fight_id,
    )
"""

from __future__ import annotations

from datetime import date

from features.career import compute_career_features
from features.rolling import compute_rolling_features
from features.decay import compute_decayed_features
from features.physical import compute_physical_features
from features.elo import get_fighter_elo_features
from features.opponent import compute_opponent_adjusted
from features.history import FightHistory


FEATURE_VERSION = 1


def build_fighter_snapshot(
    fighter: dict,
    history: list[FightHistory],
    cutoff_date: date,
    elo_data: dict[str, dict[str, float]],
    fighter_index: dict[str, list[FightHistory]],
    fighter_id: str,
    fight_id: str,
) -> dict:
    """Build a complete pre-fight feature snapshot for one fighter.

    Calls every feature module and merges results into a single flat dict.
    Keys are prefixed to avoid collisions where necessary.

    Args:
        fighter:        Fighter row from WarehouseData.fighter_by_id.
        history:        Fighter's prior fights (from get_history), sorted oldest-first.
        cutoff_date:    Event date of the target fight.
        elo_data:       Output of compute_all_elos().
        fighter_index:  Full index from build_fighter_index().
        fighter_id:     UUID string of this fighter.
        fight_id:       UUID string of the target fight.

    Returns:
        Flat dict with all snapshot features + metadata.
    """
    career = compute_career_features(history)
    rolling = compute_rolling_features(history)
    decay = compute_decayed_features(history, cutoff_date)
    physical = compute_physical_features(fighter, history, cutoff_date)
    elo = get_fighter_elo_features(elo_data, fighter_id, fight_id, history)
    opponent = compute_opponent_adjusted(history, fighter_index, elo_data)

    snapshot: dict = {}
    snapshot.update(career)
    snapshot.update(rolling)
    snapshot.update(decay)
    snapshot.update(physical)
    snapshot.update(elo)
    snapshot.update(opponent)

    # Metadata
    snapshot["fighter_id"] = fighter_id
    snapshot["fight_id"] = fight_id
    snapshot["as_of_date"] = cutoff_date
    snapshot["feature_version"] = FEATURE_VERSION

    return snapshot

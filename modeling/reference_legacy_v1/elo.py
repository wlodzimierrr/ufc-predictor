"""Sequential Elo rating system for UFC fights.

Processes all fights in chronological order in a single pass, maintaining a
running Elo rating for every fighter. Each fighter starts at `initial` (1500)
and is updated after each fight using the standard Elo formula.

Usage:
    elos = compute_all_elos(data.fights)
    pre_fight_elo = elos[fight_id][fighter_id]
"""

from __future__ import annotations


def compute_all_elos(
    fights: list[dict],
    k: float = 32.0,
    initial: float = 1500.0,
) -> dict[str, dict[str, float]]:
    """Process all fights in date order and return pre-fight Elo for each fighter.

    Args:
        fights:   list of fight dicts (must have event_date, fight_id,
                  fighter_1_id, fighter_2_id, winner_fighter_id, result_type).
        k:        K-factor for Elo updates (default 32).
        initial:  starting Elo for new fighters (default 1500).

    Returns:
        dict mapping fight_id -> {fighter_id: pre_fight_elo} for both fighters.
        Every fight has exactly two entries.
    """
    # Sort by event_date (stable: preserves card order for same-day fights)
    sorted_fights = sorted(fights, key=lambda f: f["event_date"])

    ratings: dict[str, float] = {}          # fighter_id -> current elo
    result: dict[str, dict[str, float]] = {}  # fight_id -> {fid: pre_fight_elo}

    for fight in sorted_fights:
        f1 = fight["fighter_1_id"]
        f2 = fight["fighter_2_id"]
        fight_id = fight["fight_id"]

        r_a = ratings.get(f1, initial)
        r_b = ratings.get(f2, initial)

        # Store pre-fight Elo
        result[fight_id] = {f1: r_a, f2: r_b}

        # Skip no-contests — no rating change
        if fight["result_type"] == "nc":
            continue

        # Expected scores
        e_a = 1.0 / (1.0 + 10.0 ** ((r_b - r_a) / 400.0))
        e_b = 1.0 - e_a

        # Actual scores
        winner = fight.get("winner_fighter_id")
        if fight["result_type"] == "draw":
            s_a = 0.5
            s_b = 0.5
        elif winner == f1:
            s_a = 1.0
            s_b = 0.0
        elif winner == f2:
            s_a = 0.0
            s_b = 1.0
        else:
            # Unknown winner but result_type is 'win' — should not happen
            continue

        # Update ratings
        ratings[f1] = r_a + k * (s_a - e_a)
        ratings[f2] = r_b + k * (s_b - e_b)

    return result


def get_fighter_elo_features(
    elos: dict[str, dict[str, float]],
    fighter_id: str,
    fight_id: str,
    history: list,
    initial: float = 1500.0,
) -> dict:
    """Extract Elo-related features for one fighter entering one fight.

    Args:
        elos:       output of compute_all_elos().
        fighter_id: the fighter's UUID string.
        fight_id:   the target fight's UUID string.
        history:    list of FightHistory for this fighter before cutoff
                    (sorted oldest-first). Used for elo_change_last_fight.
        initial:    initial Elo for missing data (default 1500).

    Returns:
        dict with pre_fight_elo, opponent_pre_fight_elo, elo_change_last_fight.
    """
    fight_elos = elos.get(fight_id, {})

    pre_fight_elo = fight_elos.get(fighter_id, initial)

    # Opponent's pre-fight Elo
    opponent_elo = None
    for fid, elo_val in fight_elos.items():
        if fid != fighter_id:
            opponent_elo = elo_val
            break

    # Elo change from the most recent prior fight
    elo_change_last_fight = None
    if history:
        last = history[-1]
        last_elos = elos.get(last.fight_id, {})
        last_pre = last_elos.get(fighter_id)
        if last_pre is not None:
            elo_change_last_fight = pre_fight_elo - last_pre

    return {
        "pre_fight_elo": pre_fight_elo,
        "opponent_pre_fight_elo": opponent_elo,
        "elo_change_last_fight": elo_change_last_fight,
    }

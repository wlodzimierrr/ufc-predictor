"""Physical, demographic, and activity feature computation.

Derives features from a fighter's profile row and their pre-fight history.
No imputation is performed — missing values pass through as None with a
corresponding _missing flag so Phase 4 models can handle sparsity.

Usage:
    fighter = data.fighter_by_id[fighter_id]
    history = get_history(index, fighter_id, cutoff_date)
    feats = compute_physical_features(fighter, history, cutoff_date)
"""

from __future__ import annotations

from datetime import date

from features.history import FightHistory


def compute_physical_features(
    fighter: dict,
    history: list[FightHistory],
    cutoff_date: date,
) -> dict:
    """Compute physical, demographic, and activity features.

    Args:
        fighter:      Fighter row from WarehouseData.fighter_by_id (plain dict).
        history:      FightHistory sorted oldest-first (output of get_history).
        cutoff_date:  Event date of the target fight.

    Returns:
        dict with all physical/demographic/activity/experience/flag keys.
    """
    # ── Physical ──────────────────────────────────────────────────────────────
    height_cm = fighter.get("height_cm")
    reach_cm = fighter.get("reach_cm")
    weight_lbs = fighter.get("weight_lbs")

    reach_to_height_ratio = (
        float(reach_cm) / float(height_cm)
        if height_cm and reach_cm else None
    )

    # ── Demographic ───────────────────────────────────────────────────────────
    dob = fighter.get("dob")
    if dob is not None:
        # dob may be a date object or a string; normalise to date
        if isinstance(dob, str):
            from datetime import datetime
            dob = datetime.strptime(dob, "%Y-%m-%d").date()
        age_days = (cutoff_date - dob).days
        age_at_fight = age_days / 365.25
        age_squared = age_at_fight ** 2
    else:
        age_at_fight = None
        age_squared = None

    # ── Stance ────────────────────────────────────────────────────────────────
    stance_raw = fighter.get("stance")
    stance = stance_raw.strip().lower() if stance_raw else None

    # ── Activity ──────────────────────────────────────────────────────────────
    is_debut = len(history) == 0

    if history:
        last_fight_date = history[-1].event_date
        days_since_last_fight = (cutoff_date - last_fight_date).days
        is_long_layoff = days_since_last_fight > 365

        first_fight_date = history[0].event_date
        career_span_days = (cutoff_date - first_fight_date).days
        career_span_years = career_span_days / 365.25
        fights_per_year = len(history) / career_span_years if career_span_years > 0 else None
    else:
        days_since_last_fight = None
        is_long_layoff = False
        fights_per_year = None

    # ── Experience ────────────────────────────────────────────────────────────
    ufc_fight_count = len(history)
    five_round_experience = sum(1 for h in history if h.scheduled_rounds == 5)
    title_fight_experience = sum(1 for h in history if h.is_title_fight)

    # ── Missingness flags ─────────────────────────────────────────────────────
    height_missing = height_cm is None
    reach_missing = reach_cm is None
    dob_missing = fighter.get("dob") is None

    return {
        # Physical
        "height_cm": float(height_cm) if height_cm is not None else None,
        "reach_cm": float(reach_cm) if reach_cm is not None else None,
        "weight_lbs": float(weight_lbs) if weight_lbs is not None else None,
        "reach_to_height_ratio": reach_to_height_ratio,

        # Demographic
        "age_at_fight": age_at_fight,
        "age_squared": age_squared,

        # Stance
        "stance": stance,

        # Activity
        "days_since_last_fight": days_since_last_fight,
        "is_long_layoff": is_long_layoff,
        "fights_per_year": fights_per_year,

        # Experience
        "is_debut": is_debut,
        "ufc_fight_count": ufc_fight_count,
        "five_round_experience": five_round_experience,
        "title_fight_experience": title_fight_experience,

        # Missingness flags
        "height_missing": height_missing,
        "reach_missing": reach_missing,
        "dob_missing": dob_missing,
    }

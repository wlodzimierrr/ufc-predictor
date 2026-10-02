"""Forecast replay with separate knowledge and event-date clocks.

The accepted training implementation remains unchanged. Eligibility is capped
before indexing; the existing snapshot functions receive the known event date,
just as they did during training. This module neither fits nor scores anything.
"""

from datetime import date, datetime, timezone

from features.data_loader import WarehouseData
from features.elo import compute_all_elos
from features.history import build_fighter_index, get_history
from features.pipeline import _bout_to_row
from features.snapshot import build_fighter_snapshot


DATE_SEMANTICS = {
    "history_cutoff": "min(event_date, UTC(scored_at).date()), exclusive",
    "feature_reference_date": "original known target event_date",
    "age_activity_rolling_decay": "unchanged training functions at feature_reference_date",
    "elo": "date-frozen, sorted (event_date, fight_id), label-free probe at history_cutoff",
    "opponents": "index capped at history_cutoff; each opponent history strictly before prior bout date",
    "scheduled_rounds": "unknown for every history and target; never inferred",
}
MATCHUP_COLUMNS = (
    "fight_id", "event_id", "fighter_1_id", "fighter_2_id", "event_date",
    "weight_class", "is_title_fight",
)
DEFERRED_COLUMNS = ("debut_prior_win_prob_f1", "debut_height_adv", "debut_reach_adv")


def utc_instant(value: str) -> datetime:
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("scored_at must include a timezone")
    return instant.astimezone(timezone.utc)


def reconstruct_forecast(data: WarehouseData, matchup: dict, scored_at: str) -> tuple[dict, dict]:
    """Use only caller-selected sources, without retaining any target results.

    A missing entire profile or unknown title status is an essential-input gap,
    not an empty profile or a false flag. Individual missing profile fields and
    sparse historical statistics retain the accepted feature semantics.
    """
    target = {k: matchup[k] for k in MATCHUP_COLUMNS}
    if not isinstance(target["event_date"], date):
        raise ValueError("Target event_date must be a date")
    if type(target["is_title_fight"]) is not bool:
        raise ValueError("Unknown target title status is an essential-input gap")
    if not target["weight_class"]:
        raise ValueError("Missing target weight class")
    instant = utc_instant(scored_at)
    cutoff = min(target["event_date"], instant.date())
    ids = (target["fighter_1_id"], target["fighter_2_id"])
    if ids[0] == ids[1] or any(fid not in data.fighter_by_id for fid in ids):
        raise ValueError("Missing fighter profile or invalid target orientation")
    # Remove target identity too, even if corrupt source dates would otherwise
    # admit it. No target result/statistic can enter any index or Elo update.
    prior = [dict(f, scheduled_rounds=None) for f in data.fights
             if f["event_date"] < cutoff and f["fight_id"] != target["fight_id"]
             and f.get("result_type") in {"win", "draw", "nc"}]
    prior_ids = {f["fight_id"] for f in prior}
    stats = {key: value for key, value in data.stats_by_fight_fighter.items() if key[0] in prior_ids}
    index = build_fighter_index(WarehouseData(fights=prior, stats_by_fight_fighter=stats))
    # The probe uses the knowledge clock; snapshots use the event clock.
    probe = dict(target, event_date=cutoff, scheduled_rounds=None,
                 result_type="upcoming", winner_fighter_id=None)
    elos = compute_all_elos(prior + [probe])
    histories = [get_history(index, fid, cutoff) for fid in ids]
    snapshots = [build_fighter_snapshot(data.fighter_by_id[fid], history,
                    target["event_date"], elos, index, fid, target["fight_id"])
                 for fid, history in zip(ids, histories)]
    row = _bout_to_row(dict(target, scheduled_rounds=None), *snapshots)
    row.pop("computed_at")
    row.pop("label")
    row.update({c: None for c in DEFERRED_COLUMNS})
    row.update(event_id=target["event_id"], scored_at=instant.isoformat(),
               history_date_cutoff_exclusive=cutoff.isoformat(),
               feature_reference_date=target["event_date"].isoformat())
    lineage = {
        "fight_id": target["fight_id"], "history_date_cutoff_exclusive": cutoff.isoformat(),
        "feature_reference_date": target["event_date"].isoformat(),
        "prior_fights_for_global_elo": len(prior),
        "last_eligible_resolved_event_date": max((f["event_date"].isoformat() for f in prior), default=None),
        "fighters": [{"fighter_id": fid, "archived_prior_fight_ids": [h.fight_id for h in history],
                       "prior_count": len(history),
                       "missing_own_stat_count": sum(h.fighter_stats is None for h in history),
                       "missing_opponent_stat_count": sum(h.opponent_stats is None for h in history)}
                      for fid, history in zip(ids, histories)],
    }
    return row, lineage

"""Pure, date-conservative reconstruction; never writes warehouse features.

These helpers certify event ordering, not historical source availability.
Profiles, statistics and matchup metadata must be supplied by an independently
versioned source. A current profile is not made historical by a date filter.
"""

from datetime import datetime, timezone

from features.data_loader import WarehouseData
from features.elo import compute_all_elos
from features.history import build_fighter_index, get_history
from features.pipeline import _bout_to_row
from features.snapshot import build_fighter_snapshot


def index_source(data: WarehouseData) -> None:
    """Build the same lookups as the warehouse loader for an isolated source."""
    data.event_by_id = {r["event_id"]: r for r in data.events}
    data.fighter_by_id = {r["fighter_id"]: r for r in data.fighters}
    data.fight_by_id = {r["fight_id"]: r for r in data.fights}
    data.stats_by_fight = {}
    data.stats_by_fight_fighter = {}
    for fight in data.fights:
        fight["event_date"] = data.event_by_id[fight["event_id"]]["event_date"]
    for stat in data.fight_stats:
        data.stats_by_fight.setdefault(stat["fight_id"], []).append(stat)
        data.stats_by_fight_fighter[(stat["fight_id"], stat["fighter_id"])] = stat


def reconstruct_bouts(data: WarehouseData, targets: list[dict]) -> list[dict]:
    """Reconstruct historical rows with strictly earlier histories and Elo."""
    index = build_fighter_index(data)
    elos = compute_all_elos(data.fights)
    rows = []
    for fight in sorted(targets, key=lambda f: (f["event_date"], f["fight_id"])):
        snaps = []
        for fid in (fight["fighter_1_id"], fight["fighter_2_id"]):
            snaps.append(build_fighter_snapshot(
                data.fighter_by_id[fid],
                get_history(index, fid, fight["event_date"]),
                fight["event_date"], elos, index, fid, fight["fight_id"],
            ))
        row = _bout_to_row(fight, *snaps)
        row["event_id"] = fight["event_id"]
        # Computation time is recorded once at publication, never as availability.
        row.pop("computed_at")
        rows.append(row)
    return rows


def reconstruct_at_scored_at(data: WarehouseData, matchup: dict, scored_at: str) -> dict:
    """Date-safe forecast reconstruction, still subject to source availability.

    Histories AND Elo are capped at the earlier of event date and UTC scoring
    date. With date-only result evidence, same-scoring-day fights are excluded.
    Input matchup must retain original forecast identity/orientation. Caller
    must resolve source versions available by the scoring instant separately.
    """
    instant = datetime.fromisoformat(scored_at)
    if instant.tzinfo is None:
        raise ValueError("scored_at must include a timezone")
    cutoff = min(matchup["event_date"], instant.astimezone(timezone.utc).date())
    prior = [f for f in data.fights if f["event_date"] < cutoff]
    # A label-free probe at the scoring date obtains the rating then, instead
    # of the rating at a future event after intervening results.
    probe = dict(matchup, event_date=cutoff, result_type="upcoming", winner_fighter_id=None)
    elos = compute_all_elos(prior + [probe])
    # Only prior fights enter either fighter's or opponents' histories.
    index = build_fighter_index(WarehouseData(fights=prior,
                                             stats_by_fight_fighter=data.stats_by_fight_fighter))
    snaps = [build_fighter_snapshot(
        data.fighter_by_id[fid], get_history(index, fid, cutoff), cutoff,
        elos, index, fid, probe["fight_id"],
    ) for fid in (matchup["fighter_1_id"], matchup["fighter_2_id"])]
    row = _bout_to_row(dict(matchup, result_type="upcoming", winner_fighter_id=None), *snaps)
    row.pop("computed_at")
    row["scored_at"] = instant.astimezone(timezone.utc).isoformat()
    row["history_date_cutoff_exclusive"] = cutoff.isoformat()
    row["availability_certification"] = "unverified_at_scored_at"
    return row

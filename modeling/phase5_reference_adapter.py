"""Pure pinned legacy future features from a caller-supplied frozen capture.

Snapshot date is min(UTC observation date, event date), unlike the challenger.
Legacy sequential within-date/all-capture Elo, stored schedules/defaults,
uncertified histories and profile mutability are deliberately retained.
No live loader, writer, preprocessing fit or probability calculation lives here.
History identity normalization is an input contract shared by both recipes.
Repeated pairs are rejected before indexing, except the evidenced Phase5
same-event rematch. This changes no legacy numeric feature semantics.
"""
from copy import deepcopy
from datetime import date, datetime, timezone

import pandas as pd

from features.data_loader import WarehouseData
from features.debut_prior import apply_debut_features
from modeling.refit_preflight import FEATURE_ORDER, PreflightError, validate_source
from modeling.reference_legacy_v1.history import build_fighter_index, get_history
from modeling.reference_legacy_v1.elo import compute_all_elos
from modeling.reference_legacy_v1.snapshot import build_fighter_snapshot
from modeling.reference_legacy_v1.bout_row import _bout_to_row

VERSION = 'phase5_legacy_reference_adapter_v1'
IDENTITY_CONTRACT = 'phase5_normalized_history_input_v1'


def validate_history_identities(data):
    grouped = {}
    for f in data.fights:
        key = (f['event_id'],*sorted((f['fighter_1_id'],f['fighter_2_id'])))
        grouped.setdefault(key,[]).append(f)
    for key,fs in grouped.items():
        if len(fs)==1: continue
        ids = {f['fight_id'] for f in fs}
        urls = {f.get('source_url','').rstrip('/').split('/')[-1] for f in fs}
        rematch = (len(fs)==2 and key[0]=='fe881b94-92d3-5dbe-97e7-28014b17202d'
            and set(key[1:])=={'0ce3676f-6d84-5bcf-b635-af666b7a7f19','97c74ad5-628e-5da5-bd90-b595e3f60047'}
            and ids=={'2c4d505e-c625-5e27-89f8-e36c2b8224b4','383d786b-c425-517e-a2af-d4cb19081135'}
            and urls=={'ec1bda9a4c2aab42','2750ac5854e8b28b'}
            and all(f['event_date'].isoformat()=='1997-12-21' for f in fs))
        if not rematch:
            raise PreflightError('Reference adapter requires normalized, resolved history identities before indexing')


def build_reference_features(capture: WarehouseData, target_ids: list[str], *,
                             observation_cutoff: str, saved_preprocessing: dict):
    instant = datetime.fromisoformat(observation_cutoff.replace('Z', '+00:00'))
    if instant.tzinfo is None:
        raise PreflightError('Reference observation cutoff requires timezone')
    today = instant.astimezone(timezone.utc).date()
    data = deepcopy(capture)
    # Preserve supplied source order: legacy same-date Elo was order dependent.
    events = {e['event_id']: e for e in data.events}
    fighters = {f['fighter_id']: f for f in data.fighters}
    fights = {f['fight_id']: f for f in data.fights}
    for f in data.fights:
        event = events.get(f.get('event_id'),{})
        if not isinstance(event.get('event_date'),date):
            raise PreflightError('Missing/invalid reference source event date')
        f['event_date'] = event['event_date']
    validate_history_identities(data)
    # validate_source builds lookup/statistic indexes, so it follows the guard.
    validate_source(data)
    data.stats_by_fight_fighter = {(s['fight_id'],s['fighter_id']):s for s in data.fight_stats}
    if len(target_ids) != len(set(target_ids)) or not set(target_ids) <= set(fights):
        raise PreflightError('Missing/duplicate reference target identities')
    index = build_fighter_index(data)
    elos = compute_all_elos(data.fights)
    rows = []
    for fid in target_ids:
        f = fights[fid]
        if f['result_type'] != 'upcoming' or f.get('winner_fighter_id') is not None:
            raise PreflightError('Reference future adapter requires outcome-free targets')
        snapshot_date = min(today, f['event_date'])
        snaps = [build_fighter_snapshot(fighters[pid], get_history(index,pid,snapshot_date),
                  snapshot_date, elos,index,pid,fid) for pid in (f['fighter_1_id'],f['fighter_2_id'])]
        r = _bout_to_row(f,*snaps)
        r.pop('computed_at')
        r.update(event_id=f['event_id'], reference_snapshot_date=snapshot_date.isoformat(),
                 observation_cutoff=instant.isoformat(), recipe_version=VERSION)
        rows.append(r)
    if not rows:
        return pd.DataFrame(columns=FEATURE_ORDER)
    frame = apply_debut_features(pd.DataFrame(rows), saved_preprocessing)
    for c in FEATURE_ORDER: frame[c] = pd.to_numeric(frame[c],errors='raise').astype(float)
    return frame

"""Clearly marked source/model simulations, never real trial registrations."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5
import numpy as np

from modeling.phase5c1_contract import SOURCE_VERSION, digest, encoded, instant, now, sha
from modeling.phase5c1_evidence import DOMAIN


class SyntheticPipelines:
    synthetic = True
    reference_preprocessing = {'base_prior': 0.5, 'height_stats': {}, 'reach_stats': {},
                              'global_height_std': 1.0, 'global_reach_std': 1.0,
                              'training_debut_win_rate': 0.5}

    def __init__(self, *, fail=None):
        self.fail, self.calls = fail, []

    def predict(self, name, frame, **kwargs):
        self.calls.append(name)
        if self.fail == name:
            raise RuntimeError('Synthetic pipeline failure')
        from features.debut_prior import apply_debut_features
        from modeling.phase5c1_adapters import validate_binding
        validate_binding(kwargs['binding'], kwargs['package'], kwargs['projection'], frame, kwargs['contract'], name)
        processed = frame.copy(deep=True) if name == 'reference' else apply_debut_features(frame.copy(deep=True), self.reference_preprocessing)
        raw, calibrated = {'challenger': (0.12345678901234568, 0.7),
                           'reference': (0.6234567890123457, 0.4),
                           'march': (0.5123456789012345, 0.6000000000000001)}[name]
        return processed, np.full(len(frame), raw), np.full(len(frame), calibrated)


def make_package(directory, contract, *, name='capture', rows_override=None, observation=None):
    from modeling.phase5b3_non_influence import synthetic_rows
    from modeling.phase5_current_data import table_bytes
    rows, schemas, _ = synthetic_rows()
    start = instant(observation) if observation else instant(contract['frozen_at']) + timedelta(days=2)
    start = start.replace(hour=12, minute=0, second=0, microsecond=0)
    end = start + timedelta(minutes=10)
    rows['events'][-1]['event_date'] = (end.date() + timedelta(days=7)).isoformat()
    if rows_override is not None:
        rows = rows_override
    events = {e['event_id']: e['event_date'] for e in rows['events']}
    future = [f for f in rows['fights'] if f['result_type'] == 'upcoming' and events[f['event_id']] >= end.date().isoformat()]
    considered = [{k: f[k] for k in ('fight_id', 'event_id', 'fighter_1_id', 'fighter_2_id')} |
                  {'event_date': events[f['event_id']], 'source_row_id': f['fight_id']} for f in future]
    history = [dict(f, event_date=events[f['event_id']]) for f in rows['fights'] if f['result_type'] in {'win', 'draw', 'nc'} and events[f['event_id']] < end.date().isoformat()]
    claims = []
    for t in considered:
        identity = {k: t[k] for k in ('fight_id', 'event_id', 'event_date', 'fighter_1_id', 'fighter_2_id')}
        claims.append(identity | {'is_title_fight': False})
        for pid in (t['fighter_1_id'], t['fighter_2_id']):
            occurrences = [{k: f[k] for k in ('fight_id', 'event_id', 'event_date', 'fighter_1_id', 'fighter_2_id', 'result_type')} for f in history if pid in (f['fighter_1_id'], f['fighter_2_id'])]
            claims.append(identity | {'fighter_id': pid, 'domain': DOMAIN, 'covered_from': '1993-11-12',
                'covered_before_exclusive': end.date().isoformat(), 'prior_occurrences': sorted(occurrences, key=lambda f: f['fight_id']),
                'status': 'verified_history' if occurrences else 'verified_debut', 'complete': True})
    source = encoded({'claims': claims, 'notice': 'SYNTHETIC export; no real observations'})
    observed = (start + timedelta(minutes=5)).isoformat()
    evidence = {'version': 'phase5c1_evidence_package_v1', 'synthetic': True,
                'sources': {'export': {'body': 'evidence/claims.json', 'sha256': sha(source), 'provider': 'synthetic_provider',
                    'url': 'synthetic://authoritative-export', 'authoritative': True, 'status': 200, 'access_blocked': False,
                    'requested_at': start.isoformat(), 'observed_at': observed}},
                'assertions': [{'kind': 'title' if 'is_title_fight' in c else 'experience', 'source_id': 'export',
                    'observed_at': observed, 'claim': c, 'extraction': {'method': 'json_pointer_v1',
                    'pointer': '/claims/' + str(i), 'claim_sha256': digest(c)}} for i, c in enumerate(claims)]}
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=False)
    (d / 'evidence').mkdir()
    (d / 'evidence/claims.json').write_bytes(source)
    (d / 'evidence_package.json').write_bytes(encoded(evidence))
    tables = {}
    for table, values in (rows | {'schemas': schemas}).items():
        body = table_bytes(values)
        (d / (table + '.json')).write_bytes(body)
        tables[table] = {'body': table + '.json', 'sha256': sha(body), 'bytes': len(body),
                         'provider': 'synthetic_provider', 'url': 'synthetic://table/' + table,
                         'requested_at': start.isoformat(), 'observed_at': observed}
    r = {'version': SOURCE_VERSION, 'capture_id': str(uuid5(NAMESPACE_URL, 'phase5c1-synthetic:' + name)),
         'synthetic': True, 'mode': 'authoritative_export', 'provider': 'synthetic_provider',
         'source_scope': 'synthetic_fixture_all_announced_future_rows', 'started_at': start.isoformat(),
         'completed_at': end.isoformat(), 'observation_cutoff': end.isoformat(), 'tables': tables,
         'evidence_body': 'evidence_package.json', 'evidence_sha256': sha(encoded(evidence)), 'considered': considered}
    (d / 'capture_receipt.json').write_bytes(encoded(r))
    return sha(encoded(r)), r

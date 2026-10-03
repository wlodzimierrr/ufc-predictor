"""Offline descriptive coverage; no model inputs, predictions or source requests."""
from collections import Counter
from datetime import timedelta
import csv
import io
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, '/home/wlodzimierrr/ufc-data')
from modeling.phase5c1_contract import (LEGACY, encoded, instant, local_bytes, now,
    publish, require, sha)
from modeling.phase5c1_safety import implementation_guards

p = Path('/tmp/phase5c2-session')
s = json.loads((p / 'session_state.json').read_bytes())
root = Path(s['root'])
with implementation_guards():
    run_dir = root / 'journal/runs/real_intake_v1'
    run = json.loads(local_bytes(run_dir, 'run.json'))
    readiness = json.loads(local_bytes(run_dir, 'readiness.json'))
    projection = json.loads(local_bytes(run_dir, 'projection.json'))
    receipt = json.loads(local_bytes(run_dir, 'capture_receipt.json'))
    targets = {t['source_row_id']: t for t in projection['targets']}
    profiles = {f['fighter_id']: f for f in projection['profiles']}
    considered_by_id = {r['fight_id']: r for r in receipt['considered']}
    lineage = []
    for record in sorted((LEGACY / 'registry/records').glob('*.json')):
        raw = record.read_bytes()
        b = json.loads(raw)['record']['bout']
        observed = considered_by_id.get(b['fight_id'])
        matched = observed is not None and all(b[k] == observed[k] for k in
            ('fight_id', 'event_id', 'fighter_1_id', 'fighter_2_id')) and b['announced_event_date'] == observed['event_date']
        require(matched, 'original_registration_identity_or_date_changed')
        lineage.append({'record': str(record.relative_to(LEGACY)), 'sha256': sha(raw),
            'fight_id': b['fight_id'], 'contemporary_source_row_id': observed['source_row_id'],
            'observed_unchanged_identity_date_orientation': matched,
            'original_record_preserved': True, 'new_cancellation_or_replacement_asserted': False})
    details, fighters, next_bouts = [], {}, []
    timing_reasons = {'less_than_24_hours_before_utc_event_boundary', 'outside_14_day_window'}
    for row in readiness['bouts']:
        t = targets[row['source_row_id']]
        boundary = min(t['event_date'], receipt['observation_cutoff'][:10])
        identity = {k: t[k] for k in ('fight_id', 'event_id', 'fighter_1_id', 'fighter_2_id', 'event_date')}
        event_boundary = instant(t['event_date'] + 'T00:00:00Z')
        entry = {**row, **identity, 'source_url': t['source_url'],
            'expired_announcement': t['event_date'] < receipt['observation_cutoff'][:10],
            'data_blocked': any(reason not in timing_reasons for reason in row['reasons']),
            'timing_blocked': any(reason in timing_reasons for reason in row['reasons']),
            'window_opens_at': (event_boundary - timedelta(days=14)).isoformat(),
            'window_closes_at': (event_boundary - timedelta(hours=24)).isoformat(),
            'title_requirement': {'status': 'UNSUPPLIED', 'required':
                'An explicitly trusted preserved source, observed by the common cutoff, with an explicit true/false title claim bound to all ordered target identities and announced date; warehouse defaults do not qualify.'},
            'fighter_requirements': []}
        for side in (1, 2):
            fid = t[f'fighter_{side}_id']
            history_count = sum(f['event_date'] < boundary and fid in (f['fighter_1_id'], f['fighter_2_id']) for f in projection['history'])
            e = {'side': side, 'fighter_id': fid, 'name': profiles[fid]['full_name'],
                'profile_present': True, 'admitted_prior_occurrence_count': history_count,
                'count_certifies_complete_history': False, 'experience_evidence': 'UNSUPPLIED',
                'required': {'domain': 'admitted_resolved_ufc_occurrences_v1', 'covered_from_on_or_before': '1993-11-12',
                    'covered_before_exclusive': boundary, 'complete': True,
                    'identity_binding': identity,
                    'prior_occurrences': 'Authoritative explicit complete-domain claim matching every admitted prior occurrence identity/result exactly; zero rows require an affirmative complete-domain debut claim.'}}
            entry['fighter_requirements'].append(e)
            fighters.setdefault(fid, {'fighter_id': fid, 'name': e['name'], 'profile_present': True,
                'experience_evidence': 'UNSUPPLIED', 'affected_source_rows': []})['affected_source_rows'].append(t['source_row_id'])
        if entry['timing_blocked']:
            entry['timing_resolution'] = 'This frozen observation cannot be moved or backdated. Expired/late rows remain unscored; early rows require a later separately authorized eligible observation.'
        if 'alias_represented_by_canonical_bout' in row['reasons']:
            entry['canonical_fight_id'] = projection['aliases'][t['fight_id']]
            entry['alias_resolution'] = 'Revalidated whole-row/statistic frozen identity evidence already represents this announcement by its canonical occurrence. No extra forecast or new cancellation/replacement is inferred.'
        details.append(entry)
        if not entry['timing_blocked']:
            next_bouts.append({'event_date': t['event_date'], 'fight_id': t['fight_id'],
                'fighter_1': entry['fighter_requirements'][0]['name'],
                'fighter_2': entry['fighter_requirements'][1]['name'], 'source_row_id': t['source_row_id']})
    summary = {'status': 'BLOCKED', 'prepared_at': now(), 'capture_receipt_sha256': s['capture']['capture_receipt_sha256'],
        'run_checksums_sha256': s['intake']['checksums_sha256'], 'original_registrations': len(lineage),
        'considered_source_rows': len(details), 'considered_unique_raw_fight_ids': len({r['fight_id'] for r in details}),
        'captured_upcoming_canonical_rows': sum('alias_represented_by_canonical_bout' not in r['reasons'] for r in details),
        'future_on_or_after_observation_date': sum(not r['expired_announcement'] for r in details),
        'expired_announcements': sum(r['expired_announcement'] for r in details),
        'timing_blocked': sum(r['timing_blocked'] for r in details), 'timing_in_window': len(next_bouts),
        'future_timing_blocked': sum(r['timing_blocked'] and not r['expired_announcement'] for r in details),
        'data_blocked': sum(r['data_blocked'] for r in details), 'ready': sum(r['status'] == 'READY' for r in details),
        'paired_scored': 0, 'prediction_calls': run['prediction_calls'], 'feature_matrices': 0,
        'failed_or_partial_forecast_attempts': 0, 'eligible_attempt_reservations': 0,
        'unique_affected_fighters': len(fighters), 'missing_title_assertions': len(details),
        'missing_bout_fighter_experience_assertions': sum(len(r['fighter_requirements']) for r in details),
        'overlapping_reason_counts': dict(Counter(reason for r in details for reason in r['reasons'])),
        'historical_comparison': 'STILL_BLOCKED', 'production_unchanged': True,
        'timing_eligible_bouts': sorted(next_bouts, key=lambda r: (r['event_date'], r['fight_id']))}
    text = io.StringIO(newline='')
    writer = csv.writer(text, lineterminator='\n')
    writer.writerow(['source_row_id','fight_id','event_date','fighter_1_id','fighter_1_name','fighter_2_id','fighter_2_name','timing_blocked','data_blocked','status','reasons'])
    for r in details:
        f1,f2 = r['fighter_requirements']
        writer.writerow([r['source_row_id'],r['fight_id'],r['event_date'],f1['fighter_id'],f1['name'],f2['fighter_id'],f2['name'],r['timing_blocked'],r['data_blocked'],r['status'],';'.join(r['reasons'])])
    pin = publish(root / 'coverage', {'coverage_summary.json': encoded(summary),
        'blockers_by_source_bout_fighter.json': encoded(details),
        'blockers_by_fighter.json': encoded(sorted(fighters.values(), key=lambda r: r['fighter_id'])),
        'original_registration_lineage.json': encoded(lineage), 'coverage.csv': text.getvalue().encode(),
        'source_freshness.json': (p / 'source_summary.json').read_bytes(),
        'build_coverage.py': Path(__file__).read_bytes()})
s['coverage_pin'] = pin
(p / 'session_state.json').write_bytes(encoded(s))
print(json.dumps({'coverage_checksums_sha256':pin,**summary},indent=2,sort_keys=True))

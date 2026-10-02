"""Synthetic adversarial proof and durable, nonidentity access-check evidence."""
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from features.history import build_fighter_index, get_history
from features.elo import compute_all_elos
from modeling.phase5_current_data import csv_bytes, table_bytes
from modeling.phase5b1_artifacts import PARENT
from modeling.refit_preflight import PreflightError, json_bytes, sha256


def uid(name):
    return str(uuid5(NAMESPACE_URL, 'phase5b3-synthetic:' + name))


def synthetic_rows():
    """Entirely synthetic values/identities with the accepted SQL field schema."""
    schemas = json.loads((PARENT / 'sources/schemas.json').read_bytes())
    def blank(table):
        return {s['column_name']: None for s in schemas if s['table_name'] == table}
    rows = {n: [] for n in ('events', 'fighters', 'fights', 'fight_stats_aggregate')}
    dates = ['2025-09-01', '2025-10-20', '2025-12-01', '2026-01-20', '2026-03-01',
             '2026-04-18', '2026-05-20', '2026-06-01', '2026-07-20', '2026-08-01', '2026-10-10']
    for i, d in enumerate(dates):
        e = blank('events')
        e.update(event_id=uid('event' + str(i)), event_date=d, event_name='Synthetic event',
                 event_status='completed' if i < 10 else 'upcoming', source_url='synthetic://event/' + str(i), scraped_at='2026-10-02T12:00:00+00:00')
        rows['events'].append(e)
    for i, name in enumerate('abcde'):
        p = blank('fighters')
        p.update(fighter_id=uid(name), full_name='Synthetic ' + name, first_name='Synthetic',
                 last_name=name, height_cm=str(170 + 5*i), reach_cm=str(173 + 4*i), weight_lbs='155',
                 stance='orthodox' if i % 2 else 'southpaw', dob='1990-01-01',
                 source_url='synthetic://profile/' + name, scraped_at='2026-10-02T12:00:00+00:00')
        rows['fighters'].append(p)

    def fight(name, event, a, b, result='win'):
        f = blank('fights')
        f.update(fight_id=uid(name), event_id=rows['events'][event]['event_id'],
                 fighter_1_id=uid(a), fighter_2_id=uid(b), result_type=result,
                 winner_fighter_id=uid(a) if result == 'win' else None,
                 is_title_fight=False, is_interim_title=False, scheduled_rounds=3,
                 weight_class='lightweight', finish_method='decision' if result != 'upcoming' else None,
                 finish_round=3 if result != 'upcoming' else None,
                 finish_time_seconds=300 if result != 'upcoming' else None,
                 source_url='synthetic://fight/' + name, scraped_at='2026-10-02T12:00:00+00:00')
        rows['fights'].append(f)
        for side, p in enumerate((a, b)):
            s = blank('fight_stats_aggregate')
            s.update({k: 0 for k in s if k not in {'fight_stat_id', 'fight_id', 'fighter_id', 'source_url', 'scraped_at'}})
            s.update(fight_stat_id=uid(name + '-' + p), fight_id=f['fight_id'], fighter_id=uid(p),
                     sig_strikes_landed=20 + 3*event + side, sig_strikes_attempted=100,
                     takedowns_landed=1, takedowns_attempted=5, control_time_seconds=60,
                     source_url='synthetic://statistics', scraped_at='2026-10-02T12:00:00+00:00')
            rows['fight_stats_aggregate'].append(s)
        return f
    for i in range(10):
        pair = ('b', 'c') if i in (2, 4) else ('a', 'b')
        fight('resolved' + str(i), i, *pair, result='draw' if i == 5 else 'win')
    fight('same-day', 8, 'b', 'c')
    fight('excluded', 1, 'a', 'd')
    fight('upcoming-april', 5, 'a', 'b', 'upcoming')
    fight('upcoming-earlier', 0, 'b', 'e', 'upcoming')
    fight('future-target', 10, 'a', 'c', 'upcoming')
    return rows, schemas, {uid('excluded')}


def consumer_values(rows, schemas, exclusions):
    from modeling.phase5_role_aware import project_roles, prepare_projection, preprocessing_inputs
    p = project_roles(rows, schemas, [], exclusions)
    frame, folds, experience, data = prepare_projection(p, schemas)
    index = build_fighter_index(data)
    actual_partitions = {}
    for name, entry in preprocessing_inputs(frame, folds).items():
        selected = frame.loc[frame.fight_id.isin(entry['ordered_fight_ids'])]
        actual_partitions[name] = csv_bytes(selected)
    values = {
        'complete_feature_values_and_binary_identities': csv_bytes(frame),
        'experience_debut_and_selected_history_identities': table_bytes(experience),
        'full_fighter_opponent_statistic_histories': table_bytes({k: [asdict(h) for h in hs] for k, hs in sorted(index.items())}),
        'actual_date_frozen_elo_values': json_bytes(compute_all_elos(data.fights)),
        'admitted_statistic_values_and_participants': table_bytes(sorted(data.fight_stats, key=lambda s: s['fight_stat_id'])),
        'oof_membership_and_ordering': json_bytes(folds),
        'preprocessing_selection_identities_and_order': json_bytes(preprocessing_inputs(frame, folds)),
        **{'actual_unfitted_partition_values_' + name: b for name, b in actual_partitions.items()},
    }
    return values, p, frame, data


def prove_non_influence():
    from modeling.phase5_role_aware import project_roles
    rows, schemas, exclusions = synthetic_rows()
    baseline, projection, frame, data = consumer_values(rows, schemas, exclusions)
    past = [f for f in rows['fights'] if f['result_type'] == 'upcoming' and f['fight_id'] != uid('future-target')]
    cases = []
    for mutation in ('add', 'remove', 'reorder', 'alter_metadata', 'alter_participants_and_event', 'rename_ids'):
        changed = deepcopy(rows)
        if mutation == 'add':
            for i in range(3):
                extra = deepcopy(past[0])
                extra.update(fight_id=uid('added' + str(i)), source_url='synthetic://added/' + str(i),
                             scheduled_rounds=5, is_title_fight=True)
                changed['fights'].append(extra)
                for s in rows['fight_stats_aggregate']:
                    if s['fight_id'] == past[0]['fight_id']:
                        extra_stat = dict(s, fight_id=extra['fight_id'], fight_stat_id=uid('addedstat' + str(i) + s['fighter_id']),
                                          sig_strikes_landed=20000, sig_strikes_attempted=30000)
                        changed['fight_stats_aggregate'].append(extra_stat)
        elif mutation == 'remove':
            ids = {f['fight_id'] for f in past}
            changed['fights'] = [f for f in changed['fights'] if f['fight_id'] not in ids]
            changed['fight_stats_aggregate'] = [s for s in changed['fight_stats_aggregate'] if s['fight_id'] not in ids]
        elif mutation == 'reorder':
            changed['fights'].reverse()
            changed['fight_stats_aggregate'].reverse()
        elif mutation == 'alter_metadata':
            ids = {f['fight_id'] for f in past}
            for f in changed['fights']:
                if f['fight_id'] in ids:
                    f.update(weight_class='heavyweight', is_title_fight=True, is_interim_title=True,
                             scheduled_rounds=5, source_url='synthetic://changed/' + f['fight_id'],
                             referee='Synthetic referee', scraped_at='2026-09-01T10:00:00+00:00')
            for s in changed['fight_stats_aggregate']:
                if s['fight_id'] in ids:
                    s.update(sig_strikes_landed=25000, sig_strikes_attempted=30000, control_time_seconds=32000)
        elif mutation == 'alter_participants_and_event':
            ids = {f['fight_id'] for f in past}
            for f in changed['fights']:
                if f['fight_id'] in ids:
                    f.update(event_id=rows['events'][1]['event_id'], fighter_1_id=uid('d'), fighter_2_id=uid('e'))
            for s in changed['fight_stats_aggregate']:
                if s['fight_id'] in ids:
                    original = next(f for f in past if f['fight_id'] == s['fight_id'])
                    s['fighter_id'] = uid('d') if s['fighter_id'] == original['fighter_1_id'] else uid('e')
        else:
            changes = {f['fight_id']: uid('renamed' + f['fight_id']) for f in past}
            for f in changed['fights']:
                f['fight_id'] = changes.get(f['fight_id'], f['fight_id'])
            for s in changed['fight_stats_aggregate']:
                if s['fight_id'] in changes:
                    s.update(fight_id=changes[s['fight_id']], fight_stat_id=uid('renamedstat' + s['fight_stat_id']))
        actual, p, _, _ = consumer_values(changed, schemas, exclusions)
        if actual != baseline:
            dependencies = sorted(k for k in actual if actual[k] != baseline[k])
            raise PreflightError('Non-target upcoming influence: ' + ', '.join(dependencies))
        cases.append({'mutation': mutation, 'all_actual_values_and_selected_identities_equal': True,
                      'consumers_compared': sorted(actual)})
    conflicts = []
    for mutation in ('promote_ambiguous_to_win', 'promote_ambiguous_to_draw', 'resolved_duplicate_new_id'):
        changed = deepcopy(rows)
        if mutation.startswith('promote'):
            f = next(f for f in changed['fights'] if f['fight_id'] == uid('upcoming-april'))
            f.update(result_type='win' if mutation.endswith('win') else 'draw',
                     winner_fighter_id=f['fighter_1_id'] if mutation.endswith('win') else None)
        else:
            f = deepcopy(changed['fights'][0])
            f.update(fight_id=uid('resolved-duplicate-new-id'), source_url='synthetic://changed-resolved-id')
            changed['fights'].append(f)
        try:
            project_roles(changed, schemas, [], exclusions)
        except PreflightError as exc:
            if 'Ambiguous repeated event/participant' not in str(exc):
                raise
            conflicts.append({'mutation': mutation, 'gate': str(exc)})
        else:
            raise PreflightError('Resolved occurrence conflict evaded role gate')
    # Change a separate same-day opponent result and all future results/stats.
    changed = deepcopy(rows)
    target_id = uid('resolved8')
    altered_ids = set()
    for f in changed['fights']:
        if f['fight_id'] in {uid('same-day'), uid('resolved9')}:
            f.update(winner_fighter_id=f['fighter_2_id'], finish_method='submission', finish_round=1, finish_time_seconds=10)
            altered_ids.add(f['fight_id'])
    for s in changed['fight_stats_aggregate']:
        if s['fight_id'] in altered_ids:
            s.update(sig_strikes_landed=90, sig_strikes_attempted=100)
    _, _, other_frame, _ = consumer_values(changed, schemas, exclusions)
    if csv_bytes(frame.loc[frame.fight_id == target_id]) != csv_bytes(other_frame.loc[other_frame.fight_id == target_id]):
        raise PreflightError('Same-day/future outcomes influenced earlier target features')
    index = build_fighter_index(data)
    later = get_history(index, uid('a'), next(f['event_date'] for f in data.fights if f['fight_id'] == target_id))
    if sum(h.fight_id == uid('resolved5') for h in later) != 1 or any(h.fight_id == uid('upcoming-april') for h in later):
        raise PreflightError('Draw/upcoming experience admission violated')
    return {'status': 'PROVEN', 'fixtures': 'synthetic_only', 'mutations': cases,
            'consumer_actual_value_sha256': {k: sha256(v) for k, v in sorted(baseline.items())},
            'conflict_gates': conflicts, 'draw_in_later_history_exactly_once': True,
            'same_day_and_future_outcome_feature_invariance': True,
            'partition_inputs_are_unfitted_actual_values': True,
            'non_target_upcoming_statistics_cannot_supply_defaults': True}


def phase5b2_evidence(directory=None):
    """Use preserved local bytes only, never retry the source or parse its body."""
    original = Path('/tmp/phase5b2-april-gate-vf4sz672/source-check-manifest.json')
    durable = Path('/tmp/phase5b3-role-aware/phase5b2/source-check-manifest.json')
    p = directory / 'phase5b2_evidence/source-check-manifest.json' if directory else (durable if durable.exists() else original)
    if not p.exists():
        return {'phase5b2_evidence_receipt.json': json_bytes({'status': 'NOT_PRESENT', 'identity_evidence': False, 'requests_made': 0})}
    raw = p.read_bytes()
    if sha256(raw) != '39b2f56724d62ba340089762d4876f7f8a90775c61569dd9552b780bdf5ed9ac':
        raise PreflightError('Phase5B2 evidence manifest pin mismatch')
    manifest = json.loads(raw)
    if manifest.get('request_count') != 1 or not manifest.get('no_retries_bypass_proxy_rotation_or_cookies') or not manifest['records'][0].get('access_block_detected'):
        raise PreflightError('Unexpected Phase5B2 source-check disposition')
    out = {'phase5b2_evidence/source-check-manifest.json': raw}
    for r in manifest['records']:
        name = r['body_file']
        if Path(name).name != name:
            raise PreflightError('Phase5B2 evidence path escape')
        body = (p.parent / name).read_bytes()
        if len(body) != r['body_bytes'] or sha256(body) != r['body_sha256']:
            raise PreflightError('Phase5B2 evidence checksum mismatch')
        out['phase5b2_evidence/' + name] = body
    out['phase5b2_evidence_receipt.json'] = json_bytes({'status': 'PRESERVED_VERIFIED',
        'original_manifest_path': str(original), 'manifest_sha256': sha256(raw),
        'identity_evidence': False, 'new_requests_made': 0,
        'meaning': 'Access-check body and allowlisted response metadata only; relationship UNRESOLVED'})
    return out

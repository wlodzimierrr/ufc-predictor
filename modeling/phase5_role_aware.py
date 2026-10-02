"""New role-aware preparation contract. No old readiness gate is overridden.

Raw validation dictionaries are used only for integrity/reference checks.
Computational indexes are created exclusively from validated projections.
"""
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
import json
from pathlib import Path
from uuid import UUID

import numpy as np
import pandas as pd

from features.data_loader import WarehouseData
from features.history import build_fighter_index, get_history
from features.elo import compute_all_elos
from features.replay import index_source, reconstruct_bouts
from modeling.holdout import load_holdout_fight_ids
from modeling.phase5_current_data import (
    CODE_PATHS, META, TABLES, csv_bytes, source_data, table_bytes, verify_checksums,
    versions, now, current_folds,
)
from modeling.phase5_history_identity import (
    alias_conflicts, evidence_payloads, groups, link, REMATCH,
)
from modeling.phase5b1_artifacts import PARENT, PARENT_ROOT, parent_inputs, code_hashes
from modeling.phase5_reference_adapter import validate_history_identities
from modeling.prospective_registry import eligibility, no_predictions_or_outcomes
from modeling.refit_preflight import (
    ROOT, PreflightError, json_bytes, sha256, FEATURE_ORDER, ALGORITHM_VERSION,
    FEATURE_VERSION, DEBUT_COLS, validate_training_frame,
)

VERSION = 'phase5_role_aware_preparation_v2'
POLICY = 'docs/phase5-modeling-eligibility-policy-v2.md'
CONFIG = 'configs/phase5_role_aware_preparation_v2.json'
OUTPUT = ROOT / 'data/experiments/phase5b3_role_aware_preparation'
RECONCILIATION = ROOT / 'data/experiments/phase5b1_reference_and_history/20261002_phase5b1_history_reconciliation_v1_blocked'
RECONCILIATION_ROOT = '8c28425abf011bcce88cff2fa8d3f4d866156396a84a59f9a609050aa16344cb'
REFERENCE = ROOT / 'data/experiments/phase5b1_reference_and_history/20261002_phase5b1_frozen_reference_v3_identity_guarded'
REFERENCE_ROOT = '12c17761fe76998e49fb5e79ea8a455861e22eaf57b37071f692bab17c1ba381'
APRIL_DRAW = '498de4bd-d781-52af-a383-802158196d2d'
APRIL_UPCOMING = '4b08f65d-db68-5091-9568-4748c4cf7318'
CUTOFF = '2026-10-02'
CODE = sorted(set(CODE_PATHS + [
    'modeling/phase5_role_aware.py', 'modeling/phase5b3_safety.py',
    'modeling/phase5b3_non_influence.py', 'tools/prepare_phase5b3_role_aware.py',
    'modeling/tests/test_phase5b3_role_aware.py',
    'modeling/phase5_history_identity.py', 'modeling/phase5b1_artifacts.py',
    'modeling/phase5_reference_adapter.py', 'warehouse/transform.py',
    'features/bout.py', POLICY, CONFIG, 'docs/phase5-history-identity-policy-v1.md',
    'docs/phase5-prospective-shadow-protocol-v1.md',
    'configs/phase5_prospective_shadow_v1.json',
]))


def trusted_inputs():
    """Revalidate accepted identity evidence and lineage, without using its loader."""
    checks = parent_inputs()
    verify_checksums(RECONCILIATION, expected_checksums_sha256=RECONCILIATION_ROOT)
    verify_checksums(REFERENCE, expected_checksums_sha256=REFERENCE_ROOT)
    rebuilt = evidence_payloads()
    if any((RECONCILIATION / n).read_bytes() != b for n, b in rebuilt.items()):
        raise PreflightError('Accepted identity evidence/lineage did not revalidate')
    ledger = json.loads(rebuilt['identity_ledger.json'])
    if ledger['disposition_counts'] != {'proven_duplicate': 12, 'proven_distinct': 1, 'unresolved': 1}:
        raise PreflightError('Accepted identity dispositions changed')
    for n, h in ledger['supporting_evidence_hashes'].items():
        if sha256((ROOT / n).read_bytes()) != h:
            raise PreflightError('Accepted identity evidence bytes changed')
    names = ['sources/' + n + '.json' for n in TABLES] + ['sources/schemas.json']
    raw = {n: (PARENT / n).read_bytes() for n in names}
    rows = {n: json.loads(raw['sources/' + n + '.json']) for n in TABLES}
    by_id = {f['fight_id']: f for f in rows['fights']}
    if by_id[APRIL_DRAW]['result_type'] != 'draw' or by_id[APRIL_UPCOMING]['result_type'] != 'upcoming':
        raise PreflightError('April pinned result states changed')
    config = json.loads((ROOT / CONFIG).read_bytes())
    if config['contract_version'] != VERSION or config['event_cutoff_exclusive'] != CUTOFF:
        raise PreflightError('Role-aware configuration mismatch')
    if sha256((ROOT / config['preregistered_recipe']).read_bytes()) != config['preregistered_recipe_sha256']:
        raise PreflightError('Preregistered recipe changed')
    exclusions = load_holdout_fight_ids()
    if len(exclusions) != 166 or not exclusions <= set(by_id):
        raise PreflightError('All 166 original fitting exclusions must be present')
    return raw, rows, ledger, checks, exclusions


def validate_raw(rows, schemas, cutoff):
    """Validate all raw rows, including unadmitted announcements, without indexing."""
    for table, primary in TABLES.items():
        schema = {s['column_name']: s for s in schemas if s['table_name'] == table}
        if not schema or not isinstance(rows.get(table), list):
            raise PreflightError('Missing source schema/table')
        seen = set()
        for row in rows[table]:
            if set(row) != set(schema):
                raise PreflightError('Raw source column mismatch: ' + table)
            for k, s in schema.items():
                v, t = row[k], s['data_type']
                if v is None:
                    if s['is_nullable'] == 'NO':
                        raise PreflightError('Null nonnullable source field: ' + k)
                    continue
                try:
                    if t == 'uuid':
                        if type(v) is not str or str(UUID(v)) != v:
                            raise ValueError()
                    elif t == 'date':
                        if type(v) is not str or date.fromisoformat(v).isoformat() != v:
                            raise ValueError()
                    elif 'timestamp' in t:
                        if type(v) is not str or datetime.fromisoformat(v).tzinfo is None:
                            raise ValueError()
                    elif t in {'smallint', 'integer', 'bigint'}:
                        bounds = {'smallint': (-32768, 32767), 'integer': (-2**31, 2**31-1), 'bigint': (-2**63, 2**63-1)}
                        if type(v) is not int or not bounds[t][0] <= v <= bounds[t][1]:
                            raise ValueError()
                    elif t == 'numeric':
                        if type(v) not in {str, int, float} or not Decimal(str(v)).is_finite():
                            raise ValueError()
                    elif t == 'boolean':
                        if type(v) is not bool:
                            raise ValueError()
                    elif t == 'text':
                        if type(v) is not str:
                            raise ValueError()
                    else:
                        raise ValueError()
                except (ValueError, TypeError, ArithmeticError) as exc:
                    raise PreflightError('Invalid raw source type: ' + table + '.' + k) from exc
            if row[primary] in seen:
                raise PreflightError('Duplicate raw primary ID: ' + table)
            seen.add(row[primary])
    events = {e['event_id']: e for e in rows['events']}
    profiles = {f['fighter_id'] for f in rows['fighters']}
    fights = {f['fight_id']: f for f in rows['fights']}
    for f in rows['fights']:
        ps = {f['fighter_1_id'], f['fighter_2_id']}
        if f['event_id'] not in events or not ps <= profiles or len(ps) != 2:
            raise PreflightError('Invalid raw fight event/participant reference')
        if f['result_type'] not in {'win', 'draw', 'nc', 'upcoming'} or (
            f['result_type'] == 'win' and f['winner_fighter_id'] not in ps
        ) or (f['result_type'] != 'win' and f['winner_fighter_id'] is not None):
            raise PreflightError('Invalid raw result/winner state')
        if f['result_type'] != 'upcoming' and events[f['event_id']]['event_date'] >= cutoff:
            raise PreflightError('Resolved result at/after exclusive capture-date cutoff')
        for k, lower in [('finish_round', 1), ('finish_time_seconds', 0), ('scheduled_rounds', 1)]:
            if f[k] is not None and f[k] < lower:
                raise PreflightError('Invalid raw duration/schedule value')
    pairs = set()
    for s in rows['fight_stats_aggregate']:
        f = fights.get(s['fight_id'])
        if not f or s['fighter_id'] not in (f['fighter_1_id'], f['fighter_2_id']):
            raise PreflightError('Invalid raw statistic reference')
        pair = (s['fight_id'], s['fighter_id'])
        if pair in pairs:
            raise PreflightError('Duplicate raw statistic participant pair')
        pairs.add(pair)
        for k, v in s.items():
            if type(v) is int and v < 0:
                raise PreflightError('Negative raw statistic')
            if k.endswith('_landed') and v > s[k.replace('_landed', '_attempted')]:
                raise PreflightError('Raw landed exceeds attempted')
    for p in rows['fighters']:
        if any(p[k] is not None and Decimal(str(p[k])) <= 0 for k in ('height_cm', 'reach_cm', 'weight_lbs')):
            raise PreflightError('Invalid raw physical value')


def normalize_proven(rows, decisions, exclusions):
    """Only accepted proven aliases are normalized; unresolved rows stay original."""
    fights = {f['fight_id']: f for f in rows['fights']}
    alias = {fid: fid for fid in fights}
    expanded = set(exclusions)
    events = {e['event_id']: e for e in rows['events']}
    visited = set()
    for d in decisions:
        if d['disposition'] == 'unresolved':
            continue
        ids = d['original_ids']
        if len(set(ids)) != len(ids) or visited.intersection(ids):
            raise PreflightError('Overlapping accepted identity decisions')
        visited.update(ids)
        if not set(ids) <= set(fights):
            raise PreflightError('Accepted occurrence missing; fresh validation required')
        fs = [fights[fid] for fid in ids]
        if {(f['event_id'], *sorted((f['fighter_1_id'], f['fighter_2_id']))) for f in fs} != {
            (d['event_id'], *sorted(d['participant_ids']))
        }:
            raise PreflightError('Accepted identity participant scope changed')
        if events[d['event_id']]['event_date'] != d.get('event_date', events[d['event_id']]['event_date']):
            raise PreflightError('Accepted identity event date changed')
        if len(d.get('source_rows', [])) != len(ids):
            raise PreflightError('Accepted whole-row lineage missing')
        for entry in d['source_rows']:
            fid = entry['row']['fight_id']
            actual_stats = sorted((sha256(table_bytes([s])) for s in rows['fight_stats_aggregate'] if s['fight_id'] == fid))
            if entry['row_sha256'] != sha256(table_bytes([fights[fid]])) or actual_stats != sorted(s['row_sha256'] for s in entry['statistics']):
                raise PreflightError('Accepted identity row/statistic changed; fresh validation required')
        if d['disposition'] == 'proven_distinct':
            if not accepted_rematch(d, fs):
                raise PreflightError('Unsupported distinct occurrence claim')
            continue
        if d['disposition'] != 'proven_duplicate' or d.get('proof_kind') != 'manual_announcement_to_unique_source_occurrence' or len(d.get('card_occurrences', [])) != 1 or len(d.get('evidence', [])) < 2:
            raise PreflightError('Unsupported alias evidence')
        if alias_conflicts(fs, rows['fight_stats_aggregate']):
            raise PreflightError('Conflicting alias rows/statistics')
        can = d['canonical_id']
        if can not in ids or link(fights[can]['source_url']) != d['card_occurrences'][0]['occurrence']:
            raise PreflightError('Unsupported canonical occurrence')
        for fid in ids:
            alias[fid] = can
        if expanded.intersection(ids):
            expanded.update(ids)
    normalized = deepcopy(rows)
    normalized['fights'] = [deepcopy(f) for f in rows['fights'] if alias[f['fight_id']] == f['fight_id']]
    normalized['fight_stats_aggregate'] = [deepcopy(s) for s in rows['fight_stats_aggregate'] if alias[s['fight_id']] == s['fight_id']]
    return normalized, alias, frozenset(expanded)


def accepted_rematch(d, fs):
    """Exact evidenced contract, independent of an asserted disposition label."""
    return (d.get('proof_kind') == 'explicit_two_source_occurrences'
        and d.get('event_id') == 'fe881b94-92d3-5dbe-97e7-28014b17202d'
        and d.get('event_date') == '1997-12-21'
        and set(d.get('participant_ids', [])) == {'0ce3676f-6d84-5bcf-b635-af666b7a7f19', '97c74ad5-628e-5da5-bd90-b595e3f60047'}
        and set(d['original_ids']) == REMATCH and len(fs) == 2
        and {link(f['source_url']) for f in fs} == {'ufcstats.com/fight-details/ec1bda9a4c2aab42', 'ufcstats.com/fight-details/2750ac5854e8b28b'}
        and {link(f['source_url']) for f in fs} == set(d.get('occurrence_by_id', {}).values()))


def validate_occurrences(fights, decisions, role):
    if len({f['fight_id'] for f in fights}) != len(fights):
        raise PreflightError('Duplicate ' + role + ' fight IDs')
    distinct = [d for d in decisions if d['disposition'] == 'proven_distinct']
    for key, fs in groups(fights).items():
        matching = [d for d in distinct if (d['event_id'], *sorted(d['participant_ids'])) == key and set(d['original_ids']) == {f['fight_id'] for f in fs}]
        if len(matching) != 1 or not accepted_rematch(matching[0], fs):
            raise PreflightError('Ambiguous repeated event/participant occurrences in ' + role)


def project_roles(rows, schemas, decisions, exclusions, cutoff=CUTOFF):
    validate_raw(rows, schemas, cutoff)
    normalized, alias, expanded = normalize_proven(rows, decisions, exclusions)
    events = {e['event_id']: e for e in rows['events']}
    history, fitting, future = [], [], []
    for f in normalized['fights']:
        d = events[f['event_id']]['event_date']
        if d < cutoff and f['result_type'] in {'win', 'draw', 'nc'}:
            history.append(f)
            if f['result_type'] == 'win' and f['fight_id'] not in expanded:
                fitting.append(f)
        if d >= cutoff and f['result_type'] == 'upcoming':
            future.append(f)
    for role, fs in [('history', history), ('fitting', fitting), ('future_targets', future)]:
        validate_occurrences(fs, decisions, role)
        fs.sort(key=lambda f: (events[f['event_id']]['event_date'], f['fight_id']))
    history_ids = {f['fight_id'] for f in history}
    fitting_ids = {f['fight_id'] for f in fitting}
    future_ids = {f['fight_id'] for f in future}
    historical_stats = [s for s in normalized['fight_stats_aggregate'] if s['fight_id'] in history_ids]
    admissions = []
    for f in sorted(rows['fights'], key=lambda f: f['fight_id']):
        fid, can = f['fight_id'], alias[f['fight_id']]
        d = events[f['event_id']]['event_date']
        record = {'fight_id': fid, 'canonical_id': can, 'event_id': f['event_id'],
                  'event_date': d, 'result_type': f['result_type'],
                  'orientation': [f['fighter_1_id'], f['fighter_2_id']],
                  'source_row_sha256': sha256(table_bytes([f])), 'roles': {}}
        for role, admitted in [('history', history_ids), ('fitting', fitting_ids), ('future_targets', future_ids)]:
            reasons = []
            if fid != can:
                reasons.append('proven_alias_represented_once_by_canonical_whole_row')
            if role in {'history', 'fitting'}:
                if d >= cutoff:
                    reasons.append('event_on_or_after_exclusive_cutoff')
                if f['result_type'] not in ({'win'} if role == 'fitting' else {'win', 'draw', 'nc'}):
                    reasons.append('ineligible_result_' + f['result_type'])
                if role == 'fitting' and fid in expanded:
                    reasons.append('alias_expanded_166_fitting_exclusion')
            else:
                if d < cutoff:
                    reasons.append('past_dated_source_record_coverage_only')
                if f['result_type'] != 'upcoming':
                    reasons.append('resolved_record_not_outcome_free_future_target')
            record['roles'][role] = {'admitted': fid in admitted,
                'represented_by': can if can in admitted else None,
                'reasons': reasons or ['eligible_' + role]}
        admissions.append(record)
    stat_ledger = [{'fight_stat_id': s['fight_stat_id'], 'fight_id': s['fight_id'],
        'fighter_id': s['fighter_id'], 'canonical_fight_id': alias[s['fight_id']],
        'source_row_sha256': sha256(table_bytes([s])),
        'admitted': s['fight_id'] in history_ids,
        'reason': 'admitted_history_participant_statistic' if s['fight_id'] in history_ids else
                  ('proven_alias_statistic_represented_by_canonical_whole_row' if alias[s['fight_id']] != s['fight_id'] else 'fight_not_admitted_to_history')}
        for s in sorted(rows['fight_stats_aggregate'], key=lambda s: s['fight_stat_id'])]
    # The public future projection contains no result, winner, finish or stats.
    profiles = {f['fighter_id'] for f in rows['fighters']}
    future_metadata = []
    for f in future:
        m = {k: f[k] for k in ('fight_id', 'event_id', 'fighter_1_id', 'fighter_2_id', 'weight_class', 'source_url')}
        m.update(announced_event_date=events[f['event_id']]['event_date'],
                 is_title_fight=None, title_evidence=None,
                 warehouse_stored_title_flag=f['is_title_fight'], source_scraped_at=f['scraped_at'])
        for side in (1, 2):
            m.update({f'fighter_{side}_profile_present': f[f'fighter_{side}_id'] in profiles,
                      f'fighter_{side}_experience_status': 'unverified',
                      f'fighter_{side}_experience_evidence': None})
        no_predictions_or_outcomes(m)
        future_metadata.append(m)
    return {'normalized': normalized, 'history': history, 'fitting': fitting,
            'future_source_rows': future, 'future_metadata': future_metadata,
            'historical_statistics': historical_stats, 'admissions': admissions,
            'statistic_admissions': stat_ledger, 'aliases': {k: v for k, v in alias.items() if k != v},
            'expanded_exclusions': expanded}


def computational_history(projection, schemas):
    """Only this validated history projection reaches computational indexes."""
    raw = {n: table_bytes(projection['normalized'][n]) for n in ('events', 'fighters')}
    raw.update(fights=table_bytes(projection['history']),
               fight_stats_aggregate=table_bytes(projection['historical_statistics']))
    payloads = {'sources/' + n + '.json': b for n, b in raw.items()}
    payloads['sources/schemas.json'] = table_bytes(schemas)
    data = source_data(payloads)
    for f in data.fights:
        f['scheduled_rounds'] = None
        fr, ft = f['finish_round'], f['finish_time_seconds']
        f['elapsed_duration_seconds'] = (fr - 1) * 300 + ft if fr is not None and ft is not None else None
    index_source(data)
    return data


def prepare_projection(projection, schemas, cutoff=CUTOFF):
    data = computational_history(projection, schemas)
    targets = [data.fight_by_id[f['fight_id']] for f in projection['fitting']]
    frame = pd.DataFrame(reconstruct_bouts(data, targets))
    if frame.empty:
        raise PreflightError('No fitting targets')
    for c in FEATURE_ORDER:
        frame[c] = np.nan if c in DEBUT_COLS else pd.to_numeric(frame[c], errors='raise').astype(float)
    frame['event_date'] = pd.to_datetime(frame.event_date)
    frame = frame[META + FEATURE_ORDER].sort_values(['event_date', 'fight_id']).reset_index(drop=True)
    # Pair validation happened before any index. Binary validation retains all
    # old strict schema/date/label/numeric/exclusion checks without overrides.
    validate_training_frame(frame, event_cutoff=cutoff)
    if set(frame.fight_id) & projection['expanded_exclusions']:
        raise PreflightError('Excluded alias entered fitting/preprocessing')
    if not frame[DEBUT_COLS + ['scheduled_rounds']].isna().all().all():
        raise PreflightError('Schedules/debut columns must be unknown/deferred')
    for r, f in zip(frame.to_dict('records'), targets):
        if any(r[k] != f[k] for k in ('fight_id', 'event_id', 'fighter_1_id', 'fighter_2_id')) or r['label'] != int(f['winner_fighter_id'] == f['fighter_1_id']):
            raise PreflightError('Reconstruction changed identity/orientation/label')
    folds = current_folds(frame, cutoff)
    folds.update(contract_version=VERSION, membership_stage='validated_role_projections', diagnostic_only=False)
    index = build_fighter_index(data)
    experience = []
    for f in targets:
        sides = {}
        for fid in (f['fighter_1_id'], f['fighter_2_id']):
            prior = get_history(index, fid, f['event_date'])
            sides[fid] = [h.fight_id for h in prior]
        experience.append({'fight_id': f['fight_id'], 'strictly_earlier_histories_by_fighter': sides})
    return frame, folds, experience, data


def preprocessing_inputs(frame, folds):
    """Ordered, unfitted inputs for every later partition and calibration role."""
    def entry(ids):
        selected = frame.loc[frame.fight_id.isin(ids)]
        if set(selected.fight_id) != set(ids) or len(selected) != len(ids):
            raise PreflightError('Partition identity selection mismatch')
        return {'ordered_fight_ids': list(selected.fight_id), 'rows': len(selected),
                'matrix_with_identity_sha256': sha256(csv_bytes(selected)),
                'fit_executed': False}
    result = {'final_training': entry(list(frame.fight_id))}
    all_oof = []
    for f in folds['folds']:
        result[f['name'] + '_train_preprocessing'] = entry(f['train_fight_ids'])
        result[f['name'] + '_oof_transform'] = entry(f['prediction_fight_ids'])
        all_oof += result[f['name'] + '_oof_transform']['ordered_fight_ids']
    result['later_calibration_oof'] = entry(all_oof)
    result['later_calibration_oof']['prediction_order'] = all_oof
    return result


def partition_role_admissions(admissions, folds):
    """Every original source row receives reasons for each later learner role."""
    fitting_ids = {r['fight_id'] for r in admissions if r['roles']['fitting']['admitted']}
    memberships = {'final_fitting': fitting_ids, 'final_preprocessing': fitting_ids}
    for f in folds['folds']:
        memberships[f['name'] + '_fitting_and_preprocessing'] = set(f['train_fight_ids'])
        memberships[f['name'] + '_prediction_and_transform'] = set(f['prediction_fight_ids'])
    memberships['calibration_oof'] = set().union(*(set(f['prediction_fight_ids']) for f in folds['folds']))
    records = []
    for r in admissions:
        base = r['roles']['fitting']
        roles = {}
        for name, ids in memberships.items():
            admitted = r['fight_id'] in ids
            roles[name] = {'admitted': admitted, 'reasons': ['eligible_' + name] if admitted else
                          (base['reasons'] if not base['admitted'] else ['outside_preregistered_calendar_partition'])}
        records.append({'fight_id': r['fight_id'], 'canonical_id': r['canonical_id'], 'roles': roles})
    return records


def reference_input_check(projection, schemas):
    """Check identities only, never real target feature values or probabilities."""
    payloads = {'sources/' + n + '.json': table_bytes(projection['normalized'][n]) for n in ('events', 'fighters')}
    payloads.update({'sources/fights.json': table_bytes(projection['history'] + projection['future_source_rows']),
                     'sources/fight_stats_aggregate.json': table_bytes(projection['historical_statistics']),
                     'sources/schemas.json': table_bytes(schemas)})
    data = source_data(payloads)
    events = {e['event_id']: e for e in data.events}
    for f in data.fights:
        f['event_date'] = events[f['event_id']]['event_date']
    validate_history_identities(data)
    return {'identity_input_contract': 'phase5_normalized_history_input_v1',
            'structural_compatibility': 'READY', 'adapter_changes_required': [],
            'packaging': 'admitted history plus selected outcome-free target source rows; history-scoped statistics; retain original legacy schedules',
            'real_adapter_feature_calls': 0, 'saved_component_loads': 0,
            'forecast_readiness': 'BLOCKED',
            'limitations': ['title and experience remain unattested', 'capture predates reference component freezes',
                            'new challenger not fit', 'legacy numeric recipe and source-order semantics unchanged']}


def build_payloads(*, evidence_directory=None):
    from modeling.phase5b3_non_influence import prove_non_influence
    raw, rows, ledger, checks, exclusions = trusted_inputs()
    schemas = json.loads(raw['sources/schemas.json'])
    proof = prove_non_influence()
    projection = project_roles(rows, schemas, ledger['groups'], exclusions)
    frame, folds, experience, data = prepare_projection(projection, schemas)
    preprocessing = preprocessing_inputs(frame, folds)
    summary = {'source_fights': len(rows['fights']), 'normalized_fights': len(projection['normalized']['fights']),
        'history_rows': len(projection['history']), 'history_result_counts': dict(Counter(f['result_type'] for f in projection['history'])),
        'history_statistics': len(projection['historical_statistics']), 'fitting_rows': len(frame),
        'fitting_exclusion_count': len(exclusions), 'expanded_exclusion_count': len(projection['expanded_exclusions']),
        'normalized_upcoming_rows': sum(f['result_type'] == 'upcoming' for f in projection['normalized']['fights']),
        'raw_past_dated_upcoming': sum(f['result_type'] == 'upcoming' and next(e for e in rows['events'] if e['event_id'] == f['event_id'])['event_date'] < CUTOFF for f in rows['fights']),
        'future_considered': len(projection['future_metadata']), 'oof_rows': folds['oof_rows'],
        'events': int(frame.event_id.nunique()), 'dates': int(frame.event_date.nunique()),
        'label_counts': {str(k): int(v) for k, v in frame.label.value_counts().sort_index().items()},
        'both_debuting_rows': int(frame.both_debuting.eq(1).sum()),
        'latest_binary_event': frame.event_date.max().date().isoformat(),
        'resolved_histories_missing_participant_stats': sum(any((f['fight_id'], fid) not in data.stats_by_fight_fighter for fid in (f['fighter_1_id'], f['fighter_2_id'])) for f in data.fights),
        'excluded_resolved_history_ids': sorted(f['fight_id'] for f in projection['history'] if f['fight_id'] in projection['expanded_exclusions'])}
    april_history = [h for h in data.fights if h['fight_id'] in {APRIL_DRAW, APRIL_UPCOMING}]
    if [h['fight_id'] for h in april_history] != [APRIL_DRAW] or not set(frame.fight_id).isdisjoint({APRIL_DRAW, APRIL_UPCOMING}):
        raise PreflightError('April role admissions violate policy')
    future_checks = [{'fight_id': b['fight_id'], 'reasons': eligibility(b, observation_cutoff=json.loads((PARENT / 'training_manifest.json').read_bytes())['capture_completed_at'])} for b in projection['future_metadata']]
    parent_future = json.loads((PARENT / 'registry/considered_bouts.json').read_bytes())
    if sorted(parent_future, key=lambda b: b['fight_id']) != sorted(projection['future_metadata'], key=lambda b: b['fight_id']):
        raise PreflightError('Prospective consideration/metadata changed')
    old = pd.read_csv(PARENT / 'training.csv', float_precision='round_trip')
    old['event_date'] = pd.to_datetime(old.event_date)
    old_fold = json.loads((PARENT / 'folds.json').read_bytes())
    differences = {'prior_diagnostic_rows': len(old), 'new_fitting_rows': len(frame),
        'added_ids': sorted(set(frame.fight_id) - set(old.fight_id)), 'removed_ids': sorted(set(old.fight_id) - set(frame.fight_id)),
        'feature_csv_byte_identical': csv_bytes(old) == csv_bytes(frame),
        'prior_oof_rows': old_fold['oof_rows'], 'new_oof_rows': folds['oof_rows'],
        'prior_final_membership_sha256': old_fold['final_training_ids_sha256'],
        'new_final_membership_sha256': folds['final_training_ids_sha256'],
        'prior_oof_membership_sha256': old_fold['oof_ids_sha256'], 'new_oof_membership_sha256': folds['oof_ids_sha256']}
    if set(old.fight_id) == set(frame.fight_id):
        a, b = old.set_index('fight_id')[FEATURE_ORDER], frame.set_index('fight_id')[FEATURE_ORDER].loc[list(old.fight_id)]
        mask = ~(a.eq(b) | (a.isna() & b.isna()))
        differences.update(changed_feature_cells=int(mask.to_numpy().sum()),
                           changed_feature_fight_ids=list(mask.index[mask.any(axis=1)]))
    out = {**raw, 'training.csv': csv_bytes(frame), 'folds.json': json_bytes(folds),
        'role_admissions.json': table_bytes(projection['admissions']),
        'partition_role_admissions.json': table_bytes(partition_role_admissions(projection['admissions'], folds)),
        'statistic_admissions.json': table_bytes(projection['statistic_admissions']),
        'history_projection.json': table_bytes(projection['history']),
        'historical_statistics_projection.json': table_bytes(projection['historical_statistics']),
        'fitting_projection.json': table_bytes(projection['fitting']),
        'future_targets.json': table_bytes(projection['future_metadata']),
        'future_eligibility.json': json_bytes(future_checks),
        'history_consumer_lineage.json': table_bytes(experience),
        'preprocessing_inputs.json': json_bytes(preprocessing),
        'accepted_identity_ledger.json': (RECONCILIATION / 'identity_ledger.json').read_bytes(),
        'accepted_identity_lineage.json': (RECONCILIATION / 'source_to_derived_lineage.json').read_bytes(),
        'unresolved_identity_ledger.json': json_bytes({'status': 'UNRESOLVED', 'groups': [d for d in ledger['groups'] if d['disposition'] == 'unresolved'],
            'relationship_resolved': False, 'new_aliases': [], 'cancellations': [], 'preferred_occurrences': [],
            'april_original_states': {APRIL_DRAW: 'draw', APRIL_UPCOMING: 'upcoming'}}),
        'normalization.json': json_bytes({'aliases': projection['aliases'], 'expanded_fitting_exclusion_ids': sorted(projection['expanded_exclusions'])}),
        'non_influence_proof.json': json_bytes(proof), 'prior_diagnostic_comparison.json': json_bytes(differences),
        'reference_input_compatibility.json': json_bytes(reference_input_check(projection, schemas)),
        'modeling_eligibility_policy_v2.md': (ROOT / POLICY).read_bytes(),
        'role_aware_configuration.json': (ROOT / CONFIG).read_bytes(),
        'preregistered_configuration.json': (ROOT / 'configs/phase5_prospective_shadow_v1.json').read_bytes(),
        'prospective_protocol_v1.md': (ROOT / 'docs/phase5-prospective-shadow-protocol-v1.md').read_bytes(),
        'feature_contract.json': json_bytes({'contract_version': VERSION, 'algorithm': ALGORITHM_VERSION,
            'feature_version': FEATURE_VERSION, 'ordered_features': FEATURE_ORDER, 'scheduled_rounds': 'unknown_for_every_target_and_history',
            'deferred_columns': DEBUT_COLS, 'preprocessing_fit': False}),
        'missingness.json': json_bytes({c: int(frame[c].isna().sum()) for c in FEATURE_ORDER}),
        'package_versions.json': json_bytes(versions()), 'code_versions.json': json_bytes(code_hashes(CODE)),
        'raw_source_manifest.json': (PARENT / 'source_manifest.json').read_bytes(),
        'source_capture_receipt.json': (PARENT / 'source_capture_receipt.json').read_bytes()}
    for name in checks:
        if name.startswith('registry/'):
            out[name] = (PARENT / name).read_bytes()
    for path, h in ledger['supporting_evidence_hashes'].items():
        out['identity_evidence/' + path] = (ROOT / path).read_bytes()
    from modeling.phase5b3_non_influence import phase5b2_evidence
    out.update(phase5b2_evidence(evidence_directory))
    manifest = {'contract_version': VERSION, 'artifact_role': 'validated_role_aware_challenger_preparation',
        'identity_reconciliation': 'UNRESOLVED', 'modeling_role_eligibility': 'READY', 'fitting_input_readiness': 'READY',
        'prospective_forecast_readiness': 'BLOCKED', 'historical_comparison': 'STILL_BLOCKED',
        'event_cutoff_exclusive': CUTOFF, 'capture_completed_at': json.loads((PARENT / 'training_manifest.json').read_bytes())['capture_completed_at'],
        'certification': 'retrospective_reconstruction_only_not_complete_history',
        'parent_checksums_sha256': PARENT_ROOT, 'reconciliation_checksums_sha256': RECONCILIATION_ROOT,
        'reference_checksums_sha256': REFERENCE_ROOT, 'summary': summary,
        'feature_order': FEATURE_ORDER, 'feature_algorithm_version': ALGORITHM_VERSION,
        'policy_sha256': sha256(out['modeling_eligibility_policy_v2.md']),
        'config_sha256': sha256(out['role_aware_configuration.json']),
        'source_sha256': {n: sha256(b) for n, b in raw.items()},
        'payload_sha256': {n: sha256(b) for n, b in sorted(out.items())},
        'no_fitting_prediction_outcome_evaluation': True}
    out['training_manifest.json'] = json_bytes(manifest)
    return out


def load_role_aware_preparation(directory, *, expected_checksums_sha256, expected_training_manifest_sha256):
    checks = verify_checksums(directory, expected_checksums_sha256=expected_checksums_sha256)
    raw = (directory / 'training_manifest.json').read_bytes()
    if sha256(raw) != expected_training_manifest_sha256:
        raise PreflightError('Role-aware training manifest pin mismatch')
    manifest = json.loads(raw)
    if manifest.get('contract_version') != VERSION or manifest.get('fitting_input_readiness') != 'READY':
        raise PreflightError('Blocked or incompatible role-aware preparation')
    if json.loads((directory / 'package_versions.json').read_bytes()) != versions() or json.loads((directory / 'code_versions.json').read_bytes()) != code_hashes(CODE):
        raise PreflightError('Role-aware code/package versions changed')
    rebuilt = build_payloads(evidence_directory=directory)
    if any(checks.get(n) != sha256(b) or (directory / n).read_bytes() != b for n, b in rebuilt.items()):
        raise PreflightError('Role-aware independent rebuild mismatch')
    frame = pd.read_csv(directory / 'training.csv', float_precision='round_trip')
    frame['event_date'] = pd.to_datetime(frame.event_date)
    validate_training_frame(frame, event_cutoff=CUTOFF)
    return frame, manifest, json.loads((directory / 'folds.json').read_bytes())


def publish(directory, payloads):
    directory = directory.resolve()
    if not directory.is_relative_to(OUTPUT) or directory == OUTPUT:
        raise PreflightError('Publication must remain exclusively in Phase5B3 root')
    if directory.exists():
        raise PreflightError('Never overwrite a Phase5B3 run')
    if any(Path(n).is_absolute() or '..' in Path(n).parts for n in payloads):
        raise PreflightError('Artifact path escape')
    sums = json_bytes({'files': {n: sha256(b) for n, b in sorted(payloads.items())}})
    directory.mkdir(parents=True, exist_ok=False)
    marker = directory / 'INCOMPLETE'
    marker.write_bytes(b'Phase5B3 incomplete\n')
    for n, b in {**payloads, 'checksums.json': sums}.items():
        p = directory / n
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open('xb') as stream:
            stream.write(b)
        p.chmod(0o444)
    verify_checksums(directory, expected_checksums_sha256=sha256(sums), allow_incomplete=True)
    marker.unlink()
    return sha256(sums)

"""Explicit offline tests; no real estimator, prediction, fit or outcome read."""
from copy import deepcopy
from datetime import date
import json
from pathlib import Path

import pandas as pd
import pytest

from features.history import build_fighter_index, get_history
from modeling import phase5_role_aware as role
from modeling.phase5_current_data import load_current_preparation, source_data, table_bytes
from modeling.phase5_history_identity import history_source, load_reconciliation
from modeling.phase5b3_non_influence import synthetic_rows, uid, prove_non_influence, phase5b2_evidence
from modeling.phase5b3_safety import preparation_guards
from modeling.phase5_reference_adapter import build_reference_features, validate_history_identities
from modeling.refit_preflight import PreflightError, json_bytes, sha256, FEATURE_ORDER, DEBUT_COLS


@pytest.fixture(scope='module')
def real_inputs():
    return role.trusted_inputs()


@pytest.fixture(scope='module')
def real_preparation():
    return role.build_payloads()


def projection(rows=None, schemas=None, decisions=None, exclusions=None):
    if rows is None:
        rows, schemas, exclusions = synthetic_rows()
    return role.project_roles(rows, schemas, decisions or [], exclusions or set())


def test_full_actual_consumer_non_influence_proof():
    proof = prove_non_influence()
    assert proof['status'] == 'PROVEN'
    assert {m['mutation'] for m in proof['mutations']} == {
        'add', 'remove', 'reorder', 'alter_metadata', 'alter_participants_and_event', 'rename_ids'}
    assert len(proof['consumer_actual_value_sha256']) == 17
    assert all(m['all_actual_values_and_selected_identities_equal'] for m in proof['mutations'])
    assert len(proof['conflict_gates']) == 3


@pytest.mark.parametrize('bad', ['duplicate_id', 'bad_uuid', 'wrong_type', 'null', 'bad_date',
                                      'invalid_result', 'nonbinary_winner', 'missing_event', 'missing_profile',
                                      'self_matchup', 'stat_wrong_participant', 'stat_duplicate_pair',
                                      'negative_stat', 'landed_gt_attempted', 'infinite_physical', 'unknown_column'])
def test_raw_validation_fails_before_computational_indexing(bad, monkeypatch):
    rows, schemas, exclusions = synthetic_rows()
    f = rows['fights'][0]
    if bad == 'duplicate_id': rows['fights'].append(deepcopy(f))
    elif bad == 'bad_uuid': f['fight_id'] = 'convenient-identity'
    elif bad == 'wrong_type': f['is_title_fight'] = 0
    elif bad == 'null': f['is_interim_title'] = None
    elif bad == 'bad_date': rows['events'][0]['event_date'] = 'bad'
    elif bad == 'invalid_result': f['result_type'] = 'cancelled'
    elif bad == 'nonbinary_winner': f['result_type'] = 'draw'
    elif bad == 'missing_event': f['event_id'] = uid('absent-event')
    elif bad == 'missing_profile': f['fighter_1_id'] = uid('absent-profile')
    elif bad == 'self_matchup': f['fighter_2_id'] = f['fighter_1_id']
    elif bad == 'stat_wrong_participant': rows['fight_stats_aggregate'][0]['fighter_id'] = uid('e')
    elif bad == 'stat_duplicate_pair': rows['fight_stats_aggregate'].append(dict(rows['fight_stats_aggregate'][0], fight_stat_id=uid('newstat')))
    elif bad == 'negative_stat': rows['fight_stats_aggregate'][0]['knockdowns'] = -1
    elif bad == 'landed_gt_attempted': rows['fight_stats_aggregate'][0]['sig_strikes_landed'] = 200
    elif bad == 'infinite_physical': rows['fighters'][0]['height_cm'] = 'Infinity'
    else: f['invented_column'] = 1
    monkeypatch.setattr(role, 'index_source', lambda *a: pytest.fail('Indexes reached invalid source'))
    with pytest.raises(PreflightError):
        projection(rows, schemas, exclusions=exclusions)


def alias_fixture():
    rows, schemas, _ = synthetic_rows()
    can = next(f for f in rows['fights'] if f['fight_id'] == uid('excluded'))
    alias = dict(can, fight_id=uid('alias-excluded'), source_url='synthetic://alias-excluded')
    rows['fights'].append(alias)
    original_stats = [s for s in rows['fight_stats_aggregate'] if s['fight_id'] == can['fight_id']]
    rows['fight_stats_aggregate'] += [dict(s, fight_id=alias['fight_id'], fight_stat_id=uid('alias-' + s['fighter_id'])) for s in original_stats]
    d = {'disposition': 'proven_duplicate', 'proof_kind': 'manual_announcement_to_unique_source_occurrence',
         'original_ids': [can['fight_id'], alias['fight_id']], 'canonical_id': can['fight_id'],
         'event_id': can['event_id'], 'participant_ids': [can['fighter_1_id'], can['fighter_2_id']],
         'card_occurrences': [{'occurrence': role.link(can['source_url'])}], 'evidence': ['synthetic-manual', 'synthetic-card'],
         'source_rows': [{'row': f, 'row_sha256': sha256(table_bytes([f])),
             'statistics': [{'row': s, 'row_sha256': sha256(table_bytes([s]))} for s in rows['fight_stats_aggregate'] if s['fight_id'] == f['fight_id']]}
             for f in (can, alias)]}
    return rows, schemas, d


@pytest.mark.parametrize('side', ['canonical', 'alias'])
def test_exclusion_cannot_reenter_via_alias_in_any_fitting_role(side):
    rows, schemas, d = alias_fixture()
    excluded = {d['canonical_id'] if side == 'canonical' else d['original_ids'][1]}
    p = projection(rows, schemas, [d], excluded)
    assert p['expanded_exclusions'] == frozenset(d['original_ids'])
    assert sum(f['fight_id'] == d['canonical_id'] for f in p['history']) == 1
    frame, folds, _, _ = role.prepare_projection(p, schemas)
    for entry in role.preprocessing_inputs(frame, folds).values():
        assert set(entry['ordered_fight_ids']).isdisjoint(d['original_ids'])
    assert set(frame.fight_id).isdisjoint(d['original_ids'])


@pytest.mark.parametrize('conflict', ['bout', 'statistics', 'lineage', 'unsupported'])
def test_alias_conflicts_require_fresh_validation(conflict):
    rows, schemas, d = alias_fixture()
    if conflict == 'bout': rows['fights'][-1]['weight_class'] = 'heavyweight'
    elif conflict == 'statistics': rows['fight_stats_aggregate'][-1]['knockdowns'] = 1
    elif conflict == 'lineage': d['source_rows'] = []
    else: d['proof_kind'] = 'same_participant_guess'
    with pytest.raises(PreflightError):
        projection(rows, schemas, [d], set())


def test_resolved_conflict_cannot_hide_behind_fitting_exclusion(monkeypatch):
    rows, schemas, exclusions = synthetic_rows()
    upcoming = next(f for f in rows['fights'] if f['fight_id'] == uid('upcoming-april'))
    upcoming.update(result_type='nc')
    monkeypatch.setattr(role, 'index_source', lambda *a: pytest.fail('Ambiguous history indexed'))
    with pytest.raises(PreflightError, match='Ambiguous repeated'):
        projection(rows, schemas, exclusions=exclusions | {uid('resolved5'), upcoming['fight_id']})


def test_real_draw_admitted_exactly_once_original_id_and_no_april_targets(real_inputs):
    raw, rows, ledger, _, exclusions = real_inputs
    before = deepcopy(rows)
    schemas = json.loads(raw['sources/schemas.json'])
    p = projection(rows, schemas, ledger['groups'], exclusions)
    assert rows == before
    data = role.computational_history(p, schemas)
    index = build_fighter_index(data)
    draw = data.fight_by_id[role.APRIL_DRAW]
    assert role.APRIL_UPCOMING not in data.fight_by_id
    assert role.APRIL_DRAW not in p['aliases'] and role.APRIL_UPCOMING not in p['aliases']
    for fid in (draw['fighter_1_id'], draw['fighter_2_id']):
        later = get_history(index, fid, date(2026, 10, 2))
        assert sum(h.fight_id == role.APRIL_DRAW for h in later) == 1
        assert not any(h.fight_id == role.APRIL_DRAW for h in get_history(index, fid, draw['event_date']))
    assert {role.APRIL_DRAW, role.APRIL_UPCOMING}.isdisjoint(f['fight_id'] for f in p['fitting'])


def test_real_april_promotion_and_changed_duplicate_id_block(real_inputs):
    raw, rows, ledger, _, exclusions = real_inputs
    schemas = json.loads(raw['sources/schemas.json'])
    for renamed in (False, True):
        changed = deepcopy(rows)
        f = next(f for f in changed['fights'] if f['fight_id'] == role.APRIL_UPCOMING)
        f.update(result_type='win', winner_fighter_id=f['fighter_1_id'])
        if renamed: f['fight_id'] = uid('new-april-resolved-id')
        with pytest.raises(PreflightError, match='Ambiguous repeated'):
            projection(changed, schemas, ledger['groups'], exclusions)


def test_real_accepted_rematch_retained_and_all_aliases_revalidated(real_inputs):
    raw, rows, ledger, _, exclusions = real_inputs
    schemas = json.loads(raw['sources/schemas.json'])
    p = projection(rows, schemas, ledger['groups'], exclusions)
    assert len(p['aliases']) == 12
    assert role.REMATCH <= {f['fight_id'] for f in p['history']}
    assert len(p['expanded_exclusions']) == 166
    changed = deepcopy(rows)
    f = next(f for f in changed['fights'] if f['fight_id'] in role.REMATCH)
    f['source_url'] = 'synthetic://changed-rematch-link'
    with pytest.raises(PreflightError, match='fresh validation'):
        projection(changed, schemas, ledger['groups'], exclusions)


def test_exact_order_unknown_schedules_deferred_debut_and_fold_selection(real_preparation):
    frame = pd.read_csv(__import__('io').BytesIO(real_preparation['training.csv']), float_precision='round_trip')
    assert list(frame.columns) == role.META + FEATURE_ORDER
    assert frame[DEBUT_COLS + ['scheduled_rounds']].isna().all().all()
    folds = json.loads(real_preparation['folds.json'])
    assert folds['boundaries'] == ['2025-10-02', '2026-01-02', '2026-04-02', '2026-07-02', '2026-10-02']
    assert folds['ready'] and not folds['diagnostic_only']
    for f in folds['folds']:
        assert f['preprocessing_fit_partition'] == 'train_only' and f['boosting_rounds'] == 310
        assert not f['early_stopping'] and not f['tuning']


def test_all_raw_rows_statistics_and_registry_preserved(real_preparation, real_inputs):
    raw, rows, _, checks, _ = real_inputs
    for n, b in raw.items(): assert real_preparation[n] == b
    assert len(json.loads(real_preparation['role_admissions.json'])) == len(rows['fights'])
    assert len(json.loads(real_preparation['statistic_admissions.json'])) == len(rows['fight_stats_aggregate'])
    for n in checks:
        if n.startswith('registry/'):
            assert real_preparation[n] == (role.PARENT / n).read_bytes()
    future = json.loads(real_preparation['future_targets.json'])
    assert len(future) == 35
    for b in future: role.no_predictions_or_outcomes(b)
    assert all(r['reasons'] for r in json.loads(real_preparation['future_eligibility.json']))
    assert json.loads(real_preparation['training_manifest.json'])['identity_reconciliation'] == 'UNRESOLVED'


def test_every_original_exclusion_has_each_partition_role_exclusion_reason(real_preparation, real_inputs):
    exclusions = real_inputs[-1]
    records = json.loads(real_preparation['partition_role_admissions.json'])
    assert len(records) == len(real_inputs[1]['fights'])
    for r in records:
        if r['fight_id'] in exclusions:
            assert all(not v['admitted'] and 'alias_expanded_166_fitting_exclusion' in v['reasons'] for v in r['roles'].values())


def test_a_disposition_label_cannot_invent_a_distinct_rematch_contract():
    rows, schemas, _ = synthetic_rows()
    fs = [rows['fights'][0], dict(rows['fights'][0], fight_id=uid('another-resolved-id'))]
    assert not role.accepted_rematch({'proof_kind': 'explicit_two_source_occurrences',
        'original_ids': list(role.REMATCH), 'event_id': fs[0]['event_id'],
        'event_date': '2025-09-01', 'participant_ids': [uid('a'), uid('b')]}, fs)
    with pytest.raises(PreflightError, match='Ambiguous repeated'):
        role.validate_occurrences(fs, [], 'history')


def test_original_blocked_contracts_stay_blocked(real_inputs):
    _, rows, ledger, checks, exclusions = real_inputs
    with pytest.raises(PreflightError, match='Blocked or incompatible current'):
        load_current_preparation(role.PARENT, expected_checksums_sha256=role.PARENT_ROOT,
                                 expected_training_manifest_sha256=checks['training_manifest.json'])
    with pytest.raises(PreflightError, match='remains blocked'):
        load_reconciliation(role.RECONCILIATION, expected_checksums_sha256=role.RECONCILIATION_ROOT)
    with pytest.raises(PreflightError, match='block all history'):
        history_source(rows, ledger, exclusions)


def test_reference_structural_contract_and_features_synthetic_only():
    rows, schemas, exclusions = synthetic_rows()
    p = projection(rows, schemas, exclusions=exclusions)
    assert role.reference_input_check(p, schemas)['structural_compatibility'] == 'READY'
    payload = {'sources/' + n + '.json': table_bytes(p['normalized'][n]) for n in ('events', 'fighters')}
    payload.update({'sources/fights.json': table_bytes(p['history'] + p['future_source_rows']),
                    'sources/fight_stats_aggregate.json': table_bytes(p['historical_statistics']),
                    'sources/schemas.json': table_bytes(schemas)})
    data = source_data(payload)
    saved = {'base_prior': 0.5, 'height_stats': {}, 'reach_stats': {}, 'global_height_std': 1, 'global_reach_std': 1}
    features = build_reference_features(data, [uid('future-target')], observation_cutoff='2026-10-02T13:00:00Z', saved_preprocessing=saved)
    assert list(features.fight_id) == [uid('future-target')]
    assert set(FEATURE_ORDER) <= set(features)
    assert features.reference_snapshot_date.iloc[0] == '2026-10-02'
    # Unprojected ambiguous source still fails the unchanged adapter.
    raw_data = source_data({'sources/' + n + '.json': table_bytes(v) for n, v in rows.items()} |
                           {'sources/schemas.json': table_bytes(schemas)})
    with pytest.raises(PreflightError, match='normalized, resolved'):
        build_reference_features(raw_data, [uid('future-target')], observation_cutoff='2026-10-02T13:00:00Z', saved_preprocessing=saved)


def test_durable_phase5b2_access_response_is_not_identity_evidence(tmp_path):
    payload = phase5b2_evidence()
    for n, b in payload.items():
        p = tmp_path / n; p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(b)
    assert phase5b2_evidence(tmp_path) == payload
    assert json.loads(payload['phase5b2_evidence_receipt.json'])['identity_evidence'] is False
    body = tmp_path / 'phase5b2_evidence/response-01.body'
    body.write_bytes(b'tampered')
    with pytest.raises(PreflightError, match='checksum mismatch'):
        phase5b2_evidence(tmp_path)


@pytest.mark.parametrize('bad', ['tamper', 'incomplete', 'wrong_manifest', 'repinned_ready', 'repinned_features', 'repinned_code'])
def test_guarded_loader_rejects_tampering_and_falsified_readiness(real_preparation, tmp_path, monkeypatch, bad):
    monkeypatch.setattr(role, 'OUTPUT', tmp_path)
    run = tmp_path / 'run'
    root = role.publish(run, real_preparation)
    manifest_pin = sha256(real_preparation['training_manifest.json'])
    if bad == 'tamper':
        p = run / 'training.csv'; p.chmod(0o644); p.write_bytes(b'tampered')
    elif bad == 'incomplete': (run / 'INCOMPLETE').write_bytes(b'incomplete')
    elif bad == 'wrong_manifest': manifest_pin = '0' * 64
    else:
        if bad == 'repinned_ready':
            name = 'training_manifest.json'; v = json.loads((run / name).read_bytes()); v['fitting_input_readiness'] = 'BLOCKED'
        elif bad == 'repinned_code':
            name = 'code_versions.json'; v = json.loads((run / name).read_bytes()); v[next(iter(v))] = '0' * 64
        else:
            name = 'training.csv'; v = None
        p = run / name; p.chmod(0o644)
        p.write_bytes(json_bytes(v) if v is not None else real_preparation[name].replace(b'1500', b'1499', 1) + b'\n')
        sums = {n: sha256((run / n).read_bytes()) for n in real_preparation}
        p = run / 'checksums.json'; p.chmod(0o644); p.write_bytes(json_bytes({'files': sums}))
        root = sha256(p.read_bytes()); manifest_pin = sums['training_manifest.json']
    with pytest.raises(PreflightError):
        role.load_role_aware_preparation(run, expected_checksums_sha256=root, expected_training_manifest_sha256=manifest_pin)


def test_loader_success_determinism_exclusivity_and_publication_boundary(real_preparation, tmp_path, monkeypatch):
    assert real_preparation == role.build_payloads()
    with pytest.raises(PreflightError, match='exclusively'):
        role.publish(tmp_path / 'outside', {})
    monkeypatch.setattr(role, 'OUTPUT', tmp_path)
    run = tmp_path / 'ready'
    root = role.publish(run, real_preparation)
    frame, manifest, folds = role.load_role_aware_preparation(run, expected_checksums_sha256=root,
        expected_training_manifest_sha256=sha256(real_preparation['training_manifest.json']))
    assert manifest['fitting_input_readiness'] == 'READY' and len(frame) == folds['final_training_rows']
    with pytest.raises(PreflightError, match='overwrite'):
        role.publish(run, real_preparation)


@pytest.mark.parametrize('operation', ['model_fit', 'calibrator_fit', 'prior_fit', 'prediction', 'load_model',
                                     'network', 'warehouse', 'frozen_outcomes', 'credentials'])
def test_execution_guards(operation):
    with preparation_guards(), pytest.raises(RuntimeError, match='Phase5B3 forbids'):
        if operation == 'model_fit':
            from xgboost import XGBClassifier
            XGBClassifier().fit([], [])
        elif operation == 'calibrator_fit':
            from sklearn.linear_model import LogisticRegression
            LogisticRegression().fit([], [])
        elif operation == 'prior_fit':
            from features.debut_prior import compute_debut_priors
            compute_debut_priors(pd.DataFrame())
        elif operation == 'prediction':
            from xgboost import XGBClassifier
            XGBClassifier().predict_proba([])
        elif operation == 'load_model':
            import joblib
            joblib.load('synthetic.joblib')
        elif operation == 'network':
            import requests
            requests.get('https://example.invalid')
        elif operation == 'warehouse':
            import psycopg2
            psycopg2.connect()
        elif operation == 'frozen_outcomes':
            (role.ROOT / 'data/holdouts/historical_2026_apr_aug/outcomes.csv').read_bytes()
        else:
            (role.ROOT / '.env').read_bytes()

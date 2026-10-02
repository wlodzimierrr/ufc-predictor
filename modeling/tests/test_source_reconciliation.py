"""Focused v2 reconciliation checks; synthetic replay remains covered by v1."""

from copy import deepcopy
import json
from pathlib import Path

import pytest

from modeling import source_reconciliation as recovery
from modeling.scoring_inputs import PreflightError, Snapshot, json_bytes, sha256, validated_loader_reference

CAP = '2026-05-15T14:25:46Z'


def row(clock='2026-05-13 20:26:25 UTC', value='old'):
    return {'fight_id': 'a', 'scraped_at': clock, 'value': value}


def test_identical_duplicates_collapse_with_exact_raw_records():
    r = row()
    selected, ledger, unresolved = recovery.reconcile_rows([r, deepcopy(r)], ('fight_id',), CAP)
    assert selected == [r] and not unresolved
    assert ledger[0]['reason'] == 'identical_records_collapsed'
    assert [e['line'] for e in ledger[0]['records']] == [2, 3]
    assert all(e['raw_record'] == r for e in ledger[0]['records'])


def test_observation_order_is_independent_of_row_order_and_content_hash():
    a, b = row(), row('2026-05-14 00:00:00 UTC', 'new')
    one, ledger, gaps = recovery.reconcile_rows([b, a], ('fight_id',), CAP)
    two, _, _ = recovery.reconcile_rows([a, b], ('fight_id',), CAP)
    assert one == two == [b] and not gaps
    assert ledger[0]['records'][1]['decision'] == 'rejected'
    assert ledger[0]['records'][1]['reason'] == 'superseded_observation'


@pytest.mark.parametrize('revisions,reason', [
    ([row(), row(value='conflict')], 'ambiguous_tied_observations'),
    ([row(), row(value='conflict'), row('2026-05-14 00:00:00 UTC')], 'ambiguous_tied_observations'),
    ([row('')], 'missing_or_invalid_observation'),
    ([row('2026-05-16 00:00:00 UTC')], 'observation_after_version_availability'),
])
def test_unresolved_revisions_are_retained(revisions, reason):
    selected, ledger, gaps = recovery.reconcile_rows(revisions, ('fight_id',), CAP)
    assert not selected and gaps[0]['reason'] == reason
    assert len(ledger[0]['records']) == len(revisions)
    assert all(r['decision'] == 'rejected' for r in ledger[0]['records'])


def entry(commit, path, available=CAP):
    return {'commit': commit, 'git_path': path, 'availability_utc': available,
            'blob': 'a' * 40, 'sha256': sha256(commit.encode()), 'unresolved': []}


def test_component_priority_eligibility_and_complete_lineage():
    root = entry('root', 'data/events.csv')
    scraper = entry('scraper', 'scraper/data/events.csv', '2026-05-14T00:00:00Z')
    later = entry('october', 'data/events.csv', '2026-10-02T00:00:00Z')
    r = {'event_id': 'e', 'date_formatted': '2026-05-01'}
    tables = {('root', root['git_path']): ([], [r], root),
              ('scraper', scraper['git_path']): ([], [{**r, 'date_formatted': '2026-04-01'}], scraper),
              ('october', later['git_path']): ([], [{**r, 'date_formatted': '2026-01-01'}], later)}
    chosen, receipt = recovery.select_component_reference(tables,
        ['data/events.csv', 'scraper/data/events.csv'], 'e', 'event_id', CAP)
    assert chosen == r and receipt['component']['commit'] == 'root'
    assert receipt['component']['sha256'] == root['sha256'] and receipt['raw_record'] == r
    assert recovery.select_component_reference(tables, ['data/events.csv'], 'missing', 'event_id', CAP)[0] is None


def test_tied_component_versions_do_not_use_commit_or_hash_order():
    a, b = entry('a', 'data/events.csv'), entry('b', 'data/events.csv')
    tables = {('a', a['git_path']): ([], [{'event_id': 'e', 'date': 'a'}], a),
              ('b', b['git_path']): ([], [{'event_id': 'e', 'date': 'b'}], b)}
    chosen, receipt = recovery.select_component_reference(tables, ['data/events.csv'], 'e', 'event_id', CAP)
    assert chosen is None and receipt['reason'] == 'ambiguous_tied_component_versions'


def test_primary_eligibility_and_ambiguous_tie():
    def snap(commit, available, digest):
        return Snapshot({'commit': commit, 'availability_utc': available,
                         'files': {'fights': {'sha256': digest}}}, None, {})
    a = snap('a', CAP, 'old')
    later = snap('october', '2026-10-02T00:00:00Z', 'new')
    assert recovery.select_primary([later, a], CAP) is a
    assert recovery.select_primary([later], CAP) is None
    with pytest.raises(PreflightError, match='Ambiguous'):
        recovery.select_primary([a, snap('b', CAP, 'conflict')], CAP)


def test_synthesized_generic_title_defaults_stay_unknown():
    t = {'event_id': 'e', 'fighter_1_id': '1', 'fighter_2_id': '2', 'weight_class': 'lightweight'}
    r = {**t, 'bout_type': 'Lightweight Bout'}
    assert recovery.classify_csv_title(r, t) == (None, 'generic_bout_text_without_row_producer_attestation')
    assert recovery.classify_csv_title({**r, 'is_title_fight': False}, t)[0] is None
    assert recovery.classify_csv_title({**r, 'bout_type': 'UFC Lightweight Title Bout'}, t)[0] is True
    assert recovery.classify_csv_title({**r, 'fighter_1_id': 'other'}, t)[0] is None


@pytest.fixture(scope='module')
def real_recovery():
    return recovery.prepare_recovery()


def test_exact_original_108_with_profile_experience_gaps(real_recovery):
    payload, validation = real_recovery
    assert payload['targets.csv'] == (recovery.V1_RUN / 'targets.csv').read_bytes()
    assert payload['comparison-protocol.json'] == (recovery.V1_RUN / 'comparison-protocol.json').read_bytes()
    assert validation['target_rows'] == 108
    assert validation['original_affected_forecasts'] == 98
    assert validation['resolved_original_forecasts'] == 2
    assert validation['remaining_affected_forecasts'] == 96
    assert validation['gap_counts'] == {'unknown_target_title_status': 93, 'missing_target_profile': 1,
                                      'unresolved_experience_for_supplemented_profile': 4}
    assert 'features.csv' not in payload and not validation['features_constructed']
    assignments = json.loads(payload['source-selection.json'])['assignments']
    assert len(assignments) == len({r['fight_id'] for r in assignments}) == 108
    assert all(p['zero_experience_certified'] is False for r in assignments for p in r['history_profiles'])
    with pytest.raises(PreflightError, match='108-row'):
        recovery.assess_recovery(recovery.load_frozen_targets()[:-1], [], [], {})


def test_real_new_leads_and_reconciled_sources_have_full_lineage(real_recovery):
    payload, _ = real_recovery
    manifest = json.loads(payload['source-manifest.json'])
    assert manifest['trees_inspected'] <= 256
    assert len(manifest['inspected_path_versions']) == manifest['trees_inspected'] * len(manifest['data_paths'])
    by = {r['commit'][:7]: r for r in manifest['reconciled_primary_versions']}
    assert by['e23dd7c']['coherent'] and by['4af6c2f']['coherent']
    april = by['8bb0552']
    assert not april['coherent'] and april['historical_fights_dropped'] == 0
    assert len([r for r in april['supplementary_references'] if r['destination_component'] == 'events']) == 15
    assert len([r for r in april['rejections'] if r.get('destination_component') == 'fighters']) == 8
    for r in json.loads(payload['source-selection.json'])['assignments']:
        for source in r['source_components'].values():
            assert recovery.utc_instant(source['availability_utc']) <= recovery.utc_instant(r['scored_at'])
        assert not r['source_commit'].startswith(('50f02ab', '6e5c0cd'))


def test_target_result_fields_do_not_decide_title():
    t = {'event_id': 'e', 'fighter_1_id': '1', 'fighter_2_id': '2', 'weight_class': 'lightweight'}
    r = {**t, 'bout_type': 'UFC Lightweight Title Bout'}
    before = recovery.classify_csv_title(r, t)
    assert recovery.classify_csv_title({**r, 'fighter_1_outcome': 'L', 'winner': '2',
        'finish_round': 1, 'finish_time_second': 1, 'confidence_tier': 'high'}, t) == before


def test_preserved_profile_reference_cannot_certify_zero_experience(real_recovery):
    payload, _ = real_recovery
    records = json.loads(payload['raw-profile-history-references.json'])
    wint = next(r for r in records if '41d605df' in r['body_path'])
    assert any(r['explicit_event_date'] == '2026-08-11' for r in wint['references'])
    assert all(not r['complete_experience_certified'] and not r['results_or_statistics_used'] for r in records)
    assert all(set(r) == {'fight_ids', 'event_ids', 'explicit_event_date'} for p in records for r in p['references'])


def test_deterministic_evidence_and_overwrite_refusal(real_recovery, tmp_path, monkeypatch):
    first, validation = real_recovery
    second, again = recovery.prepare_recovery()
    assert first == second and validation == again
    monkeypatch.setattr(recovery, 'OUTPUT_ROOT', tmp_path)
    run = tmp_path / 'new'
    recovery.publish_recovery(run, first)
    with pytest.raises(PreflightError, match='overwrite'):
        recovery.publish_recovery(run, first)
    with pytest.raises(PreflightError, match='features'):
        recovery.publish_recovery(tmp_path / 'bad', {**first, 'features.csv': b''})
    with pytest.raises(PreflightError, match='Blocked/incomplete'):
        validated_loader_reference(run)


def test_real_recovery_refuses_forbidden_operations(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('Forbidden reconstruction/scoring/outcome/warehouse/fitting operation')
    import features.forecast_replay
    import modeling.scoring_inputs
    import warehouse.db
    import xgboost
    import sklearn.linear_model
    monkeypatch.setattr(features.forecast_replay, 'reconstruct_forecast', forbidden)
    monkeypatch.setattr(modeling.scoring_inputs, 'reconstruct_forecast', forbidden)
    monkeypatch.setattr(modeling.scoring_inputs, 'construct_features', forbidden)
    monkeypatch.setattr(warehouse.db, 'get_connection', forbidden)
    monkeypatch.setattr(xgboost.XGBClassifier, 'fit', forbidden)
    monkeypatch.setattr(xgboost.XGBClassifier, 'predict_proba', forbidden)
    monkeypatch.setattr(sklearn.linear_model.LogisticRegression, 'fit', forbidden)
    import psycopg2
    from modeling.xgb_candidate_bundle import CandidateBundle
    import builtins
    monkeypatch.setattr(psycopg2, 'connect', forbidden)
    monkeypatch.setattr(CandidateBundle, 'predict', forbidden)
    monkeypatch.setattr('features.debut_prior.compute_debut_priors', forbidden)
    monkeypatch.setattr('features.debut_prior.apply_debut_features', forbidden)
    def check(path):
        path = Path(path)
        if 'holdouts' in path.parts and path.name not in ('manifest.json', 'predictions.csv', 'identity-exclusions.json'):
            forbidden()
        if 'identity-outcome' in path.name or path.name == 'baseline-frozen-holdout-joined.csv':
            forbidden()
    original = Path.open
    def guarded(path, *args, **kwargs):
        check(path)
        return original(path, *args, **kwargs)
    original_open = builtins.open
    def guarded_open(path, *args, **kwargs):
        if isinstance(path, (str, Path)):
            check(path)
        return original_open(path, *args, **kwargs)
    monkeypatch.setattr(builtins, 'open', guarded_open)
    monkeypatch.setattr(Path, 'open', guarded)
    _, validation = recovery.prepare_recovery()
    assert validation['status'] == 'STILL_BLOCKED'

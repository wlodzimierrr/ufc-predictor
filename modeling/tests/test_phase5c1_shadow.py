"""Scoped synthetic-only workflow tests; real loading is prediction/fit guarded."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest

from modeling.phase5c1_contract import (Blocked, INFERENCE_VERSION, PINS, SOURCE_VERSION, VERSION,
    digest, encoded, instant, inventory, now, sha, verify_components)
from modeling.phase5c1_safety import implementation_guards


@pytest.fixture(autouse=True)
def safe(request):
    with implementation_guards(allow_loading=request.node.name == 'test_real_components_load_without_prediction_or_fit'):
        yield


@pytest.fixture
def contract():
    return {'version': VERSION, 'source_version': SOURCE_VERSION, 'inference_version': INFERENCE_VERSION,
            'frozen_at': '2026-10-02T20:00:00+00:00', 'component_freezes': ['2026-10-02T18:33:00+00:00'],
            'components': deepcopy(PINS), 'synthetic_test_contract': True}


@pytest.fixture
def capture(tmp_path, contract):
    from modeling.phase5c1_synthetic import make_package
    from modeling.phase5c1_sources import validate_package
    p = tmp_path / 'capture'
    pin, r = make_package(p, contract)
    return validate_package(p, receipt_pin=pin, contract=contract)


def simulated_clock(package, delta=1):
    return lambda: (instant(package['receipt']['completed_at']) + timedelta(minutes=delta)).isoformat()


def rewrite(package, *, receipt=None, evidence=None, claims=None, table=None, values=None):
    """Intentional synthetic corruption/re-observation with honest fixture pins."""
    p = Path(package['directory'])
    r = receipt if receipt is not None else deepcopy(package['receipt'])
    if table is not None:
        body = encoded(values)
        (p / r['tables'][table]['body']).write_bytes(body)
        r['tables'][table].update(sha256=sha(body), bytes=len(body))
    if evidence is not None:
        if claims is not None:
            raw = encoded({'claims': claims})
            (p / 'evidence/claims.json').write_bytes(raw)
            evidence['sources']['export']['sha256'] = sha(raw)
        raw = encoded(evidence)
        (p / r['evidence_body']).write_bytes(raw)
        r['evidence_sha256'] = sha(raw)
    (p / 'capture_receipt.json').write_bytes(encoded(r))
    return {'directory': str(p), 'receipt_sha256': sha(encoded(r))}


def validate(fixture, contract):
    from modeling.phase5c1_sources import validate_package
    return validate_package(fixture['directory'], receipt_pin=fixture['receipt_sha256'], contract=contract)


def run(tmp_path, package, contract, **kw):
    from modeling.phase5c1_runner import forecast
    from modeling.phase5c1_synthetic import SyntheticPipelines
    pipes = kw.pop('pipelines', SyntheticPipelines())
    return forecast(tmp_path / 'journal', kw.pop('name', 'SYNTHETIC_run'), package, contract, pipes,
                    clock=kw.pop('clock', simulated_clock(package)), **kw), pipes


def test_complete_pair_control_precision_and_replay(tmp_path, capture, contract):
    from modeling.phase5c1_runner import verify_run
    from modeling.phase5c1_synthetic import SyntheticPipelines
    result, pipes = run(tmp_path, capture, contract)
    assert result['status'] == 'READY' and result['complete_primary_pair']
    assert pipes.calls == ['challenger', 'reference', 'march']
    d = tmp_path / 'journal/runs/SYNTHETIC_run'
    rows = json.loads((d / 'outputs/challenger.json').read_bytes())
    assert rows[0]['raw_probability_fighter_1'] == 0.12345678901234568
    assert rows[0]['decision']['high_confidence']
    assert verify_run(d, result['checksums_sha256'], pipelines=SyntheticPipelines())['replayed']
    assert not (d / 'INCOMPLETE').exists()
    assert not any('outcome' in n.name for n in d.rglob('*'))


def test_missing_control_does_not_change_primary_population(tmp_path, capture, contract):
    result, pipes = run(tmp_path, capture, contract, include_march=False)
    assert result['status'] == 'READY' and pipes.calls == ['challenger', 'reference']
    d = tmp_path / 'journal/runs/SYNTHETIC_run'
    assert json.loads((d / 'march_status.json').read_bytes())['status'] == 'missing_counterpart_prediction'
    assert len(json.loads((d / 'run.json').read_bytes())['primary_population']) == 1


@pytest.mark.parametrize('missing', ['title', 'experience_1', 'experience_2', 'profile', 'timing', 'weight_class'])
def test_essential_blockers_register_all_without_prediction(tmp_path, capture, contract, missing):
    p = deepcopy(capture)
    e = deepcopy(p['evidence'])
    t = p['receipt']['considered'][0]
    if missing == 'profile':
        fixture = rewrite(p, table='fighters', values=[f for f in p['rows']['fighters'] if f['fighter_id'] != t['fighter_1_id']])
    elif missing == 'weight_class':
        rows = deepcopy(p['rows']['fights'])
        next(f for f in rows if f['fight_id'] == t['fight_id'])['weight_class'] = None
        fixture = rewrite(p, table='fights', values=rows)
    elif missing == 'timing':
        result, pipes = run(tmp_path, p, contract, clock=lambda: (instant(t['event_date'] + 'T00:00:00Z') - timedelta(hours=23)).isoformat())
        assert result['status'] == 'BLOCKED' and pipes.calls == []
        return
    else:
        if missing == 'title':
            e['assertions'] = [a for a in e['assertions'] if a['kind'] != 'title']
        else:
            pid = t['fighter_1_id'] if missing.endswith('1') else t['fighter_2_id']
            e['assertions'] = [a for a in e['assertions'] if a['claim'].get('fighter_id') != pid]
        fixture = rewrite(p, evidence=e)
    result, pipes = run(tmp_path, fixture, contract, clock=simulated_clock(p))
    assert result['status'] == 'BLOCKED' and not pipes.calls and result['prediction_calls'] == 0
    from modeling.phase5c1_journal import state, latest
    assert len(state(latest(tmp_path / 'journal')[2])) == len(t and p['receipt']['considered'])


@pytest.mark.parametrize('bad', ['source_hash', 'evidence_hash', 'missing_body', 'identity', 'date', 'title_string',
    'access_page', 'late_evidence', 'late_row', 'chronology', 'coverage', 'domain', 'debut', 'unsupported_extraction'])
def test_corrupt_evidence_and_sources_rejected(tmp_path, capture, contract, bad):
    p = Path(capture['directory'])
    e = deepcopy(capture['evidence'])
    fixture = {'directory': str(p), 'receipt_sha256': capture['receipt_sha256']}
    if bad == 'source_hash':
        (p / 'fights.json').write_bytes(b'[]')
    elif bad == 'evidence_hash':
        (p / 'evidence/claims.json').write_bytes(b'tampered')
    elif bad == 'missing_body':
        (p / 'evidence/claims.json').unlink()
    elif bad == 'chronology':
        r = deepcopy(capture['receipt']); r['started_at'] = r['completed_at']
        r['tables']['fighters']['observed_at'] = '2026-10-03T00:00:00Z'
        fixture = rewrite(capture, receipt=r)
    elif bad == 'late_row':
        rows = deepcopy(capture['rows']['fighters']); rows[0]['scraped_at'] = '2099-01-01T00:00:00Z'
        fixture = rewrite(capture, table='fighters', values=rows)
    else:
        a = e['assertions'][0 if bad in {'identity', 'date', 'title_string', 'unsupported_extraction'} else 1]
        if bad == 'identity': a['claim']['event_id'] = capture['rows']['events'][0]['event_id']
        elif bad == 'date': a['claim']['event_date'] = '2026-11-01'
        elif bad == 'title_string': a['claim']['is_title_fight'] = 'Bout'
        elif bad == 'access_page':
            e['sources']['export']['access_blocked'] = True
        elif bad == 'late_evidence':
            e['sources']['export']['observed_at'] = '2099-01-01T00:00:00Z'
        elif bad == 'coverage': a['claim']['prior_occurrences'] = []
        elif bad == 'domain': a['claim']['domain'] = 'unspecified'
        elif bad == 'debut': a['claim']['status'] = 'verified_debut'
        else: a['extraction']['method'] = 'generated_bout_text'
        # Update structured source for the altered claim: hashes alone don't
        # make false identity/history assertions admissible.
        claims = [a['claim'] for a in e['assertions']]
        for i, a in enumerate(e['assertions']):
            a['extraction'].update(pointer='/claims/' + str(i), claim_sha256=digest(a['claim']))
        fixture = rewrite(capture, evidence=e, claims=claims)
    from modeling.phase5c1_sources import project
    from modeling.phase5c1_runner import check
    with pytest.raises((Blocked, ValueError)):
        package = validate(fixture, contract)
        check(package, project(package), forecast_at=simulated_clock(package)())


def test_new_receipt_can_bind_byte_identical_content_and_stale_receipt_cannot(tmp_path, capture, contract):
    from modeling.phase5c1_sources import project
    from modeling.phase5c1_adapters import inference_binding, validate_binding, build_features
    from modeling.phase5c1_synthetic import SyntheticPipelines
    r = deepcopy(capture['receipt'])
    from uuid import uuid4
    r['capture_id'] = str(uuid4())
    r['started_at'] = (instant(r['started_at']) + timedelta(hours=1)).isoformat()
    r['completed_at'] = r['observation_cutoff'] = (instant(r['completed_at']) + timedelta(hours=1)).isoformat()
    for table in r['tables'].values():
        table['requested_at'] = r['started_at']; table['observed_at'] = r['completed_at']
    fresh = validate(rewrite(capture, receipt=r), contract)
    assert fresh['source_sha256'] == capture['source_sha256'] and fresh['receipt_sha256'] != capture['receipt_sha256']
    projection = project(fresh)
    from modeling.phase5c1_runner import check
    check(fresh, projection, forecast_at=simulated_clock(fresh)())
    frames, _ = build_features(fresh, projection, [projection['targets'][0]['fight_id']], reference_preprocessing=SyntheticPipelines.reference_preprocessing)
    frame = frames['challenger']
    b = inference_binding(fresh, projection, frame, contract, 'challenger')
    validate_binding(b, fresh, projection, frame, contract, 'challenger')
    newer_contract = deepcopy(contract); newer_contract['frozen_at'] = fresh['receipt']['completed_at']
    with pytest.raises(Blocked, match='freeze'):
        validate(fresh, newer_contract)


@pytest.mark.parametrize('field', ['matrix_sha256', 'projection_sha256', 'source_sha256', 'capture_id', 'components', 'observed_at'])
def test_inference_binding_tampering(capture, contract, field):
    from modeling.phase5c1_sources import project
    from modeling.phase5c1_runner import check
    from modeling.phase5c1_adapters import build_features, inference_binding, validate_binding
    from modeling.phase5c1_synthetic import SyntheticPipelines
    projection = project(capture); check(capture, projection, forecast_at=simulated_clock(capture)())
    frame = build_features(capture, projection, [projection['targets'][0]['fight_id']], reference_preprocessing=SyntheticPipelines.reference_preprocessing)[0]['challenger']
    b = inference_binding(capture, projection, frame, contract, 'challenger'); b[field] = 'wrong'
    with pytest.raises(Blocked, match='provenance'):
        validate_binding(b, capture, projection, frame, contract, 'challenger')


def test_same_day_future_and_target_results_never_enter_histories(capture, contract):
    from modeling.phase5c1_sources import project
    rows = deepcopy(capture['rows'])
    day = instant(capture['receipt']['observation_cutoff']).date().isoformat()
    rows['events'][3]['event_date'] = day
    same_day = validate(rewrite(capture, table='events', values=rows['events']), contract)
    projected = project(same_day)
    assert not any(f['event_date'] >= day for f in projected['history'])
    assert all(s['fight_id'] in {f['fight_id'] for f in projected['history']} for s in projected['statistics'])
    rows['events'][3]['event_date'] = '2099-01-01'
    with pytest.raises(ValueError):
        validate(rewrite(same_day, table='events', values=rows['events']), contract)
    rows = deepcopy(capture['rows']['fights'])
    f = next(f for f in rows if f['fight_id'] == capture['receipt']['considered'][0]['fight_id'])
    f.update(result_type='win', winner_fighter_id=f['fighter_1_id'])
    with pytest.raises(ValueError):
        validate(rewrite(capture, table='fights', values=rows), contract)


def test_unresolved_nontarget_announcements_do_not_influence_features(capture, contract):
    from modeling.phase5c1_sources import project
    from modeling.phase5c1_adapters import build_features, frame_bytes
    from modeling.phase5c1_runner import check
    from modeling.phase5c1_synthetic import SyntheticPipelines
    def matrices(package):
        p = project(package); check(package, p, forecast_at=simulated_clock(package)())
        return {n: frame_bytes(f) for n, f in build_features(package, p, [p['targets'][0]['fight_id']], reference_preprocessing=SyntheticPipelines.reference_preprocessing)[0].items()}
    before = matrices(capture)
    rows = deepcopy(capture['rows']['fights'])
    for f in rows:
        if f['result_type'] == 'upcoming' and f['fight_id'] != capture['receipt']['considered'][0]['fight_id']:
            f.update(scheduled_rounds=5, is_title_fight=True, weight_class='heavyweight')
    changed = validate(rewrite(capture, table='fights', values=rows), contract)
    assert before == matrices(changed)


def test_recipe_separation_feature_clocks_and_optional_profile_values(capture, contract):
    from modeling.phase5c1_sources import project
    from modeling.phase5c1_adapters import build_features
    from modeling.phase5c1_runner import check
    from modeling.phase5c1_synthetic import SyntheticPipelines
    rows = deepcopy(capture['rows']['fighters']); rows[0].update(height_cm=None, reach_cm=None)
    p = validate(rewrite(capture, table='fighters', values=rows), contract)
    projection = project(p); assert check(p, projection, forecast_at=simulated_clock(p)())['status'] == 'READY'
    frames, lineage = build_features(p, projection, [projection['targets'][0]['fight_id']], reference_preprocessing=SyntheticPipelines.reference_preprocessing)
    assert frames['challenger'].scheduled_rounds.isna().all() and frames['march'].scheduled_rounds.isna().all()
    assert frames['reference'].scheduled_rounds.iloc[0] == 3
    assert lineage[0]['feature_reference_date'] == projection['targets'][0]['event_date']
    assert lineage[0]['history_date_cutoff_exclusive'] == instant(p['receipt']['observation_cutoff']).date().isoformat()
    assert frames['reference'].reference_snapshot_date.iloc[0] != lineage[0]['feature_reference_date']


@pytest.mark.parametrize('fail', ['challenger', 'reference', 'march'])
def test_partial_failures_frozen_and_no_later_favorable_selection(tmp_path, capture, contract, fail):
    from modeling.phase5c1_synthetic import SyntheticPipelines
    result, _ = run(tmp_path, capture, contract, pipelines=SyntheticPipelines(fail=fail))
    if fail == 'march':
        assert result['status'] == 'READY'
    else:
        assert result['status'] == 'BLOCKED' and not result['complete_primary_pair']
        d = tmp_path / 'journal/runs/SYNTHETIC_run'
        assert not json.loads((d / 'run.json').read_bytes())['primary_population']
        with pytest.raises(Blocked, match='first_candidate'):
            run(tmp_path, capture, contract, name='later')


def test_first_pair_overwrite_and_registry_bytes_preserved(tmp_path, capture, contract):
    from modeling.phase5c1_contract import LEGACY
    from modeling.phase5c1_journal import latest, state
    result, _ = run(tmp_path, capture, contract)
    assert result['status'] == 'READY'
    with pytest.raises(Blocked, match='overwrite'):
        run(tmp_path, capture, contract)
    with pytest.raises(Blocked, match='already_selected'):
        run(tmp_path, capture, contract, name='later')
    parent, _, entries = latest(tmp_path / 'journal')
    for p in (LEGACY / 'registry/records').glob('*.json'):
        assert (parent / 'legacy/registry/records' / p.name).read_bytes() == p.read_bytes()
    assert len(state(entries)) == 1 and state(entries)[capture['receipt']['considered'][0]['source_row_id']]['selected'] == 'SYNTHETIC_run'


def test_revision_cancellation_and_replacement_are_explicit(tmp_path, capture, contract):
    from modeling.phase5c1_journal import exclusive, register, latest, state, cancel
    result, _ = run(tmp_path, capture, contract)
    journal = tmp_path / 'journal'; old = deepcopy(capture['receipt']['considered'][0]); changed = dict(old, event_date='2026-11-01')
    with exclusive(journal):
        with pytest.raises(Blocked, match='revision_required'):
            register(journal, [changed], receipt_pin=capture['receipt_sha256'], recorded_at=now())
        register(journal, [changed], receipt_pin=capture['receipt_sha256'], recorded_at=now(), action='revise')
        assert not state(latest(journal)[2])[old['source_row_id']]['primary_valid']
        replacement = dict(changed, source_row_id='replacement')
        register(journal, [replacement], receipt_pin=capture['receipt_sha256'], recorded_at=now(), action='replace', replaces=old['source_row_id'])
        cancel(journal, 'replacement', recorded_at=now(), reason='Synthetic cancellation')
    s = state(latest(journal)[2])
    assert s[old['source_row_id']]['disposition'] == 'replaced' and s['replacement']['disposition'] == 'cancelled'
    assert s[old['source_row_id']]['selected'] == 'SYNTHETIC_run'


def test_concurrent_publication_before_prediction(tmp_path, capture, contract):
    from modeling.phase5c1_journal import exclusive
    from modeling.phase5c1_synthetic import SyntheticPipelines
    pipes = SyntheticPipelines()
    with exclusive(tmp_path / 'journal'), pytest.raises(Blocked, match='concurrent'):
        run(tmp_path, capture, contract, pipelines=pipes)
    assert pipes.calls == []


@pytest.mark.parametrize('p,status,high,latent', [(0.3,'scored_actionable',True,False),(0.4,'scored_no_pick',False,False),
    (0.5,'scored_no_pick',False,True),(0.6,'scored_no_pick',False,True),(0.7,'scored_actionable',True,True),
    (0.6000000000000001,'scored_actionable',False,True)])
def test_exact_decision_boundaries(p, status, high, latent):
    from modeling.phase5c1_runner import decision
    d = decision(p)
    assert (d['status'], d['high_confidence'], d['latent_fighter_1']) == (status, high, latent)


def test_forecast_completion_window_blocks_pair(tmp_path, capture, contract):
    from modeling.phase5c1_synthetic import SyntheticPipelines
    cutoff = instant(capture['receipt']['completed_at'])
    deadline = instant(capture['receipt']['considered'][0]['event_date'] + 'T00:00:00Z') - timedelta(hours=24)
    calls = iter([cutoff + timedelta(minutes=1), *[deadline + timedelta(seconds=i) for i in range(1, 10)]])
    result, _ = run(tmp_path, capture, contract, clock=lambda: next(calls).isoformat())
    assert result['status'] == 'BLOCKED' and result['failure'] == 'forecast_completion_outside_window'


def test_mock_capture_verifies_transaction_before_source_queries_and_closes():
    from modeling.phase5c1_sources import mocked_readonly_capture
    conn = Mock(phase5c1_mock=True)
    cur = Mock()
    conn.cursor.return_value.__enter__ = Mock(return_value=cur)
    conn.cursor.return_value.__exit__ = Mock(return_value=False)
    cur.description = [('transaction_read_only',)]
    cur.fetchall.return_value = [('off',)]
    with pytest.raises(Blocked, match='read_only'):
        mocked_readonly_capture(lambda: conn, synthetic_authorization='phase5c1_mock_capture_only', clock=now)
    assert cur.execute.call_args_list[0].args[0] == 'SHOW transaction_read_only'
    assert cur.execute.call_count == 1
    conn.rollback.assert_called_once(); conn.close.assert_called_once()
    with pytest.raises(Blocked, match='authorization'):
        mocked_readonly_capture(lambda: pytest.fail('Unauthorized factory called'), synthetic_authorization=None, clock=now)


def test_real_components_load_without_prediction_or_fit():
    from modeling.phase5c1_adapters import SavedPipelines
    p = SavedPipelines()
    assert p.challenger.manifest['artifact_readiness'] == 'READY'
    assert p.reference.manifest['ready'] is True
    assert p.march.metadata['actual_training_endpoint'] == '2026-03-07'


@pytest.mark.parametrize('operation', ['fit', 'prediction', 'network', 'warehouse', 'outcomes', 'production', 'credentials'])
def test_active_scope_guards(operation):
    with pytest.raises(RuntimeError, match='Phase5C1 forbids'):
        if operation == 'fit':
            from xgboost import XGBClassifier
            XGBClassifier().fit([], [])
        elif operation == 'prediction':
            from xgboost import XGBClassifier
            XGBClassifier().predict_proba([])
        elif operation == 'network':
            import requests
            requests.get('https://example.invalid')
        elif operation == 'warehouse':
            import psycopg2
            psycopg2.connect()
        elif operation == 'outcomes':
            from modeling.phase5c1_contract import ROOT
            (ROOT / 'data/holdouts/historical_2026_apr_aug/outcomes.csv').read_bytes()
        elif operation == 'credentials':
            from modeling.phase5c1_contract import ROOT
            (ROOT / '.env').read_bytes()
        else:
            from modeling.phase5c1_contract import ROOT
            (ROOT / 'models/UNAUTHORIZED').write_bytes(b'bad')


def test_unchanged_training_hash_old_api_refuses_and_new_saved_adapter_accepts(capture, contract, tmp_path):
    from modeling.phase5c1_sources import project
    from modeling.phase5c1_runner import check
    from modeling.phase5c1_adapters import SavedPipelines, build_features, inference_binding
    from modeling.phase5c1_synthetic import SyntheticPipelines
    from modeling.phase5b4_bundle_v1 import ChallengerBundle
    from modeling.phase5b4_contract_v1 import ALGORITHM, PREPROCESSING, ORIENTATION, ContractError, csv_bytes
    projection = project(capture); check(capture, projection, forecast_at=simulated_clock(capture)())
    frame = build_features(capture, projection, [projection['targets'][0]['fight_id']], reference_preprocessing=SyntheticPipelines.reference_preprocessing)[0]['challenger']
    (tmp_path / 'preparation').mkdir(); (tmp_path / 'preparation/code_versions.json').write_bytes(b'{}')
    old = SimpleNamespace(directory=tmp_path, manifest={'source_sha256': capture['source_sha256']})
    provenance = {'algorithm': ALGORITHM, 'feature_version': 2, 'orientation': ORIENTATION,
        'modeling_policy': 'phase5_role_aware_preparation_v2', 'preprocessing_contract_sha256': digest(PREPROCESSING),
        'feature_code_sha256': {}, 'source_sha256': capture['source_sha256'],
        'capture_completed_at': capture['receipt']['completed_at'],
        'input_matrix_with_identity_sha256': sha(csv_bytes(frame)), 'purpose': 'separately_authorized_inference'}
    # No old model or prediction is present: its content-change gate fires first.
    with pytest.raises(ContractError, match='masquerade'):
        ChallengerBundle.predict(old, frame, source_provenance=provenance)
    class LearnedStub:
        def __init__(self, positive): self.positive, self.calls = positive, []
        def predict_proba(self, matrix, **kwargs):
            self.calls.append((matrix.copy(), kwargs)); return np.tile([1-self.positive, self.positive], (len(matrix),1))
    learner, calibrator = LearnedStub(0.12345678901234568), LearnedStub(0.7)
    calibrator.classes_ = np.array([0,1])
    adapter = SavedPipelines.__new__(SavedPipelines)
    adapter.challenger = SimpleNamespace(priors={'final': {'priors': SyntheticPipelines.reference_preprocessing}},
                                         models={'final': learner}, calibrator=calibrator)
    binding = inference_binding(capture, projection, frame, contract, 'challenger')
    processed, raw, calibrated = adapter.predict('challenger', frame, binding=binding, package=capture, projection=projection, contract=contract)
    assert raw[0] == learner.positive and calibrated[0] == 0.7 and len(learner.calls) == 1
    assert np.isclose(calibrator.calls[0][0][0,0], np.log(raw[0]/(1-raw[0])))


def test_march_adapter_calls_unchanged_guarded_api_with_saved_priors(capture, contract):
    from modeling.phase5c1_sources import project
    from modeling.phase5c1_runner import check
    from modeling.phase5c1_adapters import SavedPipelines, build_features, inference_binding
    from modeling.phase5c1_synthetic import SyntheticPipelines
    from modeling.xgb_candidate_bundle import CandidateBundle
    from modeling.refit_preflight import PreflightError
    projection = project(capture); check(capture, projection, forecast_at=simulated_clock(capture)())
    frame = build_features(capture, projection, [projection['targets'][0]['fight_id']], reference_preprocessing=SyntheticPipelines.reference_preprocessing)[0]['march']
    saved = deepcopy(SyntheticPipelines.reference_preprocessing)
    march = CandidateBundle.__new__(CandidateBundle)
    march.metadata = {'input_feature_provenance': {'training_reference_contract': 'synthetic_fixed'}}
    march.priors, march.rounds = saved, 310
    march.learner = Mock()
    march.learner.predict_proba.return_value = np.array([[0.75,0.25]])
    march.calibrator = Mock(classes_=np.array([0,1]))
    march.calibrator.predict_proba.return_value = np.array([[0.6,0.4]])
    adapter = SavedPipelines.__new__(SavedPipelines); adapter.march = march
    b = inference_binding(capture, projection, frame, contract, 'march')
    _, raw, c = adapter.predict('march', frame, binding=b, package=capture, projection=projection, contract=contract)
    assert raw[0] == .25 and c[0] == .4
    assert march.learner.predict_proba.call_args.kwargs == {'iteration_range': (0,310)}
    assert march.priors == saved
    bad = frame.copy(); bad['scheduled_rounds'] = 3
    b = inference_binding(capture, projection, bad, contract, 'march')
    with pytest.raises(PreflightError, match='schedule'):
        adapter.predict('march', bad, binding=b, package=capture, projection=projection, contract=contract)
    assert march.learner.predict_proba.call_count == 1


def test_resolving_ambiguous_announcement_blocks_before_indexing(capture, contract, monkeypatch):
    from modeling.phase5c1_sources import project
    rows = deepcopy(capture['rows']['fights'])
    f = next(f for f in rows if f['result_type'] == 'upcoming' and f['fight_id'] != capture['receipt']['considered'][0]['fight_id'])
    f['result_type'] = 'draw'
    changed = validate(rewrite(capture, table='fights', values=rows), contract)
    with pytest.raises(ValueError, match='Ambiguous repeated'):
        project(changed)


def test_provisional_considered_rows_and_incomplete_scope(capture, contract, tmp_path):
    r = deepcopy(capture['receipt'])
    r['considered'].append({'source_row_id':'unidentified-page-row', 'event_date':None, 'url':'synthetic://incomplete'})
    changed = validate(rewrite(capture, receipt=r), contract)
    result, _ = run(tmp_path, changed, contract)
    assert result['status'] == 'READY'
    readiness = json.loads((tmp_path/'journal/runs/SYNTHETIC_run/readiness.json').read_bytes())
    assert readiness['considered_count'] == 2 and readiness['bouts'][1]['status'] == 'BLOCKED'
    r['considered'] = []
    with pytest.raises(Blocked, match='coverage'):
        validate(rewrite(changed, receipt=r), contract)


@pytest.mark.parametrize('days,eligible', [(14,True),(15,False),(1,True),(0,False)])
def test_inclusive_observation_timing_window(capture, contract, days, eligible):
    from modeling.phase5c1_runner import check
    from modeling.phase5c1_sources import project
    p = project(capture)
    boundary = instant(capture['receipt']['observation_cutoff']).replace(hour=0,minute=0,second=0) + timedelta(days=days)
    # Direct timing assertions use a midnight cutoff, leaving identity and
    # experience coverage dates unchanged; no real clock is fabricated.
    c = deepcopy(capture)
    c['receipt']['observation_cutoff'] = instant(capture['receipt']['observation_cutoff']).replace(hour=0,minute=0,second=0).isoformat()
    p['targets'][0]['event_date'] = boundary.date().isoformat()
    for a in c['evidence']['assertions']:
        a['claim']['event_date'] = boundary.date().isoformat()
    assert (check(c,p,forecast_at=c['receipt']['observation_cutoff'])['status']=='READY') == eligible


def test_complete_mocked_capture_receipts_and_cleanup_on_query_failure():
    from modeling.phase5c1_sources import mocked_readonly_capture, authorized_readonly_capture
    class Cursor:
        description = []
        def __init__(self, fail=False): self.fail, self.queries = fail, []
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, sql):
            self.queries.append(sql)
            assert '%s' not in sql
            if sql == 'SHOW transaction_read_only': self.description, self.result = [('transaction_read_only',)], [('on',)]
            elif sql == 'SHOW transaction_isolation': self.description, self.result = [('transaction_isolation',)], [('repeatable read',)]
            elif sql.startswith('SELECT transaction_timestamp'):
                self.description = [('transaction_started_at',), ('database_timezone',), ('snapshot',)]
                self.result = [('2026-10-04T12:00:00Z','UTC','synthetic_snapshot')]
            else:
                if self.fail: raise RuntimeError('Synthetic query failure')
                self.description, self.result = [], []
        def fetchall(self): return self.result
    class Connection:
        phase5c1_mock = True
        def __init__(self, fail=False): self.cur, self.rolled_back, self.closed = Cursor(fail), False, False
        def set_session(self, **kw): assert kw == dict(isolation_level='REPEATABLE READ',readonly=True,autocommit=False)
        def cursor(self): return self.cur
        def rollback(self): self.rolled_back = True
        def close(self): self.closed = True
    conn = Connection()
    bodies, receipt = mocked_readonly_capture(lambda:conn, synthetic_authorization='phase5c1_mock_capture_only',clock=now)
    assert set(bodies) == {'events','fighters','fights','fight_stats_aggregate','schemas'}
    assert receipt['transaction']['read_only'] == 'on' and receipt['transaction']['isolation'] == 'repeatable read'
    assert conn.rolled_back and conn.closed and len(receipt['queries']) == 8
    assert not any('bout_features' in q for q in conn.cur.queries)
    failed = Connection(True)
    with pytest.raises(RuntimeError):
        mocked_readonly_capture(lambda:failed, synthetic_authorization='phase5c1_mock_capture_only',clock=now)
    assert failed.rolled_back and failed.closed
    with pytest.raises(Blocked, match='authorization'):
        authorized_readonly_capture(lambda: pytest.fail('Unauthorized real connection'), authorization={})


def test_structured_review_exact_spans_and_access_page_rejection(capture, contract):
    p = Path(capture['directory']); e = deepcopy(capture['evidence'])
    a = e['assertions'][0]
    raw = b'Synthetic authoritative title status: false; ordered participants in signed claim.'
    (p/'evidence/original.body').write_bytes(raw)
    # Keep just one reviewed claim, so the review signature is unambiguous.
    e['assertions'] = [a]
    e['sources']['export']['review'] = {'reviewer':'Synthetic Reviewer','authority':'Synthetic authority',
        'signed_assertion_sha256':digest(a['claim']), 'original_body':'evidence/original.body',
        'original_sha256':sha(raw),'original_provider':'synthetic_provider','original_url':'synthetic://original',
        'original_observed_at':e['sources']['export']['requested_at'],'reviewed_at':e['sources']['export']['observed_at'],
        'spans':[{'start':0,'end':len(raw),'text':raw.decode()}]}
    assert validate(rewrite(capture,evidence=e,claims=[a['claim']]),contract)
    blocked = b'Access denied; checking your browser'
    (p/'evidence/original.body').write_bytes(blocked)
    e['sources']['export']['review']['original_sha256'] = sha(blocked)
    with pytest.raises(Blocked, match='access_check'):
        validate(rewrite(capture,evidence=e,claims=[a['claim']]),contract)


def test_distinct_postfreeze_capture_identity_and_contract_freeze_boundaries(capture, contract):
    r = deepcopy(capture['receipt'])
    r['started_at'] = contract['frozen_at']
    with pytest.raises(Blocked, match='freeze'):
        validate(rewrite(capture,receipt=r),contract)


def test_synthetic_namespace_cannot_be_registered_as_real(tmp_path, capture, contract):
    from modeling.phase5c1_journal import namespace, exclusive
    result,_ = run(tmp_path,capture,contract)
    with exclusive(tmp_path/'journal'), pytest.raises(Blocked,match='mixing'):
        namespace(tmp_path/'journal',synthetic=False)


def test_hash_repinned_feature_tampering_fails_independent_reconstruction(tmp_path,capture,contract):
    from modeling.phase5c1_runner import verify_run
    result,_ = run(tmp_path,capture,contract)
    d = tmp_path/'journal/runs/SYNTHETIC_run'
    f = d/'inputs/challenger.csv'; raw = f.read_bytes(); f.chmod(0o644); f.write_bytes(raw.replace(b'1500',b'1499',1)+b'\n')
    checks = json.loads((d/'checksums.json').read_bytes())
    checks['files']['inputs/challenger.csv'] = sha(f.read_bytes())
    c = d/'checksums.json'; c.chmod(0o644); c.write_bytes(encoded(checks))
    with pytest.raises(Blocked,match='matrix_replay'):
        verify_run(d,sha(c.read_bytes()))


def test_json_duplicate_and_nonfinite_values_fail_closed():
    from modeling.phase5c1_contract import decoded
    for raw in (b'{"title":false,"title":true}', b'{"n":NaN}'):
        with pytest.raises(Blocked): decoded(raw)


@pytest.mark.parametrize('conflict', [False, True])
def test_fresh_identity_claim_whole_rows_statistics_and_conflict_blocker(capture,contract,conflict):
    from uuid import uuid4
    from modeling.phase5_current_data import table_bytes
    from modeling.phase5c1_sources import project
    rows = deepcopy(capture['rows'])
    canonical = rows['fights'][0]
    alias = dict(canonical,fight_id=str(uuid4()),source_url='synthetic://explicit-alias')
    rows['fights'].append(alias)
    copies = [dict(s,fight_id=alias['fight_id'],fight_stat_id=str(uuid4())) for s in rows['fight_stats_aggregate'] if s['fight_id']==canonical['fight_id']]
    if conflict: copies[0]['knockdowns'] += 1
    rows['fight_stats_aggregate'] += copies
    fixture = rewrite(capture,table='fights',values=rows['fights'])
    changed = validate(fixture,contract)
    fixture = rewrite(changed,table='fight_stats_aggregate',values=rows['fight_stats_aggregate'])
    changed = validate(fixture,contract)
    ids = [canonical['fight_id'],alias['fight_id']]
    c = {'relationship':'same_occurrence','fight_ids':ids,'canonical_id':canonical['fight_id'],'explicit_transition':True,
         'event_id':canonical['event_id'],'event_date':rows['events'][0]['event_date'],
         'participant_ids':[canonical['fighter_1_id'],canonical['fighter_2_id']],
         'row_sha256':{f['fight_id']:sha(table_bytes([f])) for f in (canonical,alias)},
         'statistic_sha256':{fid:sorted(sha(table_bytes([s])) for s in rows['fight_stats_aggregate'] if s['fight_id']==fid) for fid in ids}}
    e = deepcopy(changed['evidence']); e['assertions'].append({'kind':'identity','source_id':'export',
        'observed_at':e['sources']['export']['observed_at'],'claim':c,
        'extraction':{'method':'json_pointer_v1','pointer':'/claims/'+str(len(e['assertions'])),'claim_sha256':digest(c)}})
    changed = validate(rewrite(changed,evidence=e,claims=[a['claim'] for a in e['assertions']]),contract)
    if conflict:
        with pytest.raises(Blocked,match='conflicting_source'):
            project(changed)
    else:
        p = project(changed)
        assert p['aliases'][alias['fight_id']] == canonical['fight_id']
        assert sum(f['fight_id']==canonical['fight_id'] for f in p['history']) == 1
        assert all(s['fight_id']!=alias['fight_id'] for s in p['statistics'])
        assert len(p['fight_lineage'])==len(rows['fights'])
        from modeling.phase5c1_runner import check
        assert check(changed,p,forecast_at=simulated_clock(changed)())['status']=='READY'


def test_publication_and_code_pins_refuse_existing_roots(tmp_path):
    from modeling.phase5c1_contract import publish
    p = tmp_path/'frozen'; pin=publish(p,{'receipt.json':encoded({'synthetic':True})})
    assert inventory(p,pin)
    with pytest.raises(FileExistsError): publish(p,{})
    raw=(p/'receipt.json').read_bytes(); (p/'receipt.json').chmod(0o644); (p/'receipt.json').write_bytes(raw+b'\n')
    with pytest.raises(Blocked,match='hash'): inventory(p,pin)


def test_cancellation_prevents_prediction_for_unscored_bout(tmp_path,capture,contract):
    from modeling.phase5c1_journal import cancel,exclusive,register,namespace
    from modeling.phase5c1_synthetic import SyntheticPipelines
    root=tmp_path/'journal'
    with exclusive(root):
        namespace(root,synthetic=True)
        register(root,capture['receipt']['considered'],receipt_pin=capture['receipt_sha256'],recorded_at=simulated_clock(capture)())
        cancel(root,capture['receipt']['considered'][0]['source_row_id'],recorded_at=simulated_clock(capture)(),reason='Synthetic')
    pipes=SyntheticPipelines()
    with pytest.raises(Blocked,match='cancelled'):
        run(tmp_path,capture,contract,pipelines=pipes)
    assert not pipes.calls

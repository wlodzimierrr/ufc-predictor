"""Explicit offline tests. All estimator fits here use synthetic values only."""
from copy import deepcopy
import io
import json
from pathlib import Path
from unittest.mock import patch
from uuid import uuid5, NAMESPACE_URL

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

from features.debut_prior import compute_debut_priors
from modeling import phase5b4_bundle_v1 as b
from modeling import phase5b4_contract_v1 as c
from modeling import phase5b4_trainer_v1 as t
from modeling.phase5b4_safety import loading_guards, preservation


def uid(value):
    return str(uuid5(NAMESPACE_URL, value))


def synthetic(n=100):
    rng = np.random.default_rng(19)
    frame = pd.DataFrame({k: rng.normal(size=n) for k in c.FEATURES})
    for col in c.DEBUT + ['scheduled_rounds']:
        frame[col] = np.nan
    frame['both_debuting'] = (np.arange(n) % 3 == 0).astype(float)
    frame['fight_id'] = [uid('fight' + str(i)) for i in range(n)]
    frame['event_id'] = [uid('event' + str(i)) for i in range(n)]
    frame['fighter_1_id'] = [uid('fighter1' + str(i)) for i in range(n)]
    frame['fighter_2_id'] = [uid('fighter2' + str(i)) for i in range(n)]
    frame['event_date'] = pd.date_range('2020-01-01', periods=n)
    frame['weight_class'] = 'Lightweight'
    frame['label'] = np.arange(n) % 2
    frame['feature_version'] = 2
    return frame[c.META + c.FEATURES]


@pytest.fixture(scope='module')
def approved():
    return t.load_approved_preparation()


@pytest.fixture(scope='module')
def estimators():
    frame = synthetic()
    prior = {'priors': compute_debut_priors(frame.copy(deep=True))}
    prepared = c.transform(frame, prior)
    xgb = XGBClassifier(**c.PARAMETERS).fit(prepared[c.FEATURES], prepared.label)
    raw = np.linspace(0.05, 0.95, 100)
    lr = LogisticRegression(**c.PLATT).fit(c.log_odds(raw), np.arange(100) % 2)
    record = {'coefficients': lr.coef_.tolist(), 'intercept': lr.intercept_.tolist(),
              'n_iter': lr.n_iter_.tolist(), 'classes': [0, 1], 'converged': True}
    return frame, prior, xgb, lr, record


def oof_fixture(approved):
    parts = []
    for f in approved.folds['folds']:
        x = approved.partition(start=f['prediction_start_inclusive'], end=f['prediction_end_exclusive'])[c.META]
        x['fold'] = f['name']
        x['raw_probability_fighter_1'] = np.linspace(0.1, 0.9, len(x))
        x['train_ids_sha256'], x['prediction_ids_sha256'] = f['train_ids_sha256'], f['prediction_ids_sha256']
        x['prior_sha256'], x['learner_sha256'] = '0' * 64, '1' * 64
        parts.append(x)
    return pd.concat(parts, ignore_index=True)


@pytest.fixture
def shell(tmp_path):
    # All required component filenames, deliberately non-estimator bytes. Useful
    # to prove refusals before even attempting a deserialize operation.
    root = tmp_path / 'shell'
    root.mkdir()
    for name in b.COMPONENTS | b.COMPLETION | {'training_manifest.json'}:
        p = root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b'{}\n')
    sums = {str(p.relative_to(root)): c.sha256(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
    raw = c.json_bytes({'files': sums})
    (root / 'checksums.json').write_bytes(raw)
    return root, c.sha256(raw), c.sha256(b'{}\n')


def repin(root):
    sums = {str(p.relative_to(root)): c.sha256(p.read_bytes()) for p in root.rglob('*')
            if p.is_file() and p.name not in {'checksums.json', 'INCOMPLETE'}}
    raw = c.json_bytes({'files': sums})
    (root / 'checksums.json').write_bytes(raw)
    return c.sha256(raw), c.sha256((root / 'training_manifest.json').read_bytes())


@pytest.fixture
def synthetic_staged_bundle(tmp_path, approved, estimators):
    """Full loader fixture: synthetic fitted components, never real-row scoring.

    Approved identity metadata tests current-population gates. Toy probabilities
    are fabricated test inputs and are explicitly marked in fixture receipts.
    """
    root = tmp_path / 'staged'
    root.mkdir(); (root / 'INCOMPLETE').write_text('synthetic fixture')
    def write(name, value):
        p = root / name; p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(value if isinstance(value, bytes) else c.json_bytes(value))
    for name in b.PREPARATION_FILES:
        write('preparation/' + name, (c.PREPARATION / name).read_bytes())
    write('contracts/preprocessing.json', c.PREPROCESSING)
    write('contracts/calibration.json', c.CALIBRATION)
    write('package_versions.json', c.versions()); write('code_versions.json', c.code_hashes())
    write('safe_test_receipt.json', {'status': 'PASSED', 'exit_code': 0, 'safe_test_list': c.SAFE_TESTS, 'code_sha256': c.code_hashes(), 'synthetic_fixture': True})
    write('training_membership.json', {'original_metadata': json.loads(approved.whole[c.META].to_json(orient='records', date_format='iso')),
                                      'orientation': c.ORIENTATION, 'deferred_training_csv_sha256': c.TRAINING_HASH})
    now = c.now()
    for name in ['final', *[w[0] for w in c.WINDOWS]]:
        end = c.CUTOFF if name == 'final' else next(w[1] for w in c.WINDOWS if w[0] == name)
        selected = approved.partition(end=end)
        key = 'final_training' if name == 'final' else name + '_train_preprocessing'
        write(f'priors/{name}.json', dict(contract_version=c.VERSION, name=name, fit_partition='training_rows_only',
            training_rows=len(selected), ordered_training_ids=list(selected.fight_id), training_ids_sha256=c.digest(sorted(selected.fight_id)),
            training_end_exclusive=end, actual_training_endpoint=selected.event_date.max().date().isoformat(),
            input_matrix_with_identity_sha256=approved.inputs[key]['matrix_with_identity_sha256'], preparation_checksums_sha256=c.PREPARATION_ROOT,
            preprocessing_contract_sha256=c.digest(c.PREPROCESSING), priors=estimators[1]['priors'], synthetic_fixture=True))
        (root / 'models').mkdir(exist_ok=True)
        estimators[2].save_model(root / f'models/{name}.json')
        write(f'models/{name}_config.json', {'parameters': c.PARAMETERS, 'rounds': 310, 'fit_arguments': ['X', 'y'], 'orientation': c.ORIENTATION,
            'effective_booster_configuration': json.loads(estimators[2].get_booster().save_config())})
    oof = oof_fixture(approved)
    provenance = {}
    for f in approved.folds['folds']:
        name = f['name']; mask = oof.fold.eq(name)
        ph = c.sha256((root / f'priors/{name}.json').read_bytes()); mh = c.sha256((root / f'models/{name}.json').read_bytes())
        oof.loc[mask, 'prior_sha256'], oof.loc[mask, 'learner_sha256'] = ph, mh
        provenance[name] = dict(prior_sha256=ph, learner_sha256=mh, train_ids_sha256=f['train_ids_sha256'], prediction_ids_sha256=f['prediction_ids_sha256'])
    oof_raw = c.csv_bytes(oof); write('oof.csv', oof_raw)
    write('oof_provenance.json', {'orientation': c.ORIENTATION, 'oof_csv_sha256': c.sha256(oof_raw), 'folds': provenance})
    input_value = dict(contract_version=c.VERSION, ordered_fight_ids=list(oof.fight_id), folds=list(oof.fold),
        labels=list(oof.label.astype(int)), raw_probabilities=list(oof.raw_probability_fighter_1),
        clipped_raw_log_odds=c.log_odds(oof.raw_probability_fighter_1).ravel().tolist(), orientation=c.ORIENTATION,
        calibration_contract_sha256=c.digest(c.CALIBRATION))
    write('calibration_input.json', input_value)
    write('calibrator_parameters.json', {**estimators[4], 'input_sha256': c.digest(input_value), 'contract': c.CALIBRATION, 'convergence_warnings': []})
    stream = io.BytesIO(); joblib.dump(estimators[3], stream); write('calibrator.joblib', stream.getvalue())
    write('authorization.txt', b'synthetic fixture only')
    auth_hash = c.sha256(b'synthetic fixture only')
    write('authorization.json', {'scope': c.VERSION, 'authorization_sha256': auth_hash, 'synthetic_fixture': True})
    write('run_receipt.json', dict(authorization_sha256=auth_hash,
        fit_counts={'oof_xgboost': 4, 'platt': 1, 'final_xgboost': 1, 'partition_priors': 5},
        forecast_calls=0, holdout_scoring_calls=0, trial_outcome_ingestion=0, source_network_calls=0,
        warehouse_connections=0, production_writes=0, search_fits=0, parameters=c.PARAMETERS, fit_arguments=['X', 'y'],
        synthetic_fixture=True, description='Contract fixture counters; actual synthetic fits are one XGBoost and one Platt.'))
    write('metadata.json', {'limitations': c.LIMITATIONS, 'orientation': c.ORIENTATION,
        'training_reference_source_sha256': approved.manifest['source_sha256'],
        'component_freeze_times': {n: now for n in ['final', *[w[0] for w in c.WINDOWS], 'calibrator']}, 'synthetic_fixture': True})
    write('preservation_baseline.json', {'synthetic_fixture': True}); write('diagnostics.json', {'synthetic_fixture': True})
    components = {n: c.sha256((root / n).read_bytes()) for n in b.COMPONENTS}
    write('training_manifest.json', dict(contract_version=c.VERSION, artifact_readiness='READY', parameters=c.PARAMETERS,
        limitations=c.LIMITATIONS, component_sha256=components, event_cutoff_exclusive=c.CUTOFF,
        preparation_checksums_sha256=c.PREPARATION_ROOT, preparation_manifest_sha256=c.PREPARATION_MANIFEST,
        source_sha256=approved.manifest['source_sha256'], synthetic_fixture=True))
    manifest_hash = c.sha256((root / 'training_manifest.json').read_bytes())
    sums = {'files': {**components, 'training_manifest.json': manifest_hash}}
    write('staging_checksums.json', sums)
    return root, c.digest(sums), manifest_hash


def test_full_staged_loader_without_fitting_and_synthetic_component_parity(synthetic_staged_bundle, estimators):
    root, checksum, manifest = synthetic_staged_bundle
    with loading_guards():
        bundle = b._load(root, expected_checksums_sha256=checksum, expected_manifest_sha256=manifest, staged=True, allow_incomplete=True)
        prepared = c.transform(estimators[0], estimators[1])
        assert np.array_equal(bundle.models['final'].predict_proba(prepared[c.FEATURES]), estimators[2].predict_proba(prepared[c.FEATURES]))
        raw = np.linspace(0.1, 0.9, 100)
        assert np.array_equal(c.calibrate(bundle.calibrator, raw), c.calibrate(estimators[3], raw))


@pytest.mark.parametrize('mutation', ['valid', 'missing', 'algorithm', 'source', 'input_hash', 'outcome', 'early_capture'])
def test_synthetic_inference_actual_provenance(synthetic_staged_bundle, estimators, mutation):
    root, checksum, manifest = synthetic_staged_bundle
    with loading_guards():
        bundle = b._load(root, expected_checksums_sha256=checksum, expected_manifest_sha256=manifest, staged=True, allow_incomplete=True)
        frame = estimators[0].copy(deep=True)
        frame['label'] = np.nan; frame['event_date'] = pd.date_range('2027-01-01', periods=len(frame))
        provenance = t.verification_provenance(bundle, frame)
        provenance['purpose'] = 'separately_authorized_inference'
        provenance['source_sha256'] = {k: '2' * 64 for k in provenance['source_sha256']}
        provenance['capture_completed_at'] = '2027-01-01T00:00:00+00:00'
        if mutation == 'missing': provenance.pop('source_sha256')
        elif mutation == 'algorithm': provenance['algorithm'] = 'legacy'
        elif mutation == 'source': provenance['source_sha256'] = bundle.manifest['source_sha256']
        elif mutation == 'input_hash': provenance['input_matrix_with_identity_sha256'] = '0' * 64
        elif mutation == 'outcome':
            frame['label'] = 1; provenance['input_matrix_with_identity_sha256'] = c.sha256(c.csv_bytes(frame))
        elif mutation == 'early_capture': provenance['capture_completed_at'] = '2026-01-01T00:00:00+00:00'
        if mutation == 'valid':
            prepared, raw, cal = bundle.predict(frame, source_provenance=provenance)
            assert prepared.equals(c.transform(frame, estimators[1]))
            assert np.array_equal(raw, estimators[2].predict_proba(prepared[c.FEATURES])[:, 1])
            assert np.array_equal(cal, c.calibrate(estimators[3], raw))
        else:
            with pytest.raises(c.ContractError): bundle.predict(frame, source_provenance=provenance)


@pytest.mark.parametrize('mutation', ['prior_membership', 'prior_value', 'calibrator_parameters', 'oof_lineage'])
def test_full_loader_component_provenance_refusals(synthetic_staged_bundle, mutation):
    root, _, _ = synthetic_staged_bundle
    if mutation.startswith('prior'):
        name = 'priors/oof_1.json'; data = b.read_json(root, name)
        if mutation == 'prior_membership': data['ordered_training_ids'] = data['ordered_training_ids'][:-1]
        else: data['priors']['global_height_std'] = -1
    elif mutation == 'calibrator_parameters':
        name = 'calibrator_parameters.json'; data = b.read_json(root, name); data['n_iter'] = [1000]
    else:
        name = 'oof_provenance.json'; data = b.read_json(root, name); data['folds']['oof_1']['learner_sha256'] = '0' * 64
    (root / name).write_bytes(c.json_bytes(data))
    m = b.read_json(root, 'training_manifest.json'); m['component_sha256'][name] = c.digest(data)
    (root / 'training_manifest.json').write_bytes(c.json_bytes(m)); manifest = c.digest(m)
    sums = {'files': {**m['component_sha256'], 'training_manifest.json': manifest}}
    (root / 'staging_checksums.json').write_bytes(c.json_bytes(sums))
    with patch.object(b.XGBClassifier, 'load_model', side_effect=AssertionError('deserialized too early')), patch.object(b.joblib, 'load', side_effect=AssertionError('deserialized too early')):
        with pytest.raises(c.ContractError): b._load(root, expected_checksums_sha256=c.digest(sums), expected_manifest_sha256=manifest, staged=True, allow_incomplete=True)


def test_exact_real_population_and_all_partitions(approved):
    whole = approved.whole
    assert len(whole) == 8558 and len(approved.exclusions) == 166
    assert whole.event_date.max().date().isoformat() == '2026-08-29'
    for f in approved.folds['folds']:
        train = approved.partition(end=f['train_end_exclusive'])
        pred = approved.partition(start=f['prediction_start_inclusive'], end=f['prediction_end_exclusive'])
        approved.guard(train, expected_ids=f['train_fight_ids'], end=f['train_end_exclusive'])
        approved.guard(pred, expected_ids=f['prediction_fight_ids'], start=f['prediction_start_inclusive'], end=f['prediction_end_exclusive'])
        assert set(train.event_id).isdisjoint(pred.event_id)
        assert set(train.fight_id).isdisjoint(approved.exclusions)
        assert set(pred.fight_id).isdisjoint(approved.exclusions)


@pytest.mark.parametrize('mutation', ['omit', 'add', 'order', 'label', 'orientation', 'date', 'feature', 'schema', 'exclude', 'schedule'])
def test_partition_refusals(approved, mutation):
    f = approved.folds['folds'][0]
    frame = approved.partition(end=f['train_end_exclusive'])
    if mutation == 'omit': frame = frame.iloc[:-1].copy()
    elif mutation == 'add': frame = pd.concat([frame, approved.whole.iloc[[-1]]], ignore_index=True)
    elif mutation == 'order': frame = frame.iloc[::-1].copy()
    elif mutation == 'label': frame.loc[0, 'label'] = 1 - frame.loc[0, 'label']
    elif mutation == 'orientation': frame.loc[0, ['fighter_1_id', 'fighter_2_id']] = frame.loc[0, ['fighter_2_id', 'fighter_1_id']].to_numpy()
    elif mutation == 'date': frame.loc[0, 'event_date'] = pd.Timestamp('2026-10-02')
    elif mutation == 'feature': frame.loc[0, 'diff_elo'] += 1
    elif mutation == 'schema': frame = frame[[*frame.columns[:-2], frame.columns[-1], frame.columns[-2]]]
    elif mutation == 'exclude': frame.loc[0, 'fight_id'] = next(iter(approved.exclusions))
    elif mutation == 'schedule': frame.loc[0, 'scheduled_rounds'] = 3
    with pytest.raises((c.ContractError, ValueError)):
        approved.guard(frame, expected_ids=f['train_fight_ids'], end=f['train_end_exclusive'])


def test_date_split_and_alias_exclusion_even_if_expected():
    whole = synthetic(4)
    whole.loc[1, ['event_id', 'event_date']] = whole.loc[0, ['event_id', 'event_date']].to_numpy()
    selected = whole.iloc[[0]].copy()
    with pytest.raises(c.ContractError):
        c.guard_partition(whole, selected, expected_ids=list(selected.fight_id), exclusions=set(), end='2020-01-02')
    excluded_alias = whole.iloc[0].fight_id
    with pytest.raises(c.ContractError, match='excluded'):
        c.guard_partition(whole, whole, expected_ids=list(whole.fight_id), exclusions={excluded_alias})


def test_training_only_priors_and_deep_copies():
    whole = synthetic(100)
    whole.loc[80:, 'diff_height_cm'] = 1e8
    original = whole.copy(deep=True)
    class Partition:
        def guard(self, frame, **kwargs):
            c.guard_partition(whole, frame, exclusions=set(), **kwargs)
    train = whole.iloc[:80].copy()
    prior = t.fit_priors(Partition(), train, name='synthetic', expected_ids=list(train.fight_id), end='2020-03-21')
    assert prior['priors'] == compute_debut_priors(train.copy(deep=True))
    assert prior['priors'] != compute_debut_priors(whole.copy(deep=True))
    assert prior['ordered_training_ids'] == list(train.fight_id)
    c.transform(train, prior)
    assert whole.equals(original) and train.equals(original.iloc[:80])
    final = compute_debut_priors(whole.copy(deep=True))
    assert final != prior['priors']  # Independent final population cannot replace fold priors.


@pytest.mark.parametrize('field', ['global_height_std', 'global_reach_std', 'base_prior', 'training_debut_win_rate'])
def test_prior_normalization_refusal(estimators, field):
    prior = deepcopy(estimators[1]['priors'])
    prior[field] = np.nan
    with pytest.raises(c.ContractError): c.check_priors(prior)


def test_unknown_schedules_and_legitimate_missing_values(estimators):
    frame, prior, *_ = estimators
    frame = frame.copy(deep=True)
    frame.loc[0, 'diff_height_cm'] = np.nan
    result = c.transform(frame, prior)
    assert result.scheduled_rounds.isna().all()
    assert pd.isna(result.loc[0, 'debut_height_adv'])
    assert result.loc[result.both_debuting.eq(0), c.DEBUT].isna().all().all()


@pytest.mark.parametrize('mutation', ['duplicate', 'omit', 'label', 'orientation', 'fold', 'lineage', 'range', 'nan', 'order'])
def test_oof_refusals(approved, mutation):
    oof = oof_fixture(approved)
    c.validate_oof(approved.whole, oof, approved.folds, approved.exclusions)
    if mutation == 'duplicate': oof.loc[1, 'fight_id'] = oof.loc[0, 'fight_id']
    elif mutation == 'omit': oof = oof.iloc[:-1]
    elif mutation == 'label': oof.loc[0, 'label'] = 1-oof.loc[0, 'label']
    elif mutation == 'orientation': oof.loc[0, 'fighter_1_id'] = uid('different')
    elif mutation == 'fold': oof.loc[0, 'fold'] = 'oof_2'
    elif mutation == 'lineage': oof.loc[0, 'train_ids_sha256'] = '0' * 64
    elif mutation == 'range': oof.loc[0, 'raw_probability_fighter_1'] = 2
    elif mutation == 'nan': oof.loc[0, 'raw_probability_fighter_1'] = np.nan
    elif mutation == 'order': oof = oof.iloc[::-1]
    with pytest.raises(c.ContractError): c.validate_oof(approved.whole, oof, approved.folds, approved.exclusions)


def test_fixed_learner_and_synthetic_save_load_parity(estimators, tmp_path):
    frame, prior, model, *_ = estimators
    c.check_learner(model)
    p = tmp_path / 'model.json'
    model.save_model(p)
    loaded = XGBClassifier(**c.PARAMETERS)
    loaded.load_model(p)
    c.check_learner(loaded)
    prepared = c.transform(frame, prior)
    assert np.array_equal(model.predict_proba(prepared[c.FEATURES]), loaded.predict_proba(prepared[c.FEATURES]))
    assert loaded.get_booster().num_boosted_rounds() == 310
    assert loaded.get_params()['early_stopping_rounds'] is None


@pytest.mark.parametrize('parameter,value', [('n_estimators', 309), ('max_depth', 7), ('early_stopping_rounds', 10), ('scale_pos_weight', 2)])
def test_fixed_parameter_refusal(estimators, parameter, value):
    model = deepcopy(estimators[2])
    model.set_params(**{parameter: value})
    with pytest.raises(c.ContractError): c.check_learner(model)


def test_fit_path_has_no_eval_or_reweighting(estimators):
    whole, prior, model, *_ = estimators
    prepared = c.transform(whole, prior)
    class Partition:
        def guard(self, frame, **kw): c.guard_partition(whole, frame, exclusions=set(), **kw)
        def partition(self, **kw): return whole.copy(deep=True)
    with patch.object(t, 'XGBClassifier', return_value=model) as factory, patch.object(model, 'fit', return_value=model) as fit:
        t.fit_xgb(Partition(), prepared, prior, expected_ids=list(whole.fight_id), end=c.CUTOFF)
        assert factory.call_args.kwargs == c.PARAMETERS
        assert len(fit.call_args.args) == 2 and fit.call_args.kwargs == {}


def test_calibration_clipping_orientation_and_synthetic_parity(estimators):
    _, _, _, lr, record = estimators
    c.check_calibrator(lr, record)
    raw = np.array([0., 1., 0.3, 0.8])
    expected = np.clip(raw, 1e-8, 1-1e-8)
    assert np.array_equal(c.log_odds(raw).ravel(), np.log(expected/(1-expected)))
    assert np.array_equal(c.calibrate(lr, raw), lr.predict_proba(c.log_odds(raw))[:, 1])
    stream = io.BytesIO(); joblib.dump(lr, stream); stream.seek(0)
    loaded = joblib.load(stream)
    assert np.array_equal(c.calibrate(lr, raw), c.calibrate(loaded, raw))
    scores = c.metrics([1, 0], [0, 1])
    assert scores['brier'] == 1 and scores['natural_log_loss'] == pytest.approx(-np.mean(np.log([1e-8, 1-(1-1e-8)])))


@pytest.mark.parametrize('mutation', ['orientation', 'nonconvergence', 'coefficient', 'recipe'])
def test_calibrator_refusals(estimators, mutation):
    lr, record = deepcopy(estimators[3]), deepcopy(estimators[4])
    if mutation == 'orientation': lr.classes_ = np.array([1, 0])
    elif mutation == 'nonconvergence': lr.n_iter_[:] = 1000
    elif mutation == 'coefficient': record['coefficients'][0][0] += 1
    elif mutation == 'recipe': lr.set_params(C=1)
    with pytest.raises(c.ContractError): c.check_calibrator(lr, record)


def test_nonconverged_platt_stops_without_fallback(approved, estimators):
    oof = oof_fixture(approved)
    lr = deepcopy(estimators[3]); lr.n_iter_[:] = 1000
    with patch.object(t, 'LogisticRegression', return_value=lr), patch.object(lr, 'fit', return_value=lr):
        with pytest.raises(c.ContractError, match='did not converge'):
            t.fit_platt(approved.whole, oof, approved.folds, approved.exclusions, '0' * 64)


@pytest.mark.parametrize('mutation', ['checksum', 'manifest', 'extra', 'missing', 'path', 'symlink', 'incomplete', 'repinned_manifest'])
def test_loader_tampering_refused_before_deserialization(shell, mutation):
    root, checksum, manifest = shell
    if mutation == 'checksum': checksum = '0' * 64
    elif mutation == 'manifest': manifest = '0' * 64
    elif mutation == 'extra': (root / 'extra').write_text('extra')
    elif mutation == 'missing': (root / 'models/final.json').unlink()
    elif mutation == 'path':
        data = b.read_json(root, 'checksums.json'); data['files']['../escape'] = '0' * 64
        raw = c.json_bytes(data); (root / 'checksums.json').write_bytes(raw); checksum = c.sha256(raw)
    elif mutation == 'symlink':
        p = root / 'models/final.json'; p.unlink(); p.symlink_to(root / 'metadata.json')
    elif mutation == 'incomplete': (root / 'INCOMPLETE').write_text('pending')
    elif mutation == 'repinned_manifest':
        (root / 'training_manifest.json').write_bytes(c.json_bytes({'contract_version': 'incompatible'}))
        checksum, manifest = repin(root)
    with patch.object(b.XGBClassifier, 'load_model', side_effect=AssertionError('deserialized too early')), patch.object(b.joblib, 'load', side_effect=AssertionError('deserialized too early')):
        with pytest.raises(c.ContractError):
            b.load_challenger_bundle(root, expected_checksums_sha256=checksum, expected_manifest_sha256=manifest)


@pytest.mark.parametrize('mutation', ['packages', 'contracts', 'configuration', 'code'])
def test_repinned_semantic_incompatibility(shell, mutation):
    root, _, _ = shell
    files = b.read_json(root, 'checksums.json')['files']
    # Build sufficient non-estimator provenance to reach semantic gates.
    for n in b.PREPARATION_FILES:
        (root / 'preparation' / n).write_bytes((c.PREPARATION / n).read_bytes())
    (root / 'contracts/preprocessing.json').write_bytes(c.json_bytes(c.PREPROCESSING))
    (root / 'contracts/calibration.json').write_bytes(c.json_bytes(c.CALIBRATION))
    (root / 'package_versions.json').write_bytes(c.json_bytes(c.versions()))
    (root / 'code_versions.json').write_bytes(c.json_bytes(c.code_hashes()))
    if mutation == 'packages': (root / 'package_versions.json').write_bytes(c.json_bytes({'xgboost': 'wrong'}))
    elif mutation == 'contracts': (root / 'contracts/preprocessing.json').write_bytes(c.json_bytes({**c.PREPROCESSING, 'missing_values': 'invented imputation'}))
    elif mutation == 'configuration': (root / 'preparation/preregistered_configuration.json').write_bytes(b'{}')
    elif mutation == 'code': (root / 'code_versions.json').write_bytes(c.json_bytes({'wrong.py': '0' * 64}))
    components = {n: c.sha256((root / n).read_bytes()) for n in b.COMPONENTS}
    m = dict(contract_version=c.VERSION, artifact_readiness='READY', component_sha256=components,
             parameters=c.PARAMETERS, limitations=c.LIMITATIONS, preparation_checksums_sha256=c.PREPARATION_ROOT,
             preparation_manifest_sha256=c.PREPARATION_MANIFEST)
    (root / 'training_manifest.json').write_bytes(c.json_bytes(m))
    checksum, manifest = repin(root)
    with patch.object(b.XGBClassifier, 'load_model', side_effect=AssertionError('deserialized too early')), patch.object(b.joblib, 'load', side_effect=AssertionError('deserialized too early')):
        with pytest.raises(c.ContractError): b.load_challenger_bundle(root, expected_checksums_sha256=checksum, expected_manifest_sha256=manifest)


def test_loading_cannot_fit_or_query_sources(estimators):
    with loading_guards():
        with pytest.raises(RuntimeError): XGBClassifier().fit([[0]], [0])
        with pytest.raises(RuntimeError): LogisticRegression().fit([[0]], [0])
        import features.debut_prior
        with pytest.raises(RuntimeError): features.debut_prior.compute_debut_priors(synthetic())
    import socket
    import requests
    import warehouse.db
    with pytest.raises(RuntimeError): socket.create_connection(('example.com', 80))
    with pytest.raises(RuntimeError): requests.get('https://example.com')
    with pytest.raises(RuntimeError): warehouse.db.get_connection()
    with pytest.raises(RuntimeError): warehouse.db.upsert(None, 'x', [], [])
    with pytest.raises(RuntimeError): (c.ROOT / 'models/forbidden').write_bytes(b'no')


def test_overwrite_and_isolation_refusal(tmp_path):
    with pytest.raises(c.ContractError): t.start_stage(tmp_path / 'outside')
    with patch.object(t, 'OUTPUT', tmp_path):
        run = t.start_stage(tmp_path / 'test')
        assert (run / 'INCOMPLETE').exists()
        with pytest.raises(c.ContractError): t.start_stage(run)
        t.exclusive_write(run, 'test.json', b'{}')
        with pytest.raises(FileExistsError): t.exclusive_write(run, 'test.json', b'{}')


def test_artifact_preservation_and_changed_bytes_refusal(tmp_path):
    from modeling import phase5b4_safety as safety
    for name in ('models', 'data/raw', 'data/holdouts', 'data/audits', 'data/experiments/frozen'):
        (tmp_path / name).mkdir(parents=True, exist_ok=True)
    protected = tmp_path / 'models/reference'
    protected.write_bytes(b'frozen')
    baseline = {'files': {'models/reference': c.sha256(b'frozen')}}
    with patch.object(safety, 'ROOT', tmp_path), patch.object(safety, 'OUTPUT', tmp_path / 'data/experiments/challenger'):
        receipt = preservation(baseline)
        assert receipt['changed'] == receipt['missing'] == receipt['new_protected_files'] == []
    protected.write_bytes(b'changed')
    with patch.object(safety, 'ROOT', tmp_path), patch.object(safety, 'OUTPUT', tmp_path / 'data/experiments/challenger'):
        with pytest.raises(c.ContractError): preservation(baseline)


def test_inference_requires_semantics_and_actual_source_binding():
    bundle = b.ChallengerBundle(Path('/unused'), {}, {}, pd.DataFrame(), {}, {}, None, pd.DataFrame())
    with pytest.raises(c.ContractError, match='actual inference-source'):
        bundle.predict(synthetic(), source_provenance={})

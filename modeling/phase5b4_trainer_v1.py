"""One fixed current challenger run, staged until exact independent replay."""
from copy import deepcopy
import io
import json
from pathlib import Path
import warnings

import joblib
import numpy as np
import pandas as pd
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

from modeling.phase5b4_contract_v1 import (
    ALGORITHM, CALIBRATION, CONFIG_HASH, CUTOFF, FEATURES, FOLDS_HASH,
    LIMITATIONS, META, ORIENTATION, OUTPUT, PARAMETERS, PLATT, PREPARATION,
    PREPARATION_MANIFEST, PREPARATION_ROOT, PREPROCESSING, TRAINING_HASH,
    VERSION, WINDOWS, SAFE_TESTS, ContractError, calibrate, check_calibrator, check_learner, check_effective_configuration,
    check_priors, code_hashes, csv_bytes, digest, guard_partition, json_bytes,
    log_odds, metrics, now, probabilities, sha256, transform, validate_frame,
    validate_oof, versions,
)
from modeling.phase5b4_bundle_v1 import (
    COMPONENTS, PREPARATION_FILES, _load,
    read_json, validate_membership,
)
from modeling.phase5b4_safety import loading_guards, preservation


def load_approved_preparation():
    # The preparation loader alone parses its CSV, following all accepted gates.
    from modeling.phase5b3_safety import preparation_guards
    with preparation_guards():
        from modeling.phase5_role_aware import load_role_aware_preparation
        frame, manifest, folds = load_role_aware_preparation(PREPARATION,
            expected_checksums_sha256=PREPARATION_ROOT,
            expected_training_manifest_sha256=PREPARATION_MANIFEST)
    return ApprovedPreparation(frame, manifest, folds)


class ApprovedPreparation:
    def __init__(self, frame, manifest, folds):
        self._whole = frame.copy(deep=True)
        self.manifest = deepcopy(manifest)
        self.folds = deepcopy(folds)
        validate_frame(self._whole, deferred=True)
        if sha256(csv_bytes(self._whole)) != TRAINING_HASH or digest(folds) != FOLDS_HASH:
            raise ContractError('Exact accepted input bytes required')
        normalization = read_json(PREPARATION, 'normalization.json')
        self.exclusions = frozenset(normalization['expanded_fitting_exclusion_ids'])
        self.inputs = read_json(PREPARATION, 'preprocessing_inputs.json')
        if len(self.exclusions) != 166 or manifest['summary']['latest_binary_event'] != '2026-08-29' or manifest['event_cutoff_exclusive'] != CUTOFF:
            raise ContractError('Accepted current population/endpoint mismatch')
        validate_membership(json.loads(self._whole[META].to_json(orient='records', date_format='iso')),
                            folds, self.exclusions)
        # Explicit source orientation/labels, separate from reconstructed features.
        fitting = {r['fight_id']: r for r in read_json(PREPARATION, 'fitting_projection.json')}
        for row in self._whole[META].to_dict('records'):
            f = fitting[row['fight_id']]
            if any(row[k] != f[k] for k in META[:4]) or row['label'] != int(f['winner_fighter_id'] == f['fighter_1_id']):
                raise ContractError('Original source label/orientation mismatch')
        admissions = read_json(PREPARATION, 'partition_role_admissions.json')
        roles = {'final_fitting': set(frame.fight_id), 'final_preprocessing': set(frame.fight_id)}
        combined = set()
        for f in folds['folds']:
            roles[f['name'] + '_fitting_and_preprocessing'] = set(f['train_fight_ids'])
            roles[f['name'] + '_prediction_and_transform'] = set(f['prediction_fight_ids'])
            combined.update(f['prediction_fight_ids'])
        roles['calibration_oof'] = combined
        if len(admissions) != 8992 or len({r['fight_id'] for r in admissions}) != 8992:
            raise ContractError('Complete raw-row role admissions required')
        for name, expected in roles.items():
            actual = {r['fight_id'] for r in admissions if r['roles'][name]['admitted']}
            if actual != expected or actual & self.exclusions or any(r['canonical_id'] in self.exclusions and r['roles'][name]['admitted'] for r in admissions):
                raise ContractError('Role admissions/excluded alias propagation mismatch: ' + name)
        self._admission_hash = sha256((PREPARATION / 'partition_role_admissions.json').read_bytes())
        self._fold_hash = digest(self.folds)
        self.initial_checks = {'guarded_loading': 'VERIFIED', 'source_orientation': 'EXACT',
            'role_admissions': 'EXACT', 'partition_admissions_sha256': self._admission_hash,
            'final_rows': len(frame), 'oof_rows': folds['oof_rows'], 'excluded_ids': len(self.exclusions),
            'accepted_input_sha256': TRAINING_HASH, 'verified_at': now()}

    @property
    def whole(self):
        return self._whole.copy(deep=True)

    def partition(self, *, start=None, end=CUTOFF):
        mask = self._whole.event_date < pd.Timestamp(end)
        if start:
            mask &= self._whole.event_date >= pd.Timestamp(start)
        return self._whole.loc[mask].copy(deep=True).reset_index(drop=True)

    def guard(self, frame, *, expected_ids, start=None, end=CUTOFF):
        if sha256(csv_bytes(self._whole)) != TRAINING_HASH or digest(self.folds) != self._fold_hash:
            raise ContractError('Accepted inputs mutated during lifecycle')
        guard_partition(self._whole, frame, expected_ids=expected_ids,
                        exclusions=self.exclusions, start=start, end=end)
        key = 'final_training' if start is None and end == CUTOFF else None
        for f in self.folds['folds']:
            if start is None and end == f['train_end_exclusive']:
                key = f['name'] + '_train_preprocessing'
            if start == f['prediction_start_inclusive'] and end == f['prediction_end_exclusive']:
                key = f['name'] + '_oof_transform'
        if key is None:
            raise ContractError('Operation outside saved partition contract')
        original = self.partition(start=start, end=end)
        if list(original.fight_id) != self.inputs[key]['ordered_fight_ids'] or sha256(csv_bytes(original)) != self.inputs[key]['matrix_with_identity_sha256']:
            raise ContractError('Accepted partition values/order changed')


def exclusive_write(directory, name, raw):
    p = directory / name
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open('xb') as stream:
        stream.write(raw)
    p.chmod(0o444)
    return sha256(raw)


def start_stage(destination):
    destination = Path(destination)
    if destination.is_symlink() or not destination.resolve().is_relative_to(OUTPUT) or destination.resolve() == OUTPUT:
        raise ContractError('Publication must remain exclusively in the Phase5B4 root')
    if destination.exists():
        raise ContractError('Never overwrite a challenger run')
    destination.mkdir(parents=True, exist_ok=False)
    (destination / 'INCOMPLETE').write_bytes(b'Phase5B4 staged; not verified or loadable\n')
    return destination


def fit_priors(approved, train, *, name, expected_ids, end):
    approved.guard(train, expected_ids=expected_ids, end=end)
    from features.debut_prior import compute_debut_priors
    started = now()
    priors = compute_debut_priors(train.copy(deep=True))
    check_priors(priors)
    return dict(contract_version=VERSION, name=name, fit_partition='training_rows_only',
                training_rows=len(train), ordered_training_ids=list(train.fight_id),
                training_ids_sha256=digest(sorted(train.fight_id)), training_end_exclusive=end,
                actual_training_endpoint=train.event_date.max().date().isoformat(),
                input_matrix_with_identity_sha256=sha256(csv_bytes(train)),
                preparation_checksums_sha256=PREPARATION_ROOT, preprocessing_contract_sha256=digest(PREPROCESSING),
                computation_started_at=started, computation_completed_at=now(), priors=priors)


def preprocess(approved, frame, record, *, expected_ids, start=None, end=CUTOFF):
    approved.guard(frame, expected_ids=expected_ids, start=start, end=end)
    result = transform(frame.copy(deep=True), deepcopy(record))
    approved.guard(result, expected_ids=expected_ids, start=start, end=end)
    return result


def fit_xgb(approved, train, prior, *, expected_ids, end):
    approved.guard(train, expected_ids=expected_ids, end=end)
    original = approved.partition(end=end)
    approved.guard(original, expected_ids=expected_ids, end=end)
    if not train.equals(transform(original, prior)):
        raise ContractError('Exact training-only prepared matrix required before learner fit')
    model = XGBClassifier(**deepcopy(PARAMETERS))
    started = now()
    model.fit(train[FEATURES].copy(deep=True), train.label.copy(deep=True))
    completed = now()
    check_learner(model)
    record = {'fit_started_at': started, 'fit_completed_at': completed,
                   'parameters': deepcopy(PARAMETERS), 'fit_arguments': ['X', 'y'],
                   'rounds': model.get_booster().num_boosted_rounds(), 'orientation': ORIENTATION,
                   'effective_booster_configuration': json.loads(model.get_booster().save_config())}
    check_effective_configuration(record)
    return model, record


def fit_platt(whole, oof, folds, exclusions, input_hash):
    validate_oof(whole, oof, folds, exclusions)
    model = LogisticRegression(**deepcopy(PLATT))
    started = now()
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always', ConvergenceWarning)
        model.fit(log_odds(oof.raw_probability_fighter_1), oof.label.to_numpy(dtype=int).copy())
    convergence = [str(w.message) for w in seen if issubclass(w.category, ConvergenceWarning)]
    if convergence or (model.n_iter_ >= 1000).any():
        raise ContractError('Preregistered Platt did not converge; no fallback authorized')
    record = {'input_sha256': input_hash, 'fit_started_at': started, 'fit_completed_at': now(),
              'coefficients': model.coef_.tolist(), 'intercept': model.intercept_.tolist(),
              'classes': model.classes_.tolist(), 'n_iter': model.n_iter_.tolist(),
              'converged': True, 'convergence_warnings': convergence, 'contract': CALIBRATION}
    check_calibrator(model, record)
    return model, record


def save_learner(directory, name, model, record):
    # Native XGBoost JSON, rather than a pickle of the classifier.
    p = directory / f'models/{name}.json'
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        raise ContractError('Refuse saved learner overwrite')
    model.save_model(p)
    p.chmod(0o444)
    frozen = now()
    exclusive_write(directory, f'models/{name}_config.json', json_bytes(record))
    return sha256(p.read_bytes()), frozen


def verification_provenance(bundle, frame):
    prep_code = read_json(bundle.directory, 'preparation/code_versions.json')
    return dict(algorithm=ALGORITHM, feature_version=2, orientation=ORIENTATION,
        modeling_policy='phase5_role_aware_preparation_v2', preprocessing_contract_sha256=digest(PREPROCESSING),
        feature_code_sha256={k: v for k, v in prep_code.items() if k.startswith('features/') or k == 'modeling/phase5_role_aware.py'},
        source_sha256=deepcopy(bundle.manifest['source_sha256']),
        capture_completed_at=read_json(bundle.directory, 'preparation/training_manifest.json')['capture_completed_at'],
        input_matrix_with_identity_sha256=sha256(csv_bytes(frame)), purpose='persistence_verification')


def array_hash(values):
    return sha256(np.asarray(values, dtype='<f8').tobytes())


def independent_replay(bundle, approved, *, in_memory=None):
    """Saved components only; exact comparisons, no fitting or source acquisition."""
    started = now()
    receipts = {}
    matrices = read_json(bundle.directory, 'run_receipt.json')['prepared_matrix_sha256']
    for f in bundle.folds['folds']:
        name = f['name']
        train = approved.partition(end=f['train_end_exclusive'])
        train_prepared = preprocess(approved, train, bundle.priors[name], expected_ids=f['train_fight_ids'], end=f['train_end_exclusive'])
        if sha256(csv_bytes(train_prepared)) != matrices[name + '_train']:
            raise ContractError('Saved fold training preprocessing mismatch')
        pred = approved.partition(start=f['prediction_start_inclusive'], end=f['prediction_end_exclusive'])
        prepared = preprocess(approved, pred, bundle.priors[name], expected_ids=f['prediction_fight_ids'], start=f['prediction_start_inclusive'], end=f['prediction_end_exclusive'])
        if sha256(csv_bytes(prepared)) != matrices[name + '_oof']:
            raise ContractError('Saved fold OOF preprocessing mismatch')
        raw = probabilities(bundle.models[name].predict_proba(prepared[FEATURES])[:, 1])
        saved = bundle.oof.loc[bundle.oof.fold.eq(name), 'raw_probability_fighter_1'].to_numpy(dtype=float)
        if not np.array_equal(raw, saved):
            raise ContractError('Exact saved fold probability parity failed: ' + name)
        receipts[name] = dict(rows=len(raw), exact_parity=True, raw_probability_sha256=array_hash(raw),
                              train_matrix_sha256=matrices[name + '_train'], oof_matrix_sha256=matrices[name + '_oof'])
    whole = approved.whole
    approved.guard(whole, expected_ids=list(whole.fight_id))
    prepared, raw, cal = bundle.predict(whole, source_provenance=verification_provenance(bundle, whole))
    final_hashes = {'prepared_matrix_sha256': sha256(csv_bytes(prepared)),
                    'raw_probability_sha256': array_hash(raw), 'calibrated_probability_sha256': array_hash(cal)}
    expected = read_json(bundle.directory, 'run_receipt.json')['in_memory_final_verification']
    if final_hashes != expected:
        raise ContractError('Independent final save/load parity failed')
    if in_memory is not None:
        x, p, q = in_memory
        if not prepared.equals(x) or not np.array_equal(raw, p) or not np.array_equal(cal, q):
            raise ContractError('In-memory final matrix/probability exact parity failed')
    return {'status': 'VERIFIED', 'started_at': started, 'completed_at': now(),
            'oof_replay_rows': 341, 'final_verification_rows': 8558, 'exact_parity': True,
            'folds': receipts, 'final': final_hashes, 'fit_calls': 0,
            'scope': 'OOF and persistence verification on approved training rows only',
            'actual_inference_source_sha256': deepcopy(bundle.manifest['source_sha256'])}


def train_and_publish(destination, *, authorization_path, baseline_path, test_receipt_path, command):
    if Path(destination).exists():
        raise ContractError('Never overwrite a challenger run')
    started = now()
    tests = json.loads(test_receipt_path.read_bytes())
    if tests.get('status') != 'PASSED' or tests.get('exit_code') != 0 or tests.get('safe_test_list') != SAFE_TESTS or tests.get('code_sha256') != code_hashes():
        raise ContractError('Current explicit safe-test receipt required before real fitting')
    if sha256((PREPARATION / 'preregistered_configuration.json').read_bytes()) != CONFIG_HASH:
        raise ContractError('Preregistration byte pin mismatch')
    baseline_raw = baseline_path.read_bytes()
    baseline = json.loads(baseline_raw)
    preservation(baseline)
    auth_raw = authorization_path.read_bytes()
    if not auth_raw or b'four OOF XGBoost fits, one Platt fit and one final XGBoost fit' not in auth_raw:
        raise ContractError('Actual explicit authorization text required')
    for run in OUTPUT.glob('*'):
        previous = run / 'authorization.txt'
        if previous.is_file() and sha256(previous.read_bytes()) == sha256(auth_raw):
            raise ContractError('This authorization already has a run; inspect it rather than refitting')
    approved = load_approved_preparation()
    whole = approved.whole
    directory = start_stage(destination)
    for name in PREPARATION_FILES:
        exclusive_write(directory, 'preparation/' + name, (PREPARATION / name).read_bytes())
    fixed = {'authorization.txt': auth_raw,
        'authorization.json': json_bytes({'scope': VERSION, 'authorization_sha256': sha256(auth_raw),
            'recorded_at': now(), 'actual_user_authorization': True, 'preparation_status_fields_rewritten': False}),
        'preservation_baseline.json': baseline_raw, 'safe_test_receipt.json': test_receipt_path.read_bytes(),
        'contracts/preprocessing.json': json_bytes(PREPROCESSING), 'contracts/calibration.json': json_bytes(CALIBRATION),
        'package_versions.json': json_bytes(versions()), 'code_versions.json': json_bytes(code_hashes()),
        'training_membership.json': json_bytes({'original_metadata': json.loads(whole[META].to_json(orient='records', date_format='iso')),
            'orientation': ORIENTATION, 'deferred_training_csv_sha256': TRAINING_HASH})}
    for name, raw in fixed.items():
        exclusive_write(directory, name, raw)
    matrices, configs, freezes, all_oof, provenance = {}, {}, {}, [], {}
    fit_counts = dict(oof_xgboost=0, platt=0, final_xgboost=0, partition_priors=0)
    for f in approved.folds['folds']:
        name, end = f['name'], f['train_end_exclusive']
        train = approved.partition(end=end)
        prior = fit_priors(approved, train, name=name, expected_ids=f['train_fight_ids'], end=end)
        fit_counts['partition_priors'] += 1
        prior_hash = exclusive_write(directory, f'priors/{name}.json', json_bytes(prior))
        prepared_train = preprocess(approved, train, prior, expected_ids=f['train_fight_ids'], end=end)
        pred = approved.partition(start=f['prediction_start_inclusive'], end=f['prediction_end_exclusive'])
        prepared_pred = preprocess(approved, pred, prior, expected_ids=f['prediction_fight_ids'], start=f['prediction_start_inclusive'], end=f['prediction_end_exclusive'])
        model, cfg = fit_xgb(approved, prepared_train, prior, expected_ids=f['train_fight_ids'], end=end)
        fit_counts['oof_xgboost'] += 1
        learner_hash, frozen = save_learner(directory, name, model, cfg)
        configs[name], freezes[name] = cfg, frozen
        approved.guard(prepared_pred, expected_ids=f['prediction_fight_ids'], start=f['prediction_start_inclusive'], end=f['prediction_end_exclusive'])
        raw = probabilities(model.predict_proba(prepared_pred[FEATURES])[:, 1])
        oof = pred[META].copy(deep=True)
        oof['fold'], oof['raw_probability_fighter_1'] = name, raw
        oof['train_ids_sha256'], oof['prediction_ids_sha256'] = f['train_ids_sha256'], f['prediction_ids_sha256']
        oof['prior_sha256'], oof['learner_sha256'] = prior_hash, learner_hash
        all_oof.append(oof)
        matrices[name + '_train'], matrices[name + '_oof'] = sha256(csv_bytes(prepared_train)), sha256(csv_bytes(prepared_pred))
        provenance[name] = dict(prior_sha256=prior_hash, learner_sha256=learner_hash,
                                train_ids_sha256=f['train_ids_sha256'], prediction_ids_sha256=f['prediction_ids_sha256'])
        print(json.dumps({'component': name, 'train_rows': len(train), 'oof_rows': len(pred), 'frozen_at': frozen}), flush=True)
    oof = pd.concat(all_oof, ignore_index=True)
    validate_oof(whole, oof, approved.folds, approved.exclusions)
    oof_hash = exclusive_write(directory, 'oof.csv', csv_bytes(oof))
    exclusive_write(directory, 'oof_provenance.json', json_bytes({'orientation': ORIENTATION, 'folds': provenance, 'oof_csv_sha256': oof_hash}))
    calibration_input = dict(contract_version=VERSION, ordered_fight_ids=list(oof.fight_id), folds=list(oof.fold),
        labels=[int(v) for v in oof.label], raw_probabilities=probabilities(oof.raw_probability_fighter_1).tolist(),
        clipped_raw_log_odds=log_odds(oof.raw_probability_fighter_1).ravel().tolist(),
        orientation=ORIENTATION, calibration_contract_sha256=digest(CALIBRATION))
    input_hash = exclusive_write(directory, 'calibration_input.json', json_bytes(calibration_input))
    approved.guard(whole, expected_ids=list(whole.fight_id))
    calibrator, calibration_record = fit_platt(whole, oof, approved.folds, approved.exclusions, input_hash)
    fit_counts['platt'] += 1
    stream = io.BytesIO()
    joblib.dump(calibrator, stream)
    exclusive_write(directory, 'calibrator.joblib', stream.getvalue())
    freezes['calibrator'] = now()
    exclusive_write(directory, 'calibrator_parameters.json', json_bytes(calibration_record))
    diagnostics = {'limitations': LIMITATIONS, 'rows': 341,
        'raw_oof_diagnostics': metrics(oof.label, oof.raw_probability_fighter_1),
        'calibration-fit diagnostics': metrics(oof.label, calibrate(calibrator, oof.raw_probability_fighter_1)),
        'diagnostics_used_for_selection': False, 'prospective_performance': False, 'certified_historical_replay': False,
        'log_loss_clip_epsilon': 1e-8, 'brier_uses_unmodified_probabilities': True}
    exclusive_write(directory, 'diagnostics.json', json_bytes(diagnostics))
    # A fresh prior computation and learner on every eligible current row.
    final_prior = fit_priors(approved, whole, name='final', expected_ids=list(whole.fight_id), end=CUTOFF)
    fit_counts['partition_priors'] += 1
    exclusive_write(directory, 'priors/final.json', json_bytes(final_prior))
    final_prepared = preprocess(approved, whole, final_prior, expected_ids=list(whole.fight_id))
    final_model, final_cfg = fit_xgb(approved, final_prepared, final_prior, expected_ids=list(whole.fight_id), end=CUTOFF)
    fit_counts['final_xgboost'] += 1
    _, freezes['final'] = save_learner(directory, 'final', final_model, final_cfg)
    matrices['final'] = sha256(csv_bytes(final_prepared))
    approved.guard(final_prepared, expected_ids=list(whole.fight_id))
    raw = probabilities(final_model.predict_proba(final_prepared[FEATURES])[:, 1])
    cal = calibrate(calibrator, raw)
    final_hashes = dict(prepared_matrix_sha256=matrices['final'], raw_probability_sha256=array_hash(raw), calibrated_probability_sha256=array_hash(cal))
    exclusive_write(directory, 'metadata.json', json_bytes({'contract_version': VERSION, 'orientation': ORIENTATION,
        'limitations': LIMITATIONS, 'training_reference_source_sha256': approved.manifest['source_sha256'],
        'component_freeze_times': freezes, 'components_frozen_at': now(),
        'future_inference_source': 'must be independently supplied and bound to actual input; never default to training reference'}))
    receipt = {'contract_version': VERSION, 'authorization_sha256': sha256(auth_raw), 'command': command,
        'execution_started_at': started, 'fitting_and_component_freezing_completed_at': now(),
        'initial_verification': approved.initial_checks,
        'fit_counts': fit_counts,
        'parameters': PARAMETERS, 'fit_arguments': ['X', 'y'], 'component_freeze_times': freezes,
        'fold_fitting_times': {n: {k: cfg[k] for k in ('fit_started_at', 'fit_completed_at')} for n, cfg in configs.items()},
        'final_fitting_times': {k: final_cfg[k] for k in ('fit_started_at', 'fit_completed_at')},
        'prepared_matrix_sha256': matrices, 'in_memory_final_verification': final_hashes,
        'pre_registration_bytes_preserved': True, 'synthetic_test_fits_separate': True,
        'baseline_sha256': sha256(baseline_raw), 'forecast_calls': 0, 'holdout_scoring_calls': 0,
        'trial_outcome_ingestion': 0, 'source_network_calls': 0, 'warehouse_connections': 0,
        'production_writes': 0, 'search_fits': 0}
    exclusive_write(directory, 'run_receipt.json', json_bytes(receipt))
    components = {n: sha256((directory / n).read_bytes()) for n in sorted(COMPONENTS)}
    manifest = {'contract_version': VERSION, 'artifact_readiness': 'READY', 'parameters': PARAMETERS,
        'limitations': LIMITATIONS, 'event_cutoff_exclusive': CUTOFF,
        'preparation_checksums_sha256': PREPARATION_ROOT, 'preparation_manifest_sha256': PREPARATION_MANIFEST,
        'source_sha256': approved.manifest['source_sha256'], 'component_sha256': components}
    manifest_hash = exclusive_write(directory, 'training_manifest.json', json_bytes(manifest))
    stage_hash = exclusive_write(directory, 'staging_checksums.json', json_bytes({'files': {**components, 'training_manifest.json': manifest_hash}}))
    with loading_guards():
        saved = _load(directory, expected_checksums_sha256=stage_hash, expected_manifest_sha256=manifest_hash, staged=True, allow_incomplete=True)
        verified = independent_replay(saved, approved, in_memory=(final_prepared, raw, cal))
    exclusive_write(directory, 'verification.json', json_bytes(verified))
    exclusive_write(directory, 'preservation_verification.json', json_bytes(preservation(baseline)))
    inventory = {str(p.relative_to(directory)): sha256(p.read_bytes()) for p in directory.rglob('*') if p.is_file() and p.name != 'INCOMPLETE'}
    root_hash = exclusive_write(directory, 'checksums.json', json_bytes({'files': inventory}))
    with loading_guards():
        _load(directory, expected_checksums_sha256=root_hash, expected_manifest_sha256=manifest_hash, allow_incomplete=True)
    # Completion is the final mutation. Every component has already passed.
    (directory / 'INCOMPLETE').unlink()
    return {'status': 'READY', 'path': str(directory), 'checksums_sha256': root_hash,
        'training_manifest_sha256': manifest_hash, 'completed_at': now(), 'fit_counts': receipt['fit_counts'],
        'independent_verification': verified, 'diagnostics': diagnostics,
        'preservation': preservation(baseline)}

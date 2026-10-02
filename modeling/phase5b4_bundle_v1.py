"""Checksum-pinned challenger loading and offline, provenance-bound inference.

Loading uses only local bundle bytes. It never rebuilds history or learns a
component. A future caller must supply actual input provenance separately from
the bundle's training reference; schema names alone establish no compatibility.
"""
from dataclasses import dataclass
import io
import json
from pathlib import Path
import re

import joblib
import numpy as np
import pandas as pd
from xgboost import XGBClassifier

from modeling.phase5b4_contract_v1 import (
    ALGORITHM, CALIBRATION, CONFIG_HASH, CUTOFF, FEATURES, FOLDS_HASH, LIMITATIONS,
    META, ORIENTATION, PARAMETERS, PREPARATION_MANIFEST, PREPARATION_ROOT,
    PREPROCESSING, SAFE_TESTS, TRAINING_HASH, VERSION, WINDOWS, ContractError, calibrate, check_calibrator,
    check_learner, check_effective_configuration, check_priors, code_hashes, csv_bytes, digest,
    probabilities, sha256, transform, validate_oof, versions,
)

PREPARATION_FILES = ['checksums.json', 'training_manifest.json', 'folds.json',
                     'normalization.json', 'feature_contract.json', 'preprocessing_inputs.json',
                     'raw_source_manifest.json', 'source_capture_receipt.json',
                     'code_versions.json', 'package_versions.json', 'preregistered_configuration.json',
                     'role_aware_configuration.json', 'unresolved_identity_ledger.json']
COMPONENTS = {'metadata.json', 'training_membership.json', 'oof.csv', 'oof_provenance.json',
              'calibration_input.json', 'calibrator_parameters.json', 'calibrator.joblib',
              'contracts/preprocessing.json', 'contracts/calibration.json',
              'package_versions.json', 'code_versions.json', 'authorization.txt',
              'authorization.json', 'run_receipt.json', 'diagnostics.json',
              'preservation_baseline.json', 'safe_test_receipt.json',
              'models/final.json', 'models/final_config.json', 'priors/final.json'}
COMPONENTS |= {'preparation/' + n for n in PREPARATION_FILES}
for name, *_ in WINDOWS:
    COMPONENTS |= {f'models/{name}.json', f'models/{name}_config.json', f'priors/{name}.json'}
COMPLETION = {'staging_checksums.json', 'verification.json', 'preservation_verification.json'}


def read_json(directory, name):
    return json.loads((directory / name).read_bytes())


def valid_hash(value):
    return isinstance(value, str) and bool(re.fullmatch('[0-9a-f]{64}', value))


def verify_inventory(directory, *, expected_checksums_sha256, staged=False, allow_incomplete=False):
    directory = Path(directory)
    if directory.is_symlink() or not directory.is_dir():
        raise ContractError('Bundle must be a real local directory')
    if (directory / 'INCOMPLETE').exists() and not allow_incomplete:
        raise ContractError('Incomplete challenger run')
    checksum_name = 'staging_checksums.json' if staged else 'checksums.json'
    if (directory / checksum_name).is_symlink():
        raise ContractError('Checksum symlinks are forbidden')
    raw = (directory / checksum_name).read_bytes()
    if not valid_hash(expected_checksums_sha256) or sha256(raw) != expected_checksums_sha256:
        raise ContractError('External checksum pin mismatch')
    checks = json.loads(raw)
    if set(checks) != {'files'} or not isinstance(checks['files'], dict):
        raise ContractError('Invalid checksum manifest')
    files = checks['files']
    expected = COMPONENTS | {'training_manifest.json'} | (set() if staged else COMPLETION)
    if set(files) != expected:
        raise ContractError('Complete component inventory mismatch')
    paths = list(directory.rglob('*'))
    if any(p.is_symlink() for p in paths):
        raise ContractError('Component symlinks are forbidden')
    actual = {str(p.relative_to(directory)) for p in paths if p.is_file()}
    if actual - {checksum_name, 'INCOMPLETE'} != expected:
        raise ContractError('Unexpected/missing bundle files')
    for name, h in files.items():
        p = Path(name)
        if p.is_absolute() or '..' in p.parts or str(p) != name or not valid_hash(h):
            raise ContractError('Invalid component path/hash')
        target = directory / p
        if not target.resolve().is_relative_to(directory.resolve()) or sha256(target.read_bytes()) != h:
            raise ContractError('Component checksum mismatch: ' + name)
    return files


def validate_membership(records, folds, exclusions):
    frame = pd.DataFrame(records)
    if set(frame.columns) != set(META) or len(frame.columns) != len(META):
        raise ContractError('Original membership metadata contract mismatch')
    frame = frame[META]
    frame.event_date = pd.to_datetime(frame.event_date)
    if len(frame) != 8558 or frame.fight_id.duplicated().any() or set(frame.fight_id) & set(exclusions):
        raise ContractError('Final membership/exclusions mismatch')
    if not frame.label.isin([0, 1]).all() or frame.label.value_counts().to_dict() != {1: 5493, 0: 3065}:
        raise ContractError('Original labels/orientation mismatch')
    if not frame.feature_version.eq(2).all() or frame.event_date.max().date().isoformat() != '2026-08-29':
        raise ContractError('Final version/training endpoint mismatch')
    if (frame.event_date >= pd.Timestamp(CUTOFF)).any() or frame.event_date.isna().any():
        raise ContractError('Final chronology mismatch')
    if not frame.equals(frame.sort_values(['event_date', 'fight_id']).reset_index(drop=True)):
        raise ContractError('Final membership order mismatch')
    if digest(sorted(frame.fight_id)) != folds['final_training_ids_sha256'] or folds['final_training_rows'] != 8558:
        raise ContractError('Final membership provenance mismatch')
    if folds['contract_version'] != 'phase5_role_aware_preparation_v2' or not folds['ready'] or folds['diagnostic_only']:
        raise ContractError('Incompatible fold readiness contract')
    if folds['boundaries'] != [WINDOWS[0][1], *[w[2] for w in WINDOWS]] or len(folds['folds']) != 4:
        raise ContractError('Fixed fold boundary mismatch')
    coverage = []
    for f, (name, start, end, train_count, pred_count) in zip(folds['folds'], WINDOWS):
        train = frame.loc[frame.event_date < pd.Timestamp(start)]
        pred = frame.loc[(frame.event_date >= pd.Timestamp(start)) & (frame.event_date < pd.Timestamp(end))]
        expected = dict(name=name, train_end_exclusive=start, prediction_start_inclusive=start,
                        prediction_end_exclusive=end, train_rows=train_count, prediction_rows=pred_count,
                        train_fight_ids=sorted(train.fight_id), prediction_fight_ids=sorted(pred.fight_id),
                        train_ids_sha256=digest(sorted(train.fight_id)), prediction_ids_sha256=digest(sorted(pred.fight_id)),
                        preprocessing_fit_partition='train_only', boosting_rounds=310,
                        early_stopping=False, tuning=False, ready=True)
        if f != expected or len(train) != train_count or len(pred) != pred_count:
            raise ContractError('Exact fold populations/chronology mismatch')
        if set(train.event_id) & set(pred.event_id) or set(train.event_date) & set(pred.event_date):
            raise ContractError('OOF event/date split')
        coverage.extend(pred.fight_id)
    if len(coverage) != 341 or len(set(coverage)) != 341 or digest(sorted(coverage)) != folds['oof_ids_sha256']:
        raise ContractError('OOF completeness mismatch')
    return frame


def validate_provenance(directory, files, *, expected_manifest_sha256):
    """Complete non-estimator checks happen before either deserialize operation."""
    raw = (directory / 'training_manifest.json').read_bytes()
    if not valid_hash(expected_manifest_sha256) or sha256(raw) != expected_manifest_sha256:
        raise ContractError('External training manifest pin mismatch')
    manifest = json.loads(raw)
    if manifest.get('contract_version') != VERSION or manifest.get('artifact_readiness') != 'READY':
        raise ContractError('Incompatible challenger manifest')
    if manifest.get('component_sha256') != {n: files[n] for n in sorted(COMPONENTS)}:
        raise ContractError('Manifest component provenance mismatch')
    if manifest.get('limitations') != LIMITATIONS or manifest.get('parameters') != PARAMETERS:
        raise ContractError('Manifest limitations/recipe mismatch')
    if manifest.get('preparation_checksums_sha256') != PREPARATION_ROOT or manifest.get('preparation_manifest_sha256') != PREPARATION_MANIFEST:
        raise ContractError('Accepted preparation pins mismatch')
    if files['preparation/checksums.json'] != PREPARATION_ROOT or files['preparation/training_manifest.json'] != PREPARATION_MANIFEST:
        raise ContractError('Accepted preparation bytes mismatch')
    prep_checks = read_json(directory, 'preparation/checksums.json')['files']
    for name in PREPARATION_FILES:
        if name != 'checksums.json' and files['preparation/' + name] != prep_checks[name]:
            raise ContractError('Accepted preparation component mismatch')
    if files['preparation/preregistered_configuration.json'] != CONFIG_HASH or files['preparation/folds.json'] != FOLDS_HASH:
        raise ContractError('Preregistration/fold pins mismatch')
    if read_json(directory, 'contracts/preprocessing.json') != PREPROCESSING or read_json(directory, 'contracts/calibration.json') != CALIBRATION:
        raise ContractError('Preprocessing/calibration contract mismatch')
    if read_json(directory, 'package_versions.json') != versions() or read_json(directory, 'code_versions.json') != code_hashes():
        raise ContractError('Package/code incompatibility')
    tests = read_json(directory, 'safe_test_receipt.json')
    if tests.get('status') != 'PASSED' or tests.get('exit_code') != 0 or tests.get('safe_test_list') != SAFE_TESTS or tests.get('code_sha256') != code_hashes():
        raise ContractError('Current scoped test receipt required')
    prep = read_json(directory, 'preparation/training_manifest.json')
    if manifest.get('source_sha256') != prep['source_sha256'] or manifest.get('event_cutoff_exclusive') != CUTOFF:
        raise ContractError('Training reference source/cutoff mismatch')
    normalization = read_json(directory, 'preparation/normalization.json')
    exclusions = normalization['expanded_fitting_exclusion_ids']
    if len(exclusions) != 166 or len(set(exclusions)) != 166:
        raise ContractError('All 166 exclusions required')
    if set(exclusions) & set(normalization['aliases']):
        # No excluded canonical/alias can disappear from the expanded set.
        if any(a in exclusions and b not in exclusions for a, b in normalization['aliases'].items()):
            raise ContractError('Excluded alias not propagated')
    folds = read_json(directory, 'preparation/folds.json')
    membership = read_json(directory, 'training_membership.json')
    reference = validate_membership(membership['original_metadata'], folds, exclusions)
    if membership.get('orientation') != ORIENTATION or membership.get('deferred_training_csv_sha256') != prep_checks['training.csv']:
        raise ContractError('Original value/orientation provenance mismatch')
    preinputs = read_json(directory, 'preparation/preprocessing_inputs.json')
    priors = {}
    for name in ['final', *[w[0] for w in WINDOWS]]:
        record = read_json(directory, f'priors/{name}.json')
        check_priors(record['priors'])
        end = CUTOFF if name == 'final' else next(w[1] for w in WINDOWS if w[0] == name)
        selected = reference.loc[reference.event_date < pd.Timestamp(end)]
        pkey = 'final_training' if name == 'final' else name + '_train_preprocessing'
        expected = dict(contract_version=VERSION, name=name, fit_partition='training_rows_only',
                        training_rows=len(selected), ordered_training_ids=list(selected.fight_id),
                        training_ids_sha256=digest(sorted(selected.fight_id)), training_end_exclusive=end,
                        actual_training_endpoint=selected.event_date.max().date().isoformat(),
                        input_matrix_with_identity_sha256=preinputs[pkey]['matrix_with_identity_sha256'],
                        preparation_checksums_sha256=PREPARATION_ROOT, preprocessing_contract_sha256=digest(PREPROCESSING))
        if any(record.get(k) != v for k, v in expected.items()):
            raise ContractError('Prior membership/boundary provenance mismatch: ' + name)
        priors[name] = record
        cfg = read_json(directory, f'models/{name}_config.json')
        if cfg['parameters'] != PARAMETERS or cfg['rounds'] != 310 or cfg['fit_arguments'] != ['X', 'y'] or cfg['orientation'] != ORIENTATION:
            raise ContractError('Saved learner fit contract mismatch')
        check_effective_configuration(cfg)
    oof = pd.read_csv(directory / 'oof.csv', float_precision='round_trip')
    oof.event_date = pd.to_datetime(oof.event_date)
    validate_oof(reference, oof, folds, exclusions)
    provenance = read_json(directory, 'oof_provenance.json')
    if provenance['orientation'] != ORIENTATION or provenance['oof_csv_sha256'] != files['oof.csv']:
        raise ContractError('OOF value provenance mismatch')
    for f in folds['folds']:
        n = f['name']
        selected = oof.loc[oof.fold.eq(n)]
        if not selected.prior_sha256.eq(files[f'priors/{n}.json']).all() or not selected.learner_sha256.eq(files[f'models/{n}.json']).all():
            raise ContractError('OOF saved learner/prior lineage mismatch')
        if provenance['folds'][n] != {'prior_sha256': files[f'priors/{n}.json'], 'learner_sha256': files[f'models/{n}.json'],
                                     'train_ids_sha256': f['train_ids_sha256'], 'prediction_ids_sha256': f['prediction_ids_sha256']}:
            raise ContractError('OOF fold provenance mismatch')
    from modeling.phase5b4_contract_v1 import log_odds
    expected_input = dict(contract_version=VERSION, ordered_fight_ids=list(oof.fight_id), folds=list(oof.fold),
                          labels=[int(v) for v in oof.label], raw_probabilities=probabilities(oof.raw_probability_fighter_1).tolist(),
                          clipped_raw_log_odds=log_odds(oof.raw_probability_fighter_1).ravel().tolist(),
                          orientation=ORIENTATION, calibration_contract_sha256=digest(CALIBRATION))
    if read_json(directory, 'calibration_input.json') != expected_input:
        raise ContractError('Exact calibration input mismatch')
    calibration = read_json(directory, 'calibrator_parameters.json')
    if calibration['input_sha256'] != files['calibration_input.json'] or not calibration['converged'] or calibration['convergence_warnings'] or calibration['contract'] != CALIBRATION:
        raise ContractError('Calibration input/convergence receipt mismatch')
    if calibration['classes'] != [0, 1] or np.asarray(calibration['coefficients']).shape != (1, 1) or np.asarray(calibration['intercept']).shape != (1,):
        raise ContractError('Saved calibration parameter dimensions/orientation mismatch')
    if not np.isfinite(calibration['coefficients']).all() or not np.isfinite(calibration['intercept']).all() or len(calibration['n_iter']) != 1 or not 0 < calibration['n_iter'][0] < 1000:
        raise ContractError('Saved calibration parameters/convergence invalid')
    auth = read_json(directory, 'authorization.json')
    if auth['authorization_sha256'] != files['authorization.txt'] or auth['scope'] != VERSION:
        raise ContractError('Actual authorization receipt mismatch')
    receipt = read_json(directory, 'run_receipt.json')
    if receipt['authorization_sha256'] != auth['authorization_sha256'] or receipt['fit_counts'] != {'oof_xgboost': 4, 'platt': 1, 'final_xgboost': 1, 'partition_priors': 5}:
        raise ContractError('Authorized fit count mismatch')
    for key in ('forecast_calls', 'holdout_scoring_calls', 'trial_outcome_ingestion', 'source_network_calls', 'warehouse_connections', 'production_writes', 'search_fits'):
        if receipt[key] != 0:
            raise ContractError('Out-of-scope execution receipt')
    if receipt['parameters'] != PARAMETERS or receipt['fit_arguments'] != ['X', 'y']:
        raise ContractError('Real fitting recipe receipt mismatch')
    metadata = read_json(directory, 'metadata.json')
    if metadata['limitations'] != LIMITATIONS or metadata['training_reference_source_sha256'] != prep['source_sha256'] or metadata['orientation'] != ORIENTATION:
        raise ContractError('Metadata provenance mismatch')
    for n in ['final', *[w[0] for w in WINDOWS], 'calibrator']:
        stamp = metadata['component_freeze_times'][n]
        from datetime import datetime
        if datetime.fromisoformat(stamp).tzinfo is None:
            raise ContractError('Component freeze time requires timezone')
    return manifest, folds, reference, priors, oof, calibration


@dataclass
class ChallengerBundle:
    directory: Path
    manifest: dict
    folds: dict
    reference: pd.DataFrame
    priors: dict
    models: dict
    calibrator: object
    oof: pd.DataFrame

    def predict(self, frame, *, source_provenance):
        """No source acquisition or fitting. Caller attests actual feature lineage.

        This API does not certify prospective eligibility; a later authorized
        orchestration must supply metadata and common-capture timing evidence.
        """
        required = {'algorithm', 'feature_version', 'orientation', 'modeling_policy',
                    'preprocessing_contract_sha256', 'feature_code_sha256', 'source_sha256',
                    'capture_completed_at', 'input_matrix_with_identity_sha256', 'purpose'}
        if set(source_provenance) != required:
            raise ContractError('Complete actual inference-source provenance required')
        p = source_provenance
        from datetime import datetime
        if p['algorithm'] != ALGORITHM or p['feature_version'] != 2 or p['orientation'] != ORIENTATION or p['modeling_policy'] != 'phase5_role_aware_preparation_v2':
            raise ContractError('Input semantics incompatible')
        prep_code = read_json(self.directory, 'preparation/code_versions.json')
        feature_code = {k: v for k, v in prep_code.items() if k.startswith('features/') or k == 'modeling/phase5_role_aware.py'}
        if p['preprocessing_contract_sha256'] != digest(PREPROCESSING) or p['feature_code_sha256'] != feature_code:
            raise ContractError('Input implementation/preprocessing incompatible')
        if not isinstance(p['source_sha256'], dict) or set(p['source_sha256']) != set(self.manifest['source_sha256']) or not all(valid_hash(v) for v in p['source_sha256'].values()):
            raise ContractError('Actual inference source hashes required')
        if datetime.fromisoformat(p['capture_completed_at']).tzinfo is None or p['input_matrix_with_identity_sha256'] != sha256(csv_bytes(frame)):
            raise ContractError('Actual capture/input binding mismatch')
        if p['purpose'] not in {'persistence_verification', 'separately_authorized_inference'}:
            raise ContractError('Explicit caller purpose required')
        if list(frame.columns) != META + FEATURES or not frame.scheduled_rounds.isna().all() or not frame['feature_version'].eq(2).all():
            raise ContractError('Exact feature semantics/schema required')
        if frame.empty or frame.fight_id.isna().any() or frame.fight_id.duplicated().any() or frame.fighter_1_id.eq(frame.fighter_2_id).any() or np.isinf(frame[FEATURES].to_numpy(dtype=float)).any() or not frame[['debut_prior_win_prob_f1', 'debut_height_adv', 'debut_reach_adv']].isna().all().all():
            raise ContractError('Invalid inference identities/features')
        if p['purpose'] == 'persistence_verification':
            if p['source_sha256'] != self.manifest['source_sha256']:
                raise ContractError('Verification must use accepted training source')
            original = self.reference.set_index('fight_id').reindex(frame.fight_id).reset_index()
            if not original[META].equals(frame[META].reset_index(drop=True)):
                raise ContractError('Verification outside approved training rows')
            if sha256(csv_bytes(frame)) != TRAINING_HASH:
                raise ContractError('Verification requires exact original training features')
        elif p['source_sha256'] == self.manifest['source_sha256']:
            raise ContractError('New inference cannot masquerade as the training capture')
        elif not frame.label.isna().all() or datetime.fromisoformat(p['capture_completed_at']) <= max(datetime.fromisoformat(s) for s in read_json(self.directory, 'metadata.json')['component_freeze_times'].values()):
            raise ContractError('Inference requires outcome-free rows and a later actual capture')
        prepared = transform(frame.copy(deep=True), self.priors['final'])
        raw = probabilities(self.models['final'].predict_proba(prepared[FEATURES])[:, 1])
        calibrated = calibrate(self.calibrator, raw)
        return prepared, raw, calibrated


def _load(directory, *, expected_checksums_sha256, expected_manifest_sha256, staged=False, allow_incomplete=False):
    directory = Path(directory)
    files = verify_inventory(directory, expected_checksums_sha256=expected_checksums_sha256, staged=staged, allow_incomplete=allow_incomplete)
    manifest, folds, reference, priors, oof, calibration = validate_provenance(directory, files, expected_manifest_sha256=expected_manifest_sha256)
    if not staged:
        verified = read_json(directory, 'verification.json')
        if verified['status'] != 'VERIFIED' or verified['oof_replay_rows'] != 341 or verified['final_verification_rows'] != 8558 or verified['exact_parity'] is not True:
            raise ContractError('Independent verification receipt required')
        if read_json(directory, 'preservation_verification.json')['status'] != 'VERIFIED':
            raise ContractError('Preservation receipt required')
    # Every non-estimator check above must finish before these calls.
    models = {}
    for name in ['final', *[w[0] for w in WINDOWS]]:
        model = XGBClassifier(**PARAMETERS)
        model.load_model(directory / f'models/{name}.json')
        check_learner(model)
        models[name] = model
    calibrator = joblib.load(io.BytesIO((directory / 'calibrator.joblib').read_bytes()))
    check_calibrator(calibrator, calibration)
    return ChallengerBundle(directory, manifest, folds, reference, priors, models, calibrator, oof)


def load_challenger_bundle(directory, *, expected_checksums_sha256, expected_manifest_sha256):
    return _load(directory, expected_checksums_sha256=expected_checksums_sha256,
                 expected_manifest_sha256=expected_manifest_sha256)

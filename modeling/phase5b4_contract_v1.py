"""Fixed current-population contracts, independent of the March lifecycle.

This module has no warehouse imports. Only the unchanged, pure debut helpers
are reused; all population, calibration and persistence guards are versioned.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
from uuid import UUID

import numpy as np
import pandas as pd

from features.debut_prior import apply_debut_features
from modeling.data import FEATURE_COLS_V2

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'data/experiments/phase5b4_current_challenger'
PREPARATION = ROOT / 'data/experiments/phase5b3_role_aware_preparation/20261002_phase5b3_role_aware_v2_validated'
PREPARATION_ROOT = '7d36dc2bdb2a99eb81a44424fe603a6beab74d05435e41c8a1489cea11f0bd4f'
PREPARATION_MANIFEST = '00f445f4fc93af986db5dbda5631c89181ccaa85271f643660dade66b4c9aa4b'
CONFIG_HASH = '6d63abe99fa6a401df2e779c68b1e3ab8aa81fd3a3d2253058c812a644732438'
TRAINING_HASH = '3a48ffa517e9cb4475212f507b91039c7e8d6fb08b68fee1bf90d010e4d5c33c'
FOLDS_HASH = '2710b451f8391ccc08b387d5b66659c0f0d6f56f2d90c343f23b321d4fdd16d3'
VERSION = 'phase5b4_current_challenger_v1'
ALGORITHM = 'v2_date_frozen_elo_schedule_unknown_v1'
CUTOFF = '2026-10-02'
DEBUT = ['debut_prior_win_prob_f1', 'debut_height_adv', 'debut_reach_adv']
FEATURES = FEATURE_COLS_V2 + DEBUT
META = ['fight_id', 'event_id', 'fighter_1_id', 'fighter_2_id', 'event_date',
        'weight_class', 'label', 'feature_version']
ORIENTATION = 'p_and_label_1_mean_fighter_1_wins'
PARAMETERS = dict(objective='binary:logistic', eval_metric='logloss', max_depth=6,
                  min_child_weight=100, reg_lambda=1.0, n_estimators=310,
                  learning_rate=0.02, subsample=0.8, colsample_bytree=0.8,
                  random_state=42, n_jobs=1, tree_method='hist', verbosity=0)
PLATT = dict(C=1e10, solver='lbfgs', max_iter=1000)
EPSILON = 1e-8
WINDOWS = [('oof_1', '2025-10-02', '2026-01-02', 8217, 128),
           ('oof_2', '2026-01-02', '2026-04-02', 8345, 114),
           ('oof_3', '2026-04-02', '2026-07-02', 8459, 69),
           ('oof_4', '2026-07-02', CUTOFF, 8528, 30)]
PREPROCESSING = dict(contract_version=VERSION, algorithm=ALGORITHM,
                     ordered_features=FEATURES, feature_version=2,
                     scheduled_rounds='unknown_for_every_target_and_history',
                     debut_columns=DEBUT, base_prior=0.5, bucket_minimum=20,
                     std_ddof=1, normalize='physical_difference / training_std; no mean subtraction',
                     missing_values='preserved; no imputation', partition_priors='training_rows_only',
                     implementation='features/debut_prior.py', orientation=ORIENTATION)
CALIBRATION = dict(contract_version=VERSION, method='platt_log_odds', clip_epsilon=EPSILON,
                   input='log(p / (1-p)) after explicit clipping', positive_class=1,
                   classes=[0, 1], orientation=ORIENTATION, **PLATT)
LIMITATIONS = dict(april_relationship='UNRESOLVED', latest_binary_training_event='2026-08-29',
                  statistics_last_refreshed='2026-08-09', source_coverage_through='2026-08-29',
                  certification='retrospective_reconstruction_only_not_complete_history',
                  mutable_contemporary_profiles=True, historical_availability_unverified=True,
                  incomplete_histories=True, historical_comparison='STILL_BLOCKED',
                  prospective_forecast_readiness='BLOCKED', prospective_superiority=False,
                  production_approval=False, automatic_promotion=False)
CODE = ['modeling/phase5b4_contract_v1.py', 'modeling/phase5b4_bundle_v1.py',
        'modeling/phase5b4_trainer_v1.py', 'modeling/phase5b4_safety.py',
        'tools/train_phase5b4_current_challenger.py', 'modeling/tests/test_phase5b4_current_challenger.py',
        'features/debut_prior.py', 'modeling/data.py']
SAFE_TESTS = ['modeling/tests/test_phase5b4_current_challenger.py', 'features/tests/test_debut_prior.py']


class ContractError(ValueError):
    """Refuse incompatible data/components before fitting or deserialization."""


def now():
    return datetime.now(timezone.utc).isoformat()


def json_bytes(value):
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def digest(value):
    return sha256(json_bytes(value))


def csv_bytes(frame):
    return frame.to_csv(index=False, date_format='%Y-%m-%d', lineterminator='\n', float_format='%.17g').encode()


def versions():
    return {'python': platform.python_version(), **{name: importlib.metadata.version(name) for name in
            ('numpy', 'pandas', 'scikit-learn', 'xgboost', 'psycopg2-binary', 'joblib', 'pytest')}}


def code_hashes():
    return {name: sha256((ROOT / name).read_bytes()) for name in sorted(CODE)}


def membership(frame):
    return digest(sorted(frame.fight_id))


def validate_frame(frame, *, deferred=False):
    if list(frame.columns) != META + FEATURES or len(FEATURES) != 50 or frame.empty:
        raise ContractError('Exact ordered metadata/50-column feature schema required')
    for key in META[:4]:
        try:
            for value in frame[key]:
                if str(UUID(value)) != value:
                    raise ValueError(value)
        except (TypeError, ValueError, AttributeError) as exc:
            raise ContractError('Invalid original identity: ' + key) from exc
    if frame.fight_id.duplicated().any() or frame.fighter_1_id.eq(frame.fighter_2_id).any():
        raise ContractError('Duplicate identity/self matchup')
    if not frame.label.isin([0, 1]).all() or not frame.feature_version.eq(2).all():
        raise ContractError('Invalid original binary labels/version')
    dates = frame.event_date
    if not pd.api.types.is_datetime64_ns_dtype(dates.dtype) and not pd.api.types.is_datetime64_dtype(dates.dtype):
        raise ContractError('Typed, timezone-naive event dates required')
    if dates.isna().any() or (dates >= pd.Timestamp(CUTOFF)).any() or not dates.eq(dates.dt.normalize()).all():
        raise ContractError('Invalid chronology/exclusive cutoff')
    if frame.groupby('event_id').event_date.nunique().gt(1).any():
        raise ContractError('Event identity spans multiple dates')
    if not frame.weight_class.dropna().map(lambda v: isinstance(v, str) and bool(v.strip())).all():
        raise ContractError('Invalid weight class')
    values = frame[FEATURES].to_numpy(dtype=float)
    if np.isinf(values).any() or not frame.scheduled_rounds.isna().all():
        raise ContractError('Invalid features/unknown schedules')
    if deferred and not frame[DEBUT].isna().all().all():
        raise ContractError('Accepted debut columns must remain deferred')


def guard_partition(whole, frame, *, expected_ids, exclusions, start=None, end=CUTOFF):
    """Exact ordered identity/value checks before each transform and fit."""
    validate_frame(frame)
    if len(expected_ids) != len(set(expected_ids)) or set(frame.fight_id) & set(exclusions):
        raise ContractError('Duplicate approved membership or excluded ID/alias')
    mask = whole.event_date < pd.Timestamp(end)
    if start is not None:
        mask &= whole.event_date >= pd.Timestamp(start)
    original = whole.loc[mask].reset_index(drop=True)
    if sorted(original.fight_id) != sorted(expected_ids) or list(frame.fight_id) != list(original.fight_id):
        raise ContractError('Exact approved membership/ordering/complete date required')
    columns = [c for c in whole.columns if c not in DEBUT]
    if not frame[columns].reset_index(drop=True).equals(original[columns]):
        raise ContractError('Original labels/orientation/non-debut features changed')
    outside = whole.loc[~mask]
    if set(frame.event_id) & set(outside.event_id) or set(frame.event_date) & set(outside.event_date):
        raise ContractError('Partition splits an event/date')


def check_priors(priors):
    keys = {'base_prior', 'height_stats', 'reach_stats', 'global_height_std',
            'global_reach_std', 'training_debut_win_rate'}
    if not isinstance(priors, dict) or set(priors) != keys or priors['base_prior'] != 0.5:
        raise ContractError('Invalid saved debut-prior contract')
    for key in ('global_height_std', 'global_reach_std'):
        if not np.isfinite(priors[key]) or priors[key] <= 0:
            raise ContractError('Invalid normalization denominator')
    if not np.isfinite(priors['training_debut_win_rate']) or not 0 <= priors['training_debut_win_rate'] <= 1:
        raise ContractError('Invalid debut diagnostic')
    for key in ('height_stats', 'reach_stats'):
        if not isinstance(priors[key], dict):
            raise ContractError('Invalid prior buckets')
        for wc, stats in priors[key].items():
            if not isinstance(wc, str) or set(stats) != {'mean', 'std'} or not np.isfinite(list(stats.values())).all() or stats['std'] < 0:
                raise ContractError('Invalid prior bucket')


def transform(frame, prior_record):
    check_priors(prior_record['priors'])
    result = apply_debut_features(frame.copy(deep=True), deepcopy(prior_record['priors']))
    if not result[[c for c in frame.columns if c not in DEBUT]].equals(frame[[c for c in frame.columns if c not in DEBUT]]):
        raise ContractError('Preprocessing changed non-debut values')
    debut = result.both_debuting.eq(1)
    if not result.loc[debut, DEBUT[0]].eq(0.5).all() or not result.loc[~debut, DEBUT].isna().all().all():
        raise ContractError('Invalid debut semantics')
    if np.isinf(result[FEATURES].to_numpy(dtype=float)).any():
        raise ContractError('Non-finite debut normalization')
    return result


def probabilities(values):
    p = np.asarray(values, dtype=np.float64)
    if p.ndim != 1 or not len(p) or not np.isfinite(p).all() or ((p < 0) | (p > 1)).any():
        raise ContractError('Invalid probabilities')
    return p


def log_odds(values):
    p = np.clip(probabilities(values), EPSILON, 1 - EPSILON)
    return np.log(p / (1 - p)).reshape(-1, 1)


def calibrate(estimator, raw):
    if list(estimator.classes_) != [0, 1]:
        raise ContractError('Calibrator orientation must be [0, 1]')
    return probabilities(estimator.predict_proba(log_odds(raw))[:, 1])


def metrics(labels, values):
    y = np.asarray(labels, dtype=np.int64)
    p = probabilities(values)
    if y.shape != p.shape or not np.isin(y, [0, 1]).all():
        raise ContractError('Invalid diagnostic labels')
    clipped = np.clip(p, EPSILON, 1 - EPSILON)
    return {'natural_log_loss': float(np.mean(-(y * np.log(clipped) + (1-y) * np.log(1-clipped)))),
            'brier': float(np.mean((p-y)**2))}


def validate_oof(whole, oof, folds, exclusions):
    expected_columns = META + ['fold', 'raw_probability_fighter_1', 'train_ids_sha256',
                               'prediction_ids_sha256', 'prior_sha256', 'learner_sha256']
    if list(oof.columns) != expected_columns or len(oof) != 341 or oof.fight_id.duplicated().any():
        raise ContractError('Exact unique 341-row OOF contract required')
    probabilities(oof.raw_probability_fighter_1)
    if set(oof.fight_id) & set(exclusions) or set(oof.label) != {0, 1}:
        raise ContractError('Invalid calibration membership/classes')
    combined = []
    for fold in folds['folds']:
        mask = (whole.event_date >= pd.Timestamp(fold['prediction_start_inclusive'])) & (whole.event_date < pd.Timestamp(fold['prediction_end_exclusive']))
        expected = whole.loc[mask, META].reset_index(drop=True)
        selected = oof.loc[oof.fold.eq(fold['name'])].reset_index(drop=True)
        if not selected[META].equals(expected):
            raise ContractError('OOF labels/orientation/coverage/fold lineage mismatch')
        for k, fk in [('train_ids_sha256', 'train_ids_sha256'), ('prediction_ids_sha256', 'prediction_ids_sha256')]:
            if not selected[k].eq(fold[fk]).all():
                raise ContractError('OOF partition provenance mismatch')
        combined.extend(expected.fight_id)
    if list(oof.fight_id) != combined:
        raise ContractError('OOF order/coverage mismatch')


def check_learner(model):
    if list(model.classes_) != [0, 1] or model.get_booster().feature_names != FEATURES:
        raise ContractError('Learner feature order/class orientation mismatch')
    if model.get_booster().num_boosted_rounds() != 310:
        raise ContractError('Learner must contain exactly 310 boosted rounds')
    parameters = model.get_params()
    if any(parameters.get(k) != v for k, v in PARAMETERS.items()):
        raise ContractError('Learner fixed parameters mismatch')
    if any(parameters.get(k) is not None for k in ('early_stopping_rounds', 'callbacks', 'scale_pos_weight')):
        raise ContractError('Unexpected stopping/callback/reweighting configuration')
    if model.get_booster().attributes() or hasattr(model, 'evals_result_'):
        raise ContractError('Unexpected evaluation/early-stopping state')
    cfg = json.loads(model.get_booster().save_config())['learner']
    if cfg['objective']['name'] != 'binary:logistic':
        raise ContractError('Booster objective mismatch')


def check_effective_configuration(record):
    cfg = record['effective_booster_configuration']['learner']
    generic = cfg['generic_param']
    tree = cfg['gradient_booster']['tree_train_param']
    for key in ('max_depth', 'min_child_weight', 'lambda', 'learning_rate', 'subsample', 'colsample_bytree'):
        expected = PARAMETERS['reg_lambda' if key == 'lambda' else key]
        if np.float32(tree[key]) != np.float32(expected):
            raise ContractError('Actual fitted tree configuration mismatch: ' + key)
    if generic['n_jobs'] != '1' or generic['seed'] != '42' or cfg['objective']['name'] != 'binary:logistic':
        raise ContractError('Actual fitted seed/parallelism/objective mismatch')
    if cfg['gradient_booster']['gbtree_train_param']['tree_method'] != 'hist' or cfg['metrics'] != [{'name': 'logloss'}]:
        raise ContractError('Actual fitted tree method/metric mismatch')
    if cfg['learner_model_param']['num_feature'] != '50' or cfg['learner_model_param']['num_class'] != '0':
        raise ContractError('Actual fitted feature/class configuration mismatch')


def check_calibrator(model, record):
    from sklearn.linear_model import LogisticRegression
    if type(model) is not LogisticRegression or any(model.get_params().get(k) != v for k, v in PLATT.items()):
        raise ContractError('Calibrator recipe mismatch')
    if model.get_params()['class_weight'] is not None or model.get_params()['warm_start']:
        raise ContractError('Unexpected calibrator reweighting/warm start')
    if list(model.classes_) != [0, 1] or model.n_features_in_ != 1 or model.coef_.shape != (1, 1) or model.intercept_.shape != (1,):
        raise ContractError('Calibrator dimensions/orientation mismatch')
    if not np.isfinite(model.coef_).all() or not np.isfinite(model.intercept_).all() or (model.n_iter_ >= 1000).any():
        raise ContractError('Invalid/non-converged calibrator')
    actual = {'coefficients': model.coef_.tolist(), 'intercept': model.intercept_.tolist(),
              'n_iter': model.n_iter_.tolist(), 'classes': model.classes_.tolist(), 'converged': True}
    if any(record.get(k) != v for k, v in actual.items()):
        raise ContractError('Saved calibrator fitted parameters mismatch')

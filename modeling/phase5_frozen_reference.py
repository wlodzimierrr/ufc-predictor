"""Production-derived prospective reference; bootstrap freeze only, no trial scoring.

No recovered deployed probability stream is claimed. The base learner is copied
unchanged; only exact accepted legacy preprocessing and one Platt fit are allowed.
"""
from dataclasses import dataclass
from datetime import datetime
import io
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

from features.debut_prior import compute_debut_priors, apply_debut_features
from modeling.holdout import load_holdout_fight_ids
from modeling.phase5_current_data import reference_preparation, versions, now, verify_checksums
from modeling.phase5b1_artifacts import (
    PARENT, PARENT_ROOT, PARENT_MANIFEST, POINTER, BASE, METADATA,
    parent_inputs, publish, code_hashes, verify_code_and_packages,
)
from modeling.refit_preflight import ROOT, FEATURE_ORDER, DEBUT_COLS, PreflightError, json_bytes, sha256

VERSION = 'phase5_production_derived_frozen_reference_v1'
RECIPE = {'input':'log_odds', 'clip_epsilon':1e-8, 'C':1e10, 'solver':'lbfgs',
          'max_iter':1000, 'positive_class':1, 'persist_estimator':True}
MEMBERSHIPS = {'preprocessing':(6391,'2013992e43c41c7c1825b93d3d71a84b14a3ef4e68760e4093a79f7bcf69fbf3'),
               'calibration':(1009,'de0ed1872e81ab8e4a5ae77a550a94a555e178ba8e53bcf42118e2f70503d5db')}
CODE = ['tools/freeze_phase5b1_reference_and_history.py',
        'modeling/phase5_frozen_reference.py','modeling/phase5_reference_adapter.py',
        'modeling/phase5b1_artifacts.py','modeling/phase5_current_data.py',
        'modeling/refit_preflight.py','modeling/holdout.py','features/data_loader.py',
        'features/debut_prior.py','features/bout.py'] + [str(p.relative_to(ROOT)) for p in
        sorted((ROOT/'modeling/reference_legacy_v1').glob('*')) if p.is_file()]
CODE += ['docs/phase5-history-identity-policy-v1.md']


def validate_bootstrap():
    checks = parent_inputs()
    meta = json.loads((PARENT/'reference_bootstrap/production_metadata.json').read_bytes())
    if checks['reference_bootstrap/production_metadata.json'] != METADATA or meta['feature_cols'] != FEATURE_ORDER:
        raise PreflightError('Reference metadata/features changed')
    if (meta['val_date'],meta['test_date'],meta['feature_version'],meta['debut_priors_applied']) != ('2022-03-12','2024-03-09',2,True):
        raise PreflightError('Reference legacy boundaries/recipe changed')
    rebuilt = reference_preparation({'reference_bootstrap/stored_features.json':
               (PARENT/'reference_bootstrap/stored_features.json').read_bytes()},meta)
    if any((PARENT/n).read_bytes()!=b for n,b in rebuilt.items()):
        raise PreflightError('Reference exact input partitions changed')
    manifest = json.loads(rebuilt['reference_bootstrap/manifest.json'])
    if not manifest['ready'] or manifest['structural_issues']:
        raise PreflightError('Blocked reference bootstrap')
    exclusions = load_holdout_fight_ids()
    if len(exclusions)!=166 or manifest['exclusion_ids']!=sorted(exclusions):
        raise PreflightError('Reference exclusions changed')
    fights = {r['fight_id']:r for r in json.loads((PARENT/'sources/fights.json').read_bytes())}
    events = {r['event_id']:r for r in json.loads((PARENT/'sources/events.json').read_bytes())}
    frames = {}
    for partition,(count,digest) in MEMBERSHIPS.items():
        rows = json.loads((PARENT/f'reference_bootstrap/{partition}_rows.json').read_bytes())
        ids = [r['fight_id'] for r in rows]
        if len(ids)!=count or len(set(ids))!=count or sha256(json_bytes(sorted(ids)))!=digest or set(ids)&exclusions:
            raise PreflightError('Reference membership/exclusion mismatch')
        for r in rows:
            f = fights[r['fight_id']]
            if r['source_event_id']!=f['event_id'] or r['source_event_date']!=events[f['event_id']]['event_date']:
                raise PreflightError('Reference event membership mismatch')
            for k in ('fighter_1_id','fighter_2_id'):
                if r[k]!=f[k] or r[k]!=r['source_'+k]:
                    raise PreflightError('Reference orientation mismatch')
            if r['fighter_1_id']==r['fighter_2_id'] or r['label']!=int(f['winner_fighter_id']==f['fighter_1_id']) or f['result_type']!='win':
                raise PreflightError('Reference label contract mismatch')
        frame = pd.DataFrame(rows)
        for c in FEATURE_ORDER[:-3]:
            frame[c] = pd.to_numeric(frame[c],errors='raise').astype(float)
            if np.isinf(frame[c]).any(): raise PreflightError('Infinite reference feature')
        frames[partition] = frame
    if set(frames['preprocessing'].fight_id)&set(frames['calibration'].fight_id):
        raise PreflightError('Reference partitions overlap')
    return frames,manifest,checks


def log_odds(prob):
    p = np.asarray(prob,dtype=float)
    if not np.isfinite(p).all() or ((p<0)|(p>1)).any():
        raise PreflightError('Invalid reference probabilities')
    p = np.clip(p,RECIPE['clip_epsilon'],1-RECIPE['clip_epsilon'])
    return np.log(p/(1-p)).reshape(-1,1)


def dump(estimator):
    stream = io.BytesIO()
    joblib.dump(estimator,stream)
    return stream.getvalue()


def array_hash(values):
    return sha256(np.asarray(values,dtype='<f8').tobytes())


def bootstrap_results(model, calibrator, preprocessing, frames):
    """Only called with the integrity-checked bootstrap partitions, never future rows."""
    results = {}
    for partition,frame in frames.items():
        prepared = apply_debut_features(frame.copy(deep=True),preprocessing)
        matrix = prepared[FEATURE_ORDER].to_numpy(dtype=float)
        raw = model.predict_proba(matrix)[:,1]
        calibrated = calibrator.predict_proba(log_odds(raw))[:,1]
        results[partition] = {'rows':len(frame),'matrix_sha256':array_hash(matrix),
                              'raw_sha256':array_hash(raw),'calibrated_sha256':array_hash(calibrated)}
    return results


@dataclass(frozen=True)
class FrozenReference:
    base: XGBClassifier
    calibrator: LogisticRegression
    preprocessing: dict
    manifest: dict


def load_reference(directory: Path, *, expected_checksums_sha256: str,
                   expected_manifest_sha256: str):
    try:
        checks = verify_checksums(directory,expected_checksums_sha256=expected_checksums_sha256)
    except (OSError,ValueError,KeyError) as exc:
        raise PreflightError('Missing or invalid reference artifact: '+str(exc)) from exc
    required = {'manifest.json','base_model.joblib','production_metadata.json','preprocessing.json',
                'calibrator.joblib','feature_order.json','package_versions.json','code_versions.json',
                'bootstrap_parity.json','legacy_source_provenance.json'}
    if set(checks)!=required or checks['manifest.json']!=expected_manifest_sha256:
        raise PreflightError('Missing reference components or manifest mismatch')
    m = json.loads((directory/'manifest.json').read_bytes())
    if m.get('contract_version')!=VERSION or m.get('ready') is not True or m.get('platt')!=RECIPE:
        raise PreflightError('Incompatible reference contract/recipe')
    if m.get('feature_order')!=FEATURE_ORDER or json.loads((directory/'feature_order.json').read_bytes())!=FEATURE_ORDER:
        raise PreflightError('Reference feature order mismatch')
    if m.get('parent_checksums_sha256')!=PARENT_ROOT or m.get('parent_manifest_sha256')!=PARENT_MANIFEST or m.get('production_pointer_sha256')!=POINTER:
        raise PreflightError('Incompatible reference source provenance')
    inputs = parent_inputs()
    expected_inputs = {n:h for n,h in inputs.items() if n.startswith('reference_bootstrap/') or n.startswith('sources/') or n=='source_capture_receipt.json'}
    if m.get('input_hashes')!=expected_inputs or m.get('memberships')!={k:{'rows':n,'ids_sha256':h} for k,(n,h) in MEMBERSHIPS.items()}:
        raise PreflightError('Incompatible reference membership provenance')
    if checks['base_model.joblib']!=BASE or checks['production_metadata.json']!=METADATA:
        raise PreflightError('Reference base learner changed')
    verify_code_and_packages(directory,CODE)
    times = m.get('component_freeze_times',{})
    if set(times)!={'base_bytes_verified_at','adapter_pinned_at','preprocessing_frozen_at','calibrator_frozen_at','bundle_frozen_at'}:
        raise PreflightError('Missing reference freeze clocks')
    parsed = [datetime.fromisoformat(times[k]) for k in ('base_bytes_verified_at','adapter_pinned_at','preprocessing_frozen_at','calibrator_frozen_at','bundle_frozen_at')]
    capture = datetime.fromisoformat(m['source_capture_completed_at'])
    if (any(t.tzinfo is None or t<=capture for t in parsed)
            or not parsed[0]<=parsed[2]<=parsed[3]<=parsed[4] or parsed[1]>parsed[4]):
        raise PreflightError('Reference freeze clocks incompatible with source capture')
    priors = json.loads((directory/'preprocessing.json').read_bytes())
    if set(priors)!={'base_prior','height_stats','reach_stats','global_height_std','global_reach_std','training_debut_win_rate'} or priors['base_prior']!=0.5:
        raise PreflightError('Invalid saved legacy preprocessing')
    # Deserialization follows all integrity and compatibility checks, with no fits.
    base = joblib.load(io.BytesIO((directory/'base_model.joblib').read_bytes()))
    calibrator = joblib.load(io.BytesIO((directory/'calibrator.joblib').read_bytes()))
    if not isinstance(base,XGBClassifier) or base.n_features_in_!=len(FEATURE_ORDER) or list(base.classes_)!=[0,1]:
        raise PreflightError('Incompatible reference learner')
    if not isinstance(calibrator,LogisticRegression) or calibrator.n_features_in_!=1 or list(calibrator.classes_)!=[0,1] or any(calibrator.get_params()[k]!=RECIPE[k] for k in ('C','solver','max_iter')):
        raise PreflightError('Incompatible saved Platt estimator')
    if not np.isfinite(calibrator.coef_).all() or not np.isfinite(calibrator.intercept_).all():
        raise PreflightError('Invalid saved Platt coefficients')
    return FrozenReference(base,calibrator,priors,m)


def freeze_reference(destination: Path):
    if destination.exists(): raise PreflightError('Never overwrite a reference bundle')
    frames,bootstrap,inputs = validate_bootstrap()
    if sha256((ROOT/'models/production_model.json').read_bytes())!=POINTER:
        raise PreflightError('Production pointer changed')
    provenance = json.loads((PARENT/'reference_bootstrap/artifact_provenance.json').read_bytes())
    raw_base = (ROOT/provenance['base_learner_path']).read_bytes()
    if sha256(raw_base)!=BASE or sha256((ROOT/provenance['metadata_path']).read_bytes())!=METADATA:
        raise PreflightError('Production learner/metadata changed')
    clocks = {'base_bytes_verified_at':now()}
    code = code_hashes(CODE)
    clocks['adapter_pinned_at'] = now()
    priors = compute_debut_priors(frames['preprocessing'])
    priors_raw = json_bytes(priors)
    clocks['preprocessing_frozen_at'] = now()
    base = joblib.load(io.BytesIO(raw_base))
    calibration = apply_debut_features(frames['calibration'].copy(deep=True),priors)
    raw = base.predict_proba(calibration[FEATURE_ORDER].to_numpy(dtype=float))[:,1]
    estimator = LogisticRegression(C=RECIPE['C'],solver=RECIPE['solver'],max_iter=RECIPE['max_iter'])
    estimator.fit(log_odds(raw),calibration.label.to_numpy(dtype=int))
    estimator_raw = dump(estimator)
    clocks['calibrator_frozen_at'] = now()
    parity = bootstrap_results(base,estimator,priors,frames)
    manifest = {'contract_version':VERSION,'ready':True,'description':'production-derived prospective reference',
        'recovered_original_deployed_probability_stream':False,'base_retrained':False,
        'parent_checksums_sha256':PARENT_ROOT,'parent_manifest_sha256':PARENT_MANIFEST,
        'production_pointer_sha256':POINTER,'base_model_sha256':BASE,'metadata_sha256':METADATA,
        'feature_order':FEATURE_ORDER,'preprocessing_recipe':'features.debut_prior.compute_debut_priors exact pre-val population',
        'platt':RECIPE,'memberships':{k:{'rows':n,'ids_sha256':h} for k,(n,h) in MEMBERSHIPS.items()},
        'original_recorded_memberships':{'preprocessing':6376,'calibration':1007},
        'membership_difference':{'preprocessing':15,'calibration':2},
        'input_hashes':{n:h for n,h in inputs.items() if n.startswith('reference_bootstrap/') or n.startswith('sources/') or n=='source_capture_receipt.json'},
        'source_capture_completed_at':json.loads((PARENT/'training_manifest.json').read_bytes())['capture_completed_at'],
        'legacy_adapter_version':'phase5_legacy_reference_adapter_v1',
        'limitations':bootstrap['inherited_limitations']+['future adapter uses min(observation UTC date,event date); sequential all-capture Elo retains order sensitivity',
            'legacy schedules/default zeros and mutable profiles retained; future input validity/eligibility remains a separate gate'],
        'prospective_predictions':0,'predictive_evaluations':0,'new_capture_after_freeze_required':True}
    clocks['bundle_frozen_at'] = now()
    manifest['component_freeze_times'] = clocks
    payloads = {'manifest.json':json_bytes(manifest),'base_model.joblib':raw_base,
        'production_metadata.json':(PARENT/'reference_bootstrap/production_metadata.json').read_bytes(),
        'preprocessing.json':priors_raw,'calibrator.joblib':estimator_raw,
        'feature_order.json':json_bytes(FEATURE_ORDER),'package_versions.json':json_bytes(versions()),
        'code_versions.json':json_bytes(code),'bootstrap_parity.json':json_bytes(parity),
        'legacy_source_provenance.json':(ROOT/'modeling/reference_legacy_v1/provenance.json').read_bytes()}
    digest = publish(destination,payloads)
    loaded = load_reference(destination,expected_checksums_sha256=digest,expected_manifest_sha256=sha256(payloads['manifest.json']))
    if bootstrap_results(loaded.base,loaded.calibrator,loaded.preprocessing,frames)!=parity:
        raise PreflightError('Reference save/load parity failed')
    return {'checksums_sha256':digest,'manifest_sha256':sha256(payloads['manifest.json']),
            'parity':'EXACT','component_freeze_times':clocks,'memberships':manifest['memberships']}


def freeze_adapter_successor(parent: Path, destination: Path, *,
                             expected_parent_checksums_sha256: str,
                             expected_parent_manifest_sha256: str):
    """Freeze a stricter adapter without a second preprocessing/calibration fit.

The trusted old root verifies saved bytes even though today's code provenance
differs. The successor then checks new code provenance and bootstrap parity.
"""
    if destination.exists(): raise PreflightError('Never overwrite a reference bundle')
    checks = verify_checksums(parent,expected_checksums_sha256=expected_parent_checksums_sha256)
    if checks['manifest.json']!=expected_parent_manifest_sha256:
        raise PreflightError('Reference successor parent manifest mismatch')
    payloads = {n:(parent/n).read_bytes() for n in checks}
    manifest = json.loads(payloads['manifest.json'])
    if manifest['contract_version']!=VERSION or checks['base_model.joblib']!=BASE or checks['production_metadata.json']!=METADATA:
        raise PreflightError('Incompatible reference successor dependency')
    clocks = {**manifest['component_freeze_times'],'adapter_pinned_at':now()}
    manifest.update(history_identity_input_contract='phase5_normalized_history_input_v1',
        predecessor_bundle={'path':str(parent.relative_to(ROOT)),
            'checksums_sha256':expected_parent_checksums_sha256,
            'manifest_sha256':expected_parent_manifest_sha256},
        successor_change='adapter rejects unnormalized repeated history pairs before indexing; numeric recipe and saved components unchanged',
        new_preprocessing_or_calibration_fit=False)
    payloads['code_versions.json'] = json_bytes(code_hashes(CODE))
    clocks['bundle_frozen_at'] = now()
    manifest['component_freeze_times'] = clocks
    payloads['manifest.json'] = json_bytes(manifest)
    digest = publish(destination,payloads)
    loaded = load_reference(destination,expected_checksums_sha256=digest,
                            expected_manifest_sha256=sha256(payloads['manifest.json']))
    frames,_,_ = validate_bootstrap()
    if bootstrap_results(loaded.base,loaded.calibrator,loaded.preprocessing,frames)!=json.loads(payloads['bootstrap_parity.json']):
        raise PreflightError('Reference adapter successor bootstrap parity failed')
    for n in ('base_model.joblib','production_metadata.json','preprocessing.json','calibrator.joblib','bootstrap_parity.json'):
        if sha256(payloads[n])!=checks[n]:raise PreflightError('Reference successor changed saved components')
    return {'checksums_sha256':digest,'manifest_sha256':sha256(payloads['manifest.json']),
            'parity':'EXACT','saved_components_unchanged':True,'new_fits':0,'component_freeze_times':clocks}

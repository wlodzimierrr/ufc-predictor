"""Three separate saved-component pipelines and receipt-based inference v2.

The challenger successor intentionally replaces the content-change gate with a
new-observation gate. It shares the exact saved transform/learner/calibrator;
the old ChallengerBundle.predict API and all artifact bytes remain unchanged.
March inference always uses CandidateBundle.predict, never its learner directly.
"""
from copy import deepcopy
from datetime import date
import io
import json

import numpy as np
import pandas as pd
from modeling.phase5c1_contract import (IDENTITY, INFERENCE_VERSION, LEGACY, PINS, ROOT,
    digest, instant, local_bytes, require, sha, uuid, verify_components)


def frame_bytes(frame):
    from modeling.phase5b4_contract_v1 import csv_bytes
    return csv_bytes(frame)


def load_frame(raw):
    frame = pd.read_csv(io.BytesIO(raw), float_precision='round_trip')
    if 'event_date' in frame:
        frame.event_date = pd.to_datetime(frame.event_date)
    return frame


def inference_binding(package, projection, frame, contract, pipeline):
    recipes = {'challenger': 'pinned_corrected_event_reference_saved_final',
               'reference': 'phase5_legacy_reference_adapter_v1_saved_components',
               'march': 'pinned_corrected_event_reference_guarded_march_saved_components'}
    return {'version': INFERENCE_VERSION, 'pipeline': pipeline, 'feature_recipe': recipes[pipeline],
            'contract_sha256': digest(contract), 'capture_receipt_sha256': package['receipt_sha256'],
            'capture_id': package['receipt']['capture_id'], 'source_sha256': package['source_sha256'],
            'observed_at': package['receipt']['observation_cutoff'],
            'projection_sha256': digest(projection), 'evidence_sha256': package['evidence_sha256'],
            'matrix_sha256': sha(frame_bytes(frame)), 'components': PINS[pipeline],
            'training_reference_is_contemporary_source': False,
            'march_training_reference_api_certificate': ('unchanged_metadata_input_feature_provenance' if pipeline == 'march' else None)}


def validate_binding(binding, package, projection, frame, contract, pipeline):
    require(binding == inference_binding(package, projection, frame, contract, pipeline), 'incompatible_inference_provenance')
    require(instant(package['receipt']['started_at']) > max([instant(contract['frozen_at']), *map(instant, contract['component_freezes'])]), 'stale_capture_replay')
    require(package['receipt_sha256'] != sha(local_bytes(LEGACY, 'source_capture_receipt.json')), 'original_training_capture_replay')
    require(frame['label'].isna().all() if 'label' in frame else True, 'target_outcome_leakage')
    for col in IDENTITY:
        for v in frame[col]:
            uuid(v)
    require(not frame.empty and not frame.fight_id.duplicated().any() and not frame.fighter_1_id.eq(frame.fighter_2_id).any(), 'inference_identity')
    targets = {t['fight_id']: t for t in projection['targets'] if t.get('fight_id')}
    for _, r in frame.iterrows():
        require(r.fight_id in targets and all(r[k] == targets[r.fight_id][k] for k in IDENTITY), 'inference_target_identity')
        require(r.event_date.date().isoformat() == targets[r.fight_id]['event_date'], 'inference_target_date')


def build_features(package, projection, ready_ids, *, reference_preprocessing):
    from modeling.phase5_current_data import source_data, table_bytes
    from modeling.phase5b4_contract_v1 import FEATURES, META
    from features.replay import index_source
    from features.forecast_replay import reconstruct_forecast
    from modeling.phase5_reference_adapter import build_reference_features
    cutoff = package['receipt']['observation_cutoff']
    source = {'events': projection['events'], 'fighters': projection['profiles'],
              'fights': [{k: v for k, v in f.items() if k != 'event_date'} for f in projection['history']],
              'fight_stats_aggregate': projection['statistics'], 'schemas': package['schemas']}
    data = source_data({'sources/' + n + '.json': table_bytes(v) for n, v in source.items()})
    for f in data.fights:
        f['scheduled_rounds'] = None
        fr, ft = f.get('finish_round'), f.get('finish_time_seconds')
        f['elapsed_duration_seconds'] = (fr - 1) * 300 + ft if fr is not None and ft is not None else None
    index_source(data)
    rows, lineage = [], []
    targets = {t['fight_id']: t for t in projection['targets'] if t.get('fight_id')}
    for fid in ready_ids:
        t = deepcopy(targets[fid])
        t['event_date'] = date.fromisoformat(t['event_date'])
        require(type(t.get('is_title_fight')) is bool, 'title_evidence_required_before_features')
        row, trace = reconstruct_forecast(data, t, cutoff)
        row.update(label=None, feature_version=2)
        rows.append(row)
        lineage.append(trace)
    challenger = pd.DataFrame(rows)[META + FEATURES]
    challenger.event_date = pd.to_datetime(challenger.event_date)
    for c in FEATURES:
        challenger[c] = pd.to_numeric(challenger[c], errors='raise').astype(float)
    # March uses the corrected numeric functions with saved March priors, with
    # its old training-reference provenance explicitly separate in the binding.
    march = challenger.copy(deep=True)
    legacy_source = deepcopy(source)
    original = {f['fight_id']: f for f in package['rows']['fights']}
    legacy_source['fights'] += [dict(original[fid], is_title_fight=targets[fid]['is_title_fight']) for fid in ready_ids]
    legacy = source_data({'sources/' + n + '.json': table_bytes(v) for n, v in legacy_source.items()})
    reference = build_reference_features(legacy, ready_ids, observation_cutoff=cutoff,
                                         saved_preprocessing=deepcopy(reference_preprocessing))
    reference.event_date = pd.to_datetime(reference.event_date)
    return {'challenger': challenger, 'reference': reference, 'march': march}, lineage


class SavedPipelines:
    """Loading does not construct features, fit, connect or predict."""
    def __init__(self):
        verify_components()
        from modeling.phase5b4_bundle_v1 import load_challenger_bundle
        from modeling.phase5_frozen_reference import load_reference
        from modeling.xgb_candidate_bundle import CandidateBundle
        p = PINS['challenger']
        self.challenger = load_challenger_bundle(ROOT / p['path'], expected_checksums_sha256=p['checksums'], expected_manifest_sha256=p['manifest'])
        p = PINS['reference']
        self.reference = load_reference(ROOT / p['path'], expected_checksums_sha256=p['checksums'], expected_manifest_sha256=p['manifest'])
        self.march = CandidateBundle(ROOT / PINS['march']['path'])

    @property
    def reference_preprocessing(self):
        return deepcopy(self.reference.preprocessing)

    def predict(self, pipeline, frame, *, binding, package, projection, contract):
        validate_binding(binding, package, projection, frame, contract, pipeline)
        from modeling.phase5b4_contract_v1 import FEATURES, META, DEBUT, calibrate, probabilities, transform
        if pipeline == 'challenger':
            require(list(frame.columns) == META + FEATURES and frame.feature_version.eq(2).all(), 'challenger_feature_contract')
            require(frame.scheduled_rounds.isna().all() and frame[DEBUT].isna().all().all() and frame.both_debuting.isin([0, 1]).all(), 'challenger_recipe_semantics')
            require(not np.isinf(frame[FEATURES].to_numpy(dtype=float)).any(), 'invalid_feature_values')
            # This is the explicitly versioned successor inference API, sharing
            # saved final components, with its own code/manifest/freeze pins.
            processed = transform(frame.copy(deep=True), self.challenger.priors['final'])
            raw = probabilities(self.challenger.models['final'].predict_proba(processed[FEATURES])[:, 1])
            calibrated = calibrate(self.challenger.calibrator, raw)
        elif pipeline == 'reference':
            require(frame.recipe_version.eq('phase5_legacy_reference_adapter_v1').all(), 'reference_recipe_contract')
            require(frame.observation_cutoff.map(instant).eq(instant(package['receipt']['observation_cutoff'])).all(), 'reference_common_cutoff')
            processed = frame.copy(deep=True)  # legacy adapter already applied saved priors
            raw = probabilities(self.reference.base.predict_proba(processed[FEATURES])[:, 1])
            calibrated = calibrate(self.reference.calibrator, raw)
        elif pipeline == 'march':
            # The guarded API retains all its schedule/debut/order checks and
            # saved-component iteration range. No direct learner bypass.
            result = self.march.predict(frame.copy(deep=True), provenance=deepcopy(self.march.metadata['input_feature_provenance']))
            from features.debut_prior import apply_debut_features
            processed = apply_debut_features(frame.copy(deep=True), deepcopy(self.march.priors))
            raw = probabilities(result.raw_prob_f1)
            calibrated = probabilities(result.calibrated_prob_f1)
        else:
            raise ValueError('Unknown pipeline')
        return processed, raw, calibrated

"""Auditable stages; no implicit acquisition, fitting, or outcome-loading paths."""
from copy import deepcopy
import json
from modeling.phase5c1_contract import decoded
from pathlib import Path
import math

from modeling.phase5c1_contract import (Blocked, IDENTITY, PINS, VERSION, digest, encoded,
    instant, inventory, local_bytes, now, publish, require, sha)
from modeling.phase5c1_sources import project, validate_package
from modeling.phase5c1_evidence import essential_reasons
from modeling.phase5c1_adapters import (build_features, frame_bytes, inference_binding,
                                      load_frame, validate_binding)
from modeling.phase5c1_journal import append, exclusive, latest, register, state, namespace


def decision(p):
    require(type(p) in (int, float) and math.isfinite(p) and 0 <= p <= 1, 'invalid_probability')
    no_pick = 0.4 <= p <= 0.6
    return {'version': 'probability_band_v1', 'status': 'scored_no_pick' if no_pick else 'scored_actionable',
            'pick_label': None if no_pick else int(p >= 0.5), 'latent_fighter_1': p >= 0.5,
            'high_confidence': p <= 0.3 or p >= 0.7}


def check(package, projection, *, forecast_at):
    from modeling.prospective_registry import lead_time_reasons
    profiles = {p['fighter_id'] for p in projection['profiles']}
    records = []
    require(instant(forecast_at) >= instant(package['receipt']['observation_cutoff']), 'prediction_before_observation')
    for t in projection['targets']:
        reasons = []
        if any(not t.get(k) for k in IDENTITY) or not t.get('event_date'):
            reasons.append('unresolved_identity_or_date')
        else:
            reasons, title = essential_reasons(t, projection['history'], profiles, package['evidence'], package['receipt']['observation_cutoff'])
            t['is_title_fight'] = title
            reasons += lead_time_reasons(t['event_date'], package['receipt']['observation_cutoff'])
            reasons += lead_time_reasons(t['event_date'], forecast_at)
            if t['fight_id'] in projection['aliases'] and projection['aliases'][t['fight_id']] != t['fight_id']:
                reasons.append('alias_represented_by_canonical_bout')
        records.append({'source_row_id': t['source_row_id'], 'fight_id': t.get('fight_id'),
                        'status': 'BLOCKED' if reasons else 'READY', 'reasons': sorted(set(reasons)),
                        'disposition': 'data_blocked_not_scored' if reasons else 'ready_for_paired_forecast'})
    return {'status': 'READY' if any(r['status'] == 'READY' for r in records) else 'BLOCKED',
            'considered_count': len(records), 'bouts': records}


def registry_gate(root, package, ready):
    current = state(latest(root)[2])
    for row in ready:
        key = row['source_row_id']
        require(key in current, 'registration_required_before_prediction')
        b = current[key]
        require(b['disposition'] == 'active', 'cancelled_or_replaced_bout')
        require(b.get('selected') is None, 'first_pair_already_selected')
        # A fully ready candidate reserved before a failed/crashed attempt cannot
        # be silently replaced by later probabilities/captures.
        require(not b['attempts'], 'failed_or_incomplete_first_candidate_requires_review')
    return current


def forecast(root, run_name, package, contract, pipelines, *, clock=now, include_march=True, authorization=None):
    """Caller supplies immutable validated capture and explicit predictor capability.

    The Phase5C.1 CLI supplies synthetic pipelines only. Real activation requires
    a separately authorized Phase5C.2 session, not an implementation flag here.
    """
    require(Path(run_name).name == run_name and run_name not in {'.', '..'}, 'unsafe_run_name')
    with exclusive(root):
        require(not (Path(root) / 'runs' / run_name).exists(), 'overwrite_refused')
        # Receipt integrity precedes coverage registration. All structurally bad
        # considered rows are retained even if raw/evidence validation blocks.
        receipt_bytes = local_bytes(package['directory'], 'capture_receipt.json')
        require(sha(receipt_bytes) == package['receipt_sha256'], 'capture_receipt_pin_mismatch')
        receipt = decoded(receipt_bytes)
        require(type(receipt.get('synthetic')) is bool, 'capture_kind_required')
        require(getattr(pipelines, 'synthetic', False) == receipt['synthetic'], 'synthetic_real_pipeline_mismatch')
        if not receipt['synthetic']:
            require(authorization is not None and authorization.get('scope') == 'separately_authorized_phase5c2_forecast' and
                    authorization.get('capture_receipt_sha256') == package['receipt_sha256'] and
                    authorization.get('contract_sha256') == digest(contract) and authorization.get('authorization_text_sha256'),
                    'separate_real_forecast_authorization_required')
            require(clock is now, 'real_forecast_requires_actual_utc_clock')
        namespace(root, synthetic=receipt['synthetic'])
        started = clock()
        existing = latest(root)[2]
        keys = {c['source_row_id'] for c in receipt['considered']}
        observations = [e for e in existing if e['action'] == 'observation_received' and keys.intersection(e['source_row_ids'])]
        current_bouts = state(existing)
        for key in keys.intersection(current_bouts):
            require(current_bouts[key].get('selected') is None, 'first_pair_already_selected')
            require(not current_bouts[key]['attempts'], 'failed_or_incomplete_first_candidate_requires_review')
        require(not any(e['capture_receipt_sha256'] == package['receipt_sha256'] for e in observations), 'capture_receipt_replay')
        require(not any(instant(e['observation_cutoff']) > instant(receipt['observation_cutoff']) for e in observations), 'out_of_order_capture_requires_review')
        registered_pin = register(root, receipt['considered'], receipt_pin=package['receipt_sha256'], recorded_at=started)
        append(root, {'action': 'observation_received', 'source_row_ids': sorted(keys), 'recorded_at': started,
                      'capture_receipt_sha256': package['receipt_sha256'], 'capture_id': receipt['capture_id'],
                      'observation_cutoff': receipt['observation_cutoff'], 'synthetic': receipt['synthetic']})
        try:
            package = validate_package(package['directory'], receipt_pin=package['receipt_sha256'], contract=contract)
            projection = project(package)
            readiness = check(package, projection, forecast_at=started)
        except (ValueError, KeyError, TypeError, OSError) as exc:
            reason = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            result = {'status': 'BLOCKED', 'prediction_calls': 0, 'reason': reason,
                      'considered_count': len(receipt['considered']), 'bouts': [
                          {'source_row_id': c['source_row_id'], 'status': 'BLOCKED', 'reasons': [reason],
                           'disposition': 'data_blocked_not_scored'} for c in receipt['considered']]}
            payload = {'run.json': encoded({'version': VERSION, 'status': 'BLOCKED', 'synthetic': receipt['synthetic'],
                        'prediction_calls': 0, 'capture_receipt_sha256': package['receipt_sha256'], 'failure': reason,
                        'registered_checksums_sha256': registered_pin, 'started_at': started, 'completed_at': clock()}),
                       'readiness.json': encoded(result), 'capture_receipt.json': receipt_bytes,
                       'contract.json': encoded(contract)}
            for source_file in Path(package['directory']).rglob('*'):
                if source_file.is_file():
                    n = str(source_file.relative_to(package['directory']))
                    payload['untrusted_observation/' + n] = local_bytes(package['directory'], n)
            pin = publish(Path(root) / 'runs' / run_name, payload)
            return result | {'checksums_sha256': pin}

        ready = [r for r in readiness['bouts'] if r['status'] == 'READY']
        payloads = {'readiness.json': encoded(readiness), 'capture_receipt.json': local_bytes(package['directory'], 'capture_receipt.json'),
                    'package.json': encoded({k: v for k, v in package.items() if k != 'directory'}),
                    'projection.json': encoded(projection), 'contract.json': encoded(contract)}
        # Preserve exact source/evidence bodies, including reviewed originals.
        for p in Path(package['directory']).rglob('*'):
            if p.is_file():
                n = str(p.relative_to(package['directory']))
                payloads['observation/' + n] = local_bytes(package['directory'], n)
        base = {'version': VERSION, 'synthetic': package['receipt']['synthetic'], 'wall_started_at': now(),
                'clock_kind': 'synthetic_scenario' if package['receipt']['synthetic'] else 'actual_utc',
                'started_at': started, 'capture_id': package['receipt']['capture_id'],
                'capture_receipt_sha256': package['receipt_sha256'], 'registered_checksums_sha256': registered_pin,
                'common_observation_cutoff': package['receipt']['observation_cutoff'],
                'components': PINS, 'primary_pair': ['challenger', 'reference'],
                'outcomes_loaded': False, 'fits': 0, 'real_source_access': False}
        if not ready:
            payloads['run.json'] = encoded(base | {'status': 'BLOCKED', 'prediction_calls': 0, 'completed_at': clock()})
            pin = publish(Path(root) / 'runs' / run_name, payloads)
            return {'status': 'BLOCKED', 'checksums_sha256': pin, 'prediction_calls': 0, 'readiness': readiness}
        registry_gate(root, package, ready)
        source_keys = [r['source_row_id'] for r in ready]
        append(root, {'action': 'attempt_reserved', 'source_row_ids': source_keys, 'recorded_at': started,
                      'capture_receipt_sha256': package['receipt_sha256'], 'run': run_name})
        ids = [r['fight_id'] for r in ready]
        results, calls, error = {}, 0, None
        try:
            saved_reference_preprocessing = pipelines.reference_preprocessing
            payloads['preprocessing/reference.json'] = encoded(saved_reference_preprocessing)
            frames, trace = build_features(package, projection, ids, reference_preprocessing=saved_reference_preprocessing)
            payloads['feature_lineage.json'] = encoded(trace)
            for name in ('challenger', 'reference', 'march'):
                payloads[f'inputs/{name}.csv'] = frame_bytes(frames[name])
            # Bind the exact inputs after round-trip; the exact serialized values
            # are what every predictor and deterministic replay consume.
            frames = {n: load_frame(frame_bytes(f)) for n, f in frames.items()}
            for name in ('challenger', 'reference', 'march'):
                if name == 'march' and not include_march:
                    payloads['march_status.json'] = encoded({'status': 'missing_counterpart_prediction', 'reason': 'optional_control_not_requested'})
                    continue
                binding = inference_binding(package, projection, frames[name], contract, name)
                payloads[f'bindings/{name}.json'] = encoded(binding)
                validate_binding(binding, package, projection, frames[name], contract, name)
                try:
                    invoked = clock()
                    require(instant(invoked) >= instant(started), 'forecast_clock_regression')
                    calls += 1
                    processed, raw, calibrated = pipelines.predict(name, frames[name], binding=binding,
                                                                   package=package, projection=projection, contract=contract)
                    completed = clock()
                    require(instant(completed) >= instant(invoked), 'forecast_clock_regression')
                    require(len(raw) == len(ids) == len(calibrated), 'incomplete_pipeline_output')
                    rows = []
                    for fid, r, c in zip(ids, raw, calibrated):
                        r, c = float(r), float(c)
                        decision(r)
                        rows.append({'fight_id': fid, 'raw_probability_fighter_1': r,
                                     'calibrated_probability_fighter_1': c, 'decision': decision(c),
                                     'forecast_started_at': invoked, 'forecast_completed_at': completed})
                    payloads[f'processed/{name}.csv'] = frame_bytes(processed)
                    payloads[f'outputs/{name}.json'] = encoded(rows)
                    results[name] = rows
                    if name == 'march':
                        payloads['march_status.json'] = encoded({'status': 'READY'})
                except Exception as exc:
                    if name != 'march':
                        raise
                    payloads['march_status.json'] = encoded({'status': 'missing_counterpart_prediction', 'reason': type(exc).__name__})
            completed = clock()
            end_readiness = check(package, deepcopy(projection), forecast_at=completed)
            require(all(r['status'] == 'READY' for r in end_readiness['bouts'] if r['fight_id'] in ids), 'forecast_completion_outside_window')
            require('challenger' in results and 'reference' in results, 'incomplete_primary_pair')
        except Exception as exc:
            error = str(exc) if isinstance(exc, Blocked) else type(exc).__name__
            completed = clock()
        status = 'READY' if error is None else 'BLOCKED'
        payloads['run.json'] = encoded(base | {'status': status, 'completed_at': completed, 'wall_completed_at': now(),
            'prediction_calls': calls, 'failure': error, 'complete_primary_pair': error is None,
            'selected_without_probabilities': True, 'scored_fight_ids': ids if error is None else [],
            'primary_population': ids if error is None else [], 'partial_outputs_are_published_forecasts': False})
        pin = publish(Path(root) / 'runs' / run_name, payloads)
        append(root, {'action': 'attempt_completed' if error is None else 'attempt_failed',
                      'source_row_ids': source_keys, 'recorded_at': completed,
                      'capture_receipt_sha256': package['receipt_sha256'], 'run': run_name, 'run_checksums_sha256': pin})
        return {'status': status, 'checksums_sha256': pin, 'prediction_calls': calls, 'failure': error,
                'complete_primary_pair': error is None, 'synthetic': package['receipt']['synthetic']}


def verify_run(directory, pin, *, pipelines=None, expected_contract=None):
    """Hash/contract/input/decision verification; optional explicit synthetic replay."""
    files = inventory(directory, pin)
    run = decoded(local_bytes(directory, 'run.json'))
    contract = decoded(local_bytes(directory, 'contract.json'))
    if expected_contract is not None:
        require(contract == expected_contract, 'external_contract_mismatch')
    if 'projection.json' not in files:
        require(run['status'] == 'BLOCKED' and run['prediction_calls'] == 0, 'invalid_blocked_run')
        return {'status': 'READY', 'verification': 'VERIFIED', 'run_status': 'BLOCKED', 'checksums_sha256': pin}
    package = validate_package(Path(directory) / 'observation', receipt_pin=run['capture_receipt_sha256'], contract=contract)
    projection = project(package)
    readiness = check(package, projection, forecast_at=run['started_at'])
    require(encoded(readiness) == local_bytes(directory, 'readiness.json'), 'readiness_replay_mismatch')
    require(encoded(projection) == local_bytes(directory, 'projection.json'), 'projection_replay_mismatch')
    if run['status'] == 'READY':
        require(run['complete_primary_pair'] and run['components'] == PINS, 'published_pair_contract')
        require(all('outputs/' + n + '.json' in files for n in ('challenger', 'reference')), 'missing_primary_counterpart')
        from modeling.prospective_registry import lead_time_reasons
        for t in projection['targets']:
            if t.get('fight_id') in run['primary_population']:
                require(not lead_time_reasons(t['event_date'], run['completed_at']), 'late_published_forecast')
    if 'preprocessing/reference.json' in files:
        saved = decoded(local_bytes(directory, 'preprocessing/reference.json'))
        ready_ids = [r['fight_id'] for r in readiness['bouts'] if r['status'] == 'READY']
        rebuilt, trace = build_features(package, projection, ready_ids, reference_preprocessing=saved)
        require(encoded(trace) == local_bytes(directory, 'feature_lineage.json'), 'feature_lineage_replay_mismatch')
        for name, frame in rebuilt.items():
            require(frame_bytes(frame) == local_bytes(directory, f'inputs/{name}.csv'), 'feature_matrix_replay_mismatch')
    for name in ('challenger', 'reference', 'march'):
        output = f'outputs/{name}.json'
        if output not in files:
            continue
        frame = load_frame(local_bytes(directory, f'inputs/{name}.csv'))
        binding = decoded(local_bytes(directory, f'bindings/{name}.json'))
        validate_binding(binding, package, projection, frame, contract, name)
        rows = decoded(local_bytes(directory, output))
        require([r['fight_id'] for r in rows] == list(frame.fight_id), 'output_identity_mismatch')
        for row in rows:
            require(row['decision'] == decision(row['calibrated_probability_fighter_1']), 'decision_replay_mismatch')
            decision(row['raw_probability_fighter_1'])
            require(instant(run['started_at']) <= instant(row['forecast_started_at']) <= instant(row['forecast_completed_at']) <= instant(run['completed_at']), 'forecast_receipt_chronology')
        if pipelines is not None:
            require(run['synthetic'] is True, 'real_probability_replay_not_authorized_in_phase5c1')
            processed, raw, calibrated = pipelines.predict(name, frame, binding=binding, package=package, projection=projection, contract=contract)
            require(frame_bytes(processed) == local_bytes(directory, f'processed/{name}.csv'), 'processed_replay_mismatch')
            require([float(v) for v in raw] == [r['raw_probability_fighter_1'] for r in rows] and
                    [float(v) for v in calibrated] == [r['calibrated_probability_fighter_1'] for r in rows], 'probability_replay_mismatch')
    return {'status': 'READY', 'verification': 'VERIFIED', 'run_status': run['status'],
            'synthetic': run['synthetic'], 'replayed': pipelines is not None, 'checksums_sha256': pin}

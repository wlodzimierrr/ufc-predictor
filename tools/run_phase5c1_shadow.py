#!/usr/bin/env python3
"""Explicit offline shadow stages. Real forecasting stays separately authorized."""
import argparse
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modeling.phase5c1_contract import (Blocked, CODE, INFERENCE_VERSION, LEGACY, OUTPUT, PINS, ROOT,
    SOURCE_VERSION, VERSION, digest, encoded, instant, inventory, load_contract,
    local_bytes, now, publish, require, sha, verify_components)
from modeling.phase5c1_safety import implementation_guards


def freeze(destination):
    files = verify_components()
    challenger = json.loads(local_bytes(ROOT / PINS['challenger']['path'], 'metadata.json'))
    reference = json.loads(local_bytes(ROOT / PINS['reference']['path'], 'manifest.json'))
    march = json.loads(local_bytes(ROOT / PINS['march']['path'], 'run_receipt.json'))
    # March predates both primary bundles; pin its actual saved receipt as well.
    freezes = list(challenger['component_freeze_times'].values()) + list(reference['component_freeze_times'].values()) + [march['fits_finished_at'], march['published_at']]
    timestamp = now()
    c = {'version': VERSION, 'inference_version': INFERENCE_VERSION, 'source_version': SOURCE_VERSION,
         'frozen_at': timestamp, 'component_freezes': freezes, 'components': PINS,
         'march_freeze_receipt_sha256': files['march']['run_receipt.json'],
         'code_sha256': {n: sha(local_bytes(ROOT, n)) for n in CODE},
         'real_forecasting': 'BLOCKED_PENDING_SEPARATE_AUTHORIZATION_AND_VALID_INPUT',
         'learned_components_unchanged': True, 'historical_comparison': 'STILL_BLOCKED'}
    pin = publish(destination, {'contract.json': encoded(c), 'freeze_receipt.json': encoded({'frozen_at': timestamp,
                    'fits': 0, 'real_predictions': 0, 'real_source_access': 0}),
                    'component_verification.json': encoded(files)})
    return {'status': 'READY', 'contract_checksums_sha256': pin, 'contract_sha256': digest(c), 'frozen_at': timestamp}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('stage', choices=['freeze-contract', 'validate', 'register', 'revise', 'replace', 'cancel',
        'prepare', 'check', 'build', 'forecast', 'verify', 'replay', 'synthetic-demo', 'verify-components', 'load-components'])
    p.add_argument('--contract', type=Path)
    p.add_argument('--contract-pin')
    p.add_argument('--package', type=Path)
    p.add_argument('--receipt-pin')
    p.add_argument('--output', type=Path)
    p.add_argument('--journal', type=Path)
    p.add_argument('--run-name')
    p.add_argument('--run', type=Path)
    p.add_argument('--run-pin')
    p.add_argument('--source-row-id')
    p.add_argument('--reason')
    p.add_argument('--forecast-at')
    p.add_argument('--without-march', action='store_true')
    args = p.parse_args()
    result = None
    try:
        with implementation_guards(allow_loading=args.stage == 'load-components'):
            if args.stage == 'verify-components':
                result = {'status': 'READY', 'components_verified': {n: len(v) for n, v in verify_components().items()}}
            elif args.stage == 'load-components':
                from modeling.phase5c1_adapters import SavedPipelines
                SavedPipelines()
                result = {'status': 'READY', 'loaded': list(PINS), 'prediction_calls': 0, 'fit_calls': 0}
            elif args.stage == 'freeze-contract':
                require(args.output is not None and args.output.resolve().is_relative_to(OUTPUT), 'isolated_phase5c_root_required')
                result = freeze(args.output)
            else:
                require(args.contract is not None and args.contract_pin, 'external_contract_pin_required')
                contract = load_contract(args.contract, args.contract_pin)
                from modeling.phase5c1_synthetic import SyntheticPipelines, make_package
                from modeling.phase5c1_sources import project, validate_package
                from modeling.phase5c1_journal import cancel, exclusive, register, namespace
                from modeling.phase5c1_runner import check, forecast, verify_run
                if args.stage in {'verify', 'replay'}:
                    result = verify_run(args.run, args.run_pin, pipelines=SyntheticPipelines() if args.stage == 'replay' else None, expected_contract=contract)
                elif args.stage == 'cancel':
                    with exclusive(args.journal):
                        pin = cancel(args.journal, args.source_row_id, recorded_at=now(), reason=args.reason)
                    result = {'status': 'READY', 'registry_checksums_sha256': pin}
                elif args.stage == 'synthetic-demo':
                    require(args.output and args.output.resolve().is_relative_to(OUTPUT), 'isolated_phase5c_root_required')
                    pin, receipt = make_package(args.output / 'SYNTHETIC_capture', contract, name=args.run_name or 'demo')
                    package = validate_package(args.output / 'SYNTHETIC_capture', receipt_pin=pin, contract=contract)
                    simulated = instant(receipt['completed_at'])
                    from datetime import timedelta
                    clock = lambda: (simulated + timedelta(minutes=1)).isoformat()
                    result = forecast(args.output / 'SYNTHETIC_journal', args.run_name or 'SYNTHETIC_demo', package,
                                      contract, SyntheticPipelines(), clock=clock, include_march=not args.without_march)
                    result['replay'] = verify_run(args.output / 'SYNTHETIC_journal/runs' / (args.run_name or 'SYNTHETIC_demo'),
                                                 result['checksums_sha256'], pipelines=SyntheticPipelines())
                else:
                    require(args.package is not None and args.receipt_pin, 'external_capture_receipt_pin_required')
                    if args.stage == 'forecast':
                        raw = json.loads(local_bytes(args.package, 'capture_receipt.json'))
                        require(raw['synthetic'], 'real_forecasting_requires_separately_authorized_phase5c2_activation')
                        result = forecast(args.journal, args.run_name, {'directory': str(args.package), 'receipt_sha256': args.receipt_pin},
                            contract, SyntheticPipelines(), clock=(lambda: args.forecast_at) if args.forecast_at else now,
                            include_march=not args.without_march)
                    elif args.stage in {'register', 'revise', 'replace'}:
                        raw_bytes = local_bytes(args.package, 'capture_receipt.json')
                        require(sha(raw_bytes) == args.receipt_pin, 'capture_receipt_pin_mismatch')
                        with exclusive(args.journal):
                            namespace(args.journal, synthetic=json.loads(raw_bytes)['synthetic'])
                            pin = register(args.journal, json.loads(raw_bytes)['considered'], receipt_pin=args.receipt_pin,
                                           recorded_at=now(), action=args.stage, replaces=args.source_row_id)
                        result = {'status': 'READY', 'registry_checksums_sha256': pin}
                    else:
                        package = validate_package(args.package, receipt_pin=args.receipt_pin, contract=contract)
                        if args.stage == 'validate':
                            result = {'status': 'READY', 'capture_id': package['receipt']['capture_id'], 'receipt_sha256': args.receipt_pin}
                        else:
                            projection = project(package)
                            if args.stage == 'prepare':
                                result = {'status': 'READY', 'projection_sha256': digest(projection), 'history_rows': len(projection['history'])}
                                if args.output:
                                    result['checksums_sha256'] = publish(args.output, {'projection.json': encoded(projection), 'provenance.json': encoded({'receipt_sha256': args.receipt_pin})})
                            else:
                                require(args.forecast_at, 'explicit_forecast_time_required')
                                result = check(package, projection, forecast_at=args.forecast_at)
                                if args.stage == 'build' and result['status'] == 'READY':
                                    from modeling.phase5c1_adapters import build_features, frame_bytes
                                    # Saved reference preprocessing only; no estimator load.
                                    saved = json.loads(local_bytes(ROOT / PINS['reference']['path'], 'preprocessing.json'))
                                    frames, lineage = build_features(package, projection, [b['fight_id'] for b in result['bouts'] if b['status'] == 'READY'], reference_preprocessing=saved)
                                    require(args.output, 'exclusive_build_output_required')
                                    result['checksums_sha256'] = publish(args.output, {f'{n}.csv': frame_bytes(f) for n, f in frames.items()} | {'lineage.json': encoded(lineage), 'readiness.json': encoded(result)})
    except (ValueError, KeyError, TypeError, OSError, RuntimeError) as exc:
        result = {'status': 'BLOCKED', 'reason': str(exc) if isinstance(exc, ValueError) else type(exc).__name__, 'stage': args.stage}
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0 if result['status'] == 'READY' else 2


if __name__ == '__main__':
    raise SystemExit(main())

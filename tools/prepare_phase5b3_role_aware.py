"""Offline guarded Phase5B3 preparation, explicit tests, and integrity verification."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modeling.phase5b3_safety import preparation_guards

SAFE_TESTS = [
    'modeling/tests/test_phase5b3_role_aware.py',
    'features/tests/test_replay.py', 'features/tests/test_elo.py', 'features/tests/test_history.py',
    'modeling/tests/test_phase5a_prospective.py::test_forecast_lead_inclusive_boundaries',
    'modeling/tests/test_phase5a_prospective.py::test_first_complete_pair_and_common_observation_cutoff',
    'modeling/tests/test_phase5a_prospective.py::test_unknown_metadata_never_defaults',
    'modeling/tests/test_phase5a_prospective.py::test_future_or_unfrozen_component_inputs_block_pair',
    'modeling/tests/test_phase5a_prospective.py::test_distinct_ambiguous_bouts_preserved_in_blocked_diagnostic_dataset',
]


def preservation(baseline):
    """Hash-only reading via file descriptors, including protected outcomes."""
    from modeling.phase5_role_aware import ROOT, OUTPUT, now, PreflightError
    changed, missing = [], []
    for name, expected in baseline['files'].items():
        path = ROOT / name
        if not path.is_file():
            missing.append(name)
            continue
        digest = hashlib.sha256()
        fd = os.open(path, os.O_RDONLY)
        try:
            while chunk := os.read(fd, 1024 * 1024):
                digest.update(chunk)
        finally:
            os.close(fd)
        if digest.hexdigest() != expected:
            changed.append(name)
    protected = [ROOT / p for p in ('models', 'data/raw', 'data/holdouts', 'data/audits')]
    protected += [p for p in (ROOT / 'data/experiments').iterdir() if p != OUTPUT]
    new = sorted(str(p.relative_to(ROOT)) for d in protected for p in d.rglob('*')
                 if p.is_file() and '__pycache__' not in p.parts and str(p.relative_to(ROOT)) not in baseline['files'])
    if changed or missing or new:
        raise PreflightError(f'Preservation failed: {changed}, {missing}, {new}')
    return {'status': 'VERIFIED', 'checked_at': now(), 'existing_files_checked': len(baseline['files']),
            'changed': changed, 'missing': missing, 'new_protected_files': new,
            'protected_outcomes': 'hash-only, never parsed'}


def prepare(destination, baseline_path, test_receipt):
    from modeling.phase5_role_aware import (
        build_payloads, publish, load_role_aware_preparation, now, json_bytes, sha256, PreflightError,
    )
    if destination.exists():
        raise PreflightError('Never overwrite a Phase5B3 run')
    started = now()
    baseline_raw = baseline_path.read_bytes()
    baseline = json.loads(baseline_raw)
    first_start = now()
    first = build_payloads()
    first_end = now()
    second_start = now()
    second = build_payloads()
    second_end = now()
    if first != second:
        raise PreflightError('Two independent deterministic builds differ')
    tests = json.loads(test_receipt.read_bytes())
    if tests.get('status') != 'PASSED' or tests.get('exit_code') != 0 or tests.get('safe_test_list') != SAFE_TESTS:
        raise PreflightError('Explicit guarded safe test receipt required')
    if tests.get('code_sha256') != json.loads(first['code_versions.json']):
        raise PreflightError('Safe tests did not cover current code bytes')
    preserved = preservation(baseline)
    first.update({'preservation_baseline.json': baseline_raw,
                  'preservation_verification.json': json_bytes(preserved),
                  'safe_test_receipt.json': test_receipt.read_bytes(),
                  'run_receipt.json': json_bytes({'contract_version': 'phase5_role_aware_preparation_v2',
                    'computation_started_at': started, 'first_build_started_at': first_start,
                    'first_build_completed_at': first_end, 'second_build_started_at': second_start,
                    'second_build_completed_at': second_end, 'publication_started_at': now(),
                    'two_independent_builds_byte_identical': True, 'deterministic_payloads': len(second),
                    'baseline_sha256': sha256(baseline_raw), 'source_capture_advanced': False,
                    'fitting_calls': 0, 'real_prediction_calls': 0, 'data_network_calls': 0,
                    'warehouse_connections': 0, 'frozen_outcome_parsing': 0})})
    root = publish(destination, first)
    readback_start = now()
    frame, manifest, folds = load_role_aware_preparation(destination,
        expected_checksums_sha256=root, expected_training_manifest_sha256=sha256(first['training_manifest.json']))
    return {'path': str(destination), 'checksums_sha256': root,
            'training_manifest_sha256': sha256(first['training_manifest.json']),
            'guarded_loading': 'PASSED', 'readback_started_at': readback_start, 'readback_completed_at': now(),
            'publication_readback_and_third_rebuild': 'EXACT', 'fitting_rows': len(frame),
            'oof_rows': folds['oof_rows'], 'fitting_input_readiness': manifest['fitting_input_readiness'],
            'post_publication_preservation': preservation(baseline)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('test'); p.add_argument('--receipt', type=Path, required=True)
    p = sub.add_parser('prepare')
    p.add_argument('--run-name', required=True)
    p.add_argument('--baseline', type=Path, required=True)
    p.add_argument('--test-receipt', type=Path, required=True)
    p.add_argument('--receipt', type=Path, required=True)
    p = sub.add_parser('verify')
    p.add_argument('--run', type=Path, required=True)
    p.add_argument('--checksums-sha256', required=True)
    p.add_argument('--manifest-sha256', required=True)
    p = sub.add_parser('preserve'); p.add_argument('--baseline', type=Path, required=True)
    args = parser.parse_args()
    with preparation_guards():
        from modeling.phase5_role_aware import (
            OUTPUT, CODE, code_hashes, json_bytes, load_role_aware_preparation, PreflightError, now,
        )
        if args.command == 'test':
            import pytest
            start = now()
            code = code_hashes(CODE)
            result = int(pytest.main(['-q', *SAFE_TESTS]))
            receipt = {'status': 'PASSED' if result == 0 else 'FAILED', 'exit_code': result,
                       'started_at': start, 'completed_at': now(), 'safe_test_list': SAFE_TESTS,
                       'execution_guards': 'fit/prior/calibrator/predict/deserialization/network/warehouse/outcome/credential reads',
                       'code_sha256': code}
            args.receipt.write_bytes(json_bytes(receipt))
            raise SystemExit(result)
        if args.command == 'prepare':
            if Path(args.run_name).name != args.run_name or args.run_name in ('', '.', '..'):
                raise PreflightError('Run name must be one path component')
            result = prepare(OUTPUT / args.run_name, args.baseline, args.test_receipt)
            args.receipt.write_bytes(json_bytes(result))
        elif args.command == 'verify':
            frame, manifest, folds = load_role_aware_preparation(args.run,
                expected_checksums_sha256=args.checksums_sha256,
                expected_training_manifest_sha256=args.manifest_sha256)
            result = {'status': 'VERIFIED', 'fitting_input_readiness': manifest['fitting_input_readiness'],
                      'fitting_rows': len(frame), 'oof_rows': folds['oof_rows'], 'verified_at': now()}
        else:
            result = preservation(json.loads(args.baseline.read_bytes()))
        print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()

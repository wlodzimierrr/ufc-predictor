"""Explicit safe tests, one authorized fixed run, and offline saved replay."""
import argparse
import json
from pathlib import Path
import shlex
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modeling.phase5b4_contract_v1 import (
    OUTPUT, SAFE_TESTS, ContractError, code_hashes, json_bytes, now,
)
from modeling.phase5b4_safety import offline_guards, loading_guards, preservation

def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    t = sub.add_parser('test'); t.add_argument('--receipt', type=Path, required=True)
    t = sub.add_parser('train')
    t.add_argument('--run-name', required=True)
    t.add_argument('--authorization', type=Path, required=True)
    t.add_argument('--baseline', type=Path, required=True)
    t.add_argument('--test-receipt', type=Path, required=True)
    t.add_argument('--receipt', type=Path, required=True)
    t = sub.add_parser('verify')
    t.add_argument('--run', type=Path, required=True)
    t.add_argument('--checksums-sha256', required=True)
    t.add_argument('--manifest-sha256', required=True)
    t.add_argument('--receipt', type=Path, required=True)
    t = sub.add_parser('preserve'); t.add_argument('--baseline', type=Path, required=True)
    t.add_argument('--receipt', type=Path, required=True)
    a = p.parse_args()
    with offline_guards():
        if a.command == 'test':
            sys.dont_write_bytecode = True
            import pytest
            started = now()
            code = code_hashes()
            class Counts:
                counts = {'passed': 0, 'failed': 0, 'skipped': 0}
                def pytest_runtest_logreport(self, report):
                    if report.when == 'call' or report.failed or report.skipped:
                        self.counts[report.outcome] += 1
            counts = Counts()
            from unittest.mock import patch
            from xgboost import XGBClassifier
            from sklearn.linear_model import LogisticRegression
            synthetic_fits = {'xgboost': 0, 'platt': 0}
            def tracked(original, name):
                def fit(model, *args, **kwargs):
                    synthetic_fits[name] += 1
                    return original(model, *args, **kwargs)
                return fit
            with patch.object(XGBClassifier, 'fit', tracked(XGBClassifier.fit, 'xgboost')), patch.object(LogisticRegression, 'fit', tracked(LogisticRegression.fit, 'platt')):
                exit_code = int(pytest.main(['-q', '-p', 'no:cacheprovider', '--basetemp=/tmp/phase5b4-current-challenger/pytest', *SAFE_TESTS], plugins=[counts]))
            result = {'status': 'PASSED' if exit_code == 0 else 'FAILED', 'exit_code': exit_code,
                'started_at': started, 'completed_at': now(), 'safe_test_list': SAFE_TESTS,
                'code_sha256': code, 'command': shlex.join(['python3', *sys.argv]),
                'guards': 'no network, warehouse, production writes, credentials or frozen outcome parsing',
                'test_counts': counts.counts, 'synthetic_estimator_fit_counts': synthetic_fits,
                'synthetic_fits': 'test fixtures only, separate from six authorized real estimator fits'}
            a.receipt.write_bytes(json_bytes(result))
            raise SystemExit(exit_code)
        elif a.command == 'train':
            if Path(a.run_name).name != a.run_name or a.run_name in ('', '.', '..'):
                raise ContractError('Run name must be one path component')
            from modeling.phase5b4_trainer_v1 import train_and_publish
            result = train_and_publish(OUTPUT / a.run_name, authorization_path=a.authorization,
                baseline_path=a.baseline, test_receipt_path=a.test_receipt,
                command=shlex.join(['python3', *sys.argv]))
        elif a.command == 'verify':
            from modeling.phase5b4_bundle_v1 import load_challenger_bundle
            from modeling.phase5b4_trainer_v1 import independent_replay, load_approved_preparation
            with loading_guards():
                approved = load_approved_preparation()
                bundle = load_challenger_bundle(a.run, expected_checksums_sha256=a.checksums_sha256,
                    expected_manifest_sha256=a.manifest_sha256)
                result = independent_replay(bundle, approved)
                result['command'] = shlex.join(['python3', *sys.argv])
        else:
            result = preservation(json.loads(a.baseline.read_bytes()))
        a.receipt.write_bytes(json_bytes(result))
        print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()

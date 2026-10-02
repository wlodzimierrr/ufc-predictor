"""Publish one bounded, outcome-independent Phase 4A.2 recovery attempt."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modeling.scoring_inputs import json_bytes, preservation_check, sha256
from modeling.source_reconciliation import OUTPUT_ROOT, prepare_recovery, publish_recovery


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--preservation-baseline', type=Path, required=True)
    parser.add_argument('--tests-log', type=Path, required=True)
    parser.add_argument('--commands-file', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.parent.resolve() != OUTPUT_ROOT.resolve():
        parser.error('Require a new isolated run; refusing overwrite')
    started = datetime.now(timezone.utc).isoformat()
    baseline_bytes = args.preservation_baseline.read_bytes()
    baseline = json.loads(baseline_bytes)
    preservation_check(baseline)
    first, validation = prepare_recovery()
    second, second_validation = prepare_recovery()
    if first != second or validation != second_validation:
        raise ValueError('Deterministic recovery rebuild differs')
    validation['deterministic_rebuild'] = {'passed': True, 'independent_builds': 2,
        'byte_identical_files': len(first), 'same_bounded_source_scope': True,
        'component_sha256': {k: sha256(v) for k, v in sorted(first.items())}}
    first['validation_results.json'] = json_bytes(validation)
    first['preservation_baseline.json'] = baseline_bytes
    first['preservation_check.json'] = json_bytes(preservation_check(baseline))
    first['regressions.log'] = args.tests_log.read_bytes()
    first['commands.json'] = args.commands_file.read_bytes()
    first['run_receipt.json'] = json_bytes({'status': validation['status'], 'started_at': started,
        'finished_at': datetime.now(timezone.utc).isoformat(),
        'authorization': 'Phase 4A.2 bounded archived-source recovery and reconciliation; commit and normal fast-forward upstream push',
        'receipt_time_meaning': 'current computation, never historical availability',
        'scoring_evaluation_fitting_promotion_authorized': False, 'bounded_attempt_complete': True})
    publish_recovery(args.output, first)
    preservation_check(baseline)
    print(json.dumps({'output': str(args.output), 'status': validation['status'],
        'resolved_original_forecasts': validation['resolved_original_forecasts'],
        'remaining_affected_forecasts': validation['remaining_affected_forecasts'],
        'gap_counts': validation['gap_counts'],
        'checksums_sha256': sha256((args.output / 'checksums.json').read_bytes())}, indent=2))


if __name__ == '__main__':
    main()

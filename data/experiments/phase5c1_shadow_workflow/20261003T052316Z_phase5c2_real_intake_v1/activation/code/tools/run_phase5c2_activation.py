#!/usr/bin/env python3
"""Separate bounded Phase5C.2 entrypoint; no changes to the synthetic-only CLI."""
import argparse
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modeling.phase5c2_activation import freeze, capture, intake, verify
from modeling.phase5c1_contract import OUTPUT, require


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('stage', choices=['freeze', 'capture', 'intake', 'verify'])
    p.add_argument('--activation', required=True, type=Path)
    p.add_argument('--activation-pin')
    p.add_argument('--capture-result-pin')
    p.add_argument('--intake-result-pin')
    p.add_argument('--authorization-text', type=Path)
    args = p.parse_args()
    try:
        require(args.activation.resolve().is_relative_to(OUTPUT), 'isolated_phase5c_root_required')
        if args.stage == 'freeze':
            result = freeze(args.activation, args.authorization_text)
        elif args.stage == 'capture':
            # Import credentials only inside the separately authorized capture.
            # The capture wrapper suppresses all driver diagnostics.
            from warehouse.db import get_connection
            result = capture(args.activation.parent / 'capture_attempt', activation_directory=args.activation,
                             activation_pin=args.activation_pin, connection_factory=get_connection)
        elif args.stage == 'intake':
            result = intake(activation_directory=args.activation, activation_pin=args.activation_pin,
                            capture_result_pin=args.capture_result_pin)
        else:
            result = verify(activation_directory=args.activation, activation_pin=args.activation_pin,
                            intake_result_pin=args.intake_result_pin)
    except Exception as exc:
        result = {'status': 'BLOCKED', 'stage': args.stage, 'failure_class': type(exc).__name__}
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result['status'] == 'READY' else 2


if __name__ == '__main__':
    raise SystemExit(main())

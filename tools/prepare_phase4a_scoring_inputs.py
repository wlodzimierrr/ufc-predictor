"""Execute outcome-free Phase 4A; two independent rebuilds before freeze."""

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modeling.scoring_inputs import (
    OUTPUT_ROOT, PreflightError, json_bytes, prepare_payload, preservation_check, publish, sha256,
)
import json


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--authorization", type=Path, required=True)
    p.add_argument("--preservation-baseline", type=Path, required=True)
    p.add_argument("--tests-log", type=Path, required=True)
    p.add_argument("--commands-file", type=Path, required=True)
    args = p.parse_args()
    if args.output.exists() or args.output.parent.resolve() != OUTPUT_ROOT.resolve():
        p.error("Output must be a new isolated Phase 4A run; refusing overwrite")
    started = datetime.now(timezone.utc).isoformat()
    baseline_bytes = args.preservation_baseline.read_bytes()
    baseline = json.loads(baseline_bytes)
    preservation_check(baseline)
    first, validation = prepare_payload()
    second, second_validation = prepare_payload()
    if first != second or validation != second_validation:
        raise PreflightError("Independent deterministic rebuild differs")
    authorization = args.authorization.read_bytes()
    tests = args.tests_log.read_bytes()
    validation["deterministic_rebuild"] = {"passed": True, "independent_builds": 2,
        "byte_identical_files": len(first), "component_sha256": {k: sha256(v) for k, v in sorted(first.items())},
        "excludes": "wall-clock authorization/run receipt and preservation/test attachments"}
    validation["regression_log_sha256"] = sha256(tests)
    first["validation_results.json"] = json_bytes(validation)
    first["preservation_baseline.json"] = baseline_bytes
    first["preservation_check.json"] = json_bytes(preservation_check(baseline))
    first["regressions.log"] = tests
    first["commands.json"] = args.commands_file.read_bytes()
    first["user_authorization.txt"] = authorization
    first["run_receipt.json"] = json_bytes({"status": validation["status"], "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(), "authorization_sha256": sha256(authorization),
        "authorization": "deterministic repository-evidenced archived-as-of selection at original scored_at; no fitting, scoring or outcomes",
        "rebuild_parity": True, "promotion_authorized": False,
        "receipt_time_meaning": "preparation/publication time, never historical source availability"})
    checksums = publish(args.output, first)
    # Publication read-back uses hashes only, including protected baseline bytes.
    if any(sha256((args.output / k).read_bytes()) != v for k, v in checksums["files"].items()):
        raise PreflightError("Published read-back hashes differ")
    preservation_check(baseline)
    print(json.dumps({"output": str(args.output), "status": validation["status"],
        "target_rows": 108, "gaps": validation["gap_counts"],
        "checksums_sha256": sha256((args.output / "checksums.json").read_bytes())}, indent=2))


if __name__ == "__main__":
    main()

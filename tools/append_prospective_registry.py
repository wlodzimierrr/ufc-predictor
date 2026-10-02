"""Publish a new immutable successor registry run; metadata only, no scoring.

Input records contain registrations, captures, revisions or cancellations.
The parent run and all its old record bytes stay unchanged.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modeling.phase5_current_data import OUTPUT_ROOT, now, publish, verify_checksums
from modeling.prospective_registry import append_record, read_records, verify_coverage
from modeling.refit_preflight import PreflightError, json_bytes, sha256


def successor(parent: Path, parent_checksums: str, records: list[dict], output: Path) -> str:
    verify_checksums(parent, expected_checksums_sha256=parent_checksums)
    if output.exists():
        raise PreflightError("Successor registry must never overwrite a run")
    original = read_records(parent / "registry/records")
    with tempfile.TemporaryDirectory(prefix="phase5-registry-") as temp:
        journal = Path(temp) / "records"
        journal.mkdir()
        for path in (parent / "registry/records").glob("*.json"):
            shutil.copyfile(path, journal / path.name)
            (journal/path.name).chmod(0o444)
        for record in records:
            append_record(journal, record)
        chain = read_records(journal)
        expected = [r["record"]["registry_key"] for r in chain if r["record"]["kind"] == "registration"]
        verify_coverage(journal, expected)
        payloads = {"registry/records/" + p.name: p.read_bytes() for p in journal.glob("*.json")}
        payloads["registry/coverage.json"] = json_bytes({"registry_keys": expected,
            "considered_count": len(expected), "registered_count": len(expected),
            "appended_records": len(records), "preserved_parent_records": len(original),
            "scoring_executed": False, "outcomes_ingested": False})
        payloads["registry/tail_receipt.json"] = json_bytes({"sequence": len(chain),
            "tail_sha256": chain[-1]["sha256"] if chain else None})
        payloads["run_receipt.json"] = json_bytes({"kind": "append_only_registry_successor_v1",
            "parent_checksums_sha256": parent_checksums, "parent_run": str(parent),
            "published_at": now(), "metadata_only": True})
        return publish(output, payloads)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent-run", type=Path, required=True)
    parser.add_argument("--parent-checksums-sha256", required=True)
    parser.add_argument("--records-json", type=Path, required=True)
    parser.add_argument("--run-name", required=True)
    args = parser.parse_args()
    if Path(args.run_name).name != args.run_name or args.run_name in {".", ".."}:
        parser.error("run-name must be a new single directory name")
    records = json.loads(args.records_json.read_bytes())
    if not isinstance(records, list):
        parser.error("records-json must contain an array of metadata records")
    output = OUTPUT_ROOT / args.run_name
    digest = successor(args.parent_run, args.parent_checksums_sha256, records, output)
    print(json.dumps({"run": str(output), "checksums_sha256": digest}, indent=2))


if __name__ == "__main__":
    main()

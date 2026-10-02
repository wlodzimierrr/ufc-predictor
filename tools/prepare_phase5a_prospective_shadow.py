"""Capture/prepare Phase5A, or deterministically rebuild its frozen sources.

No estimator loading, fitting, probabilities, outcomes or warehouse writes.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modeling.phase5_current_data import (
    CODE_PATHS, OUTPUT_ROOT, ROOT, TABLES, VERSION, build_payloads, capture_warehouse,
    json_bytes, load_current_preparation, now, publish, sha256, verify_checksums, versions,
)
from modeling.holdout import load_holdout_fight_ids
from modeling.prospective_registry import verify_coverage
from modeling.refit_preflight import PreflightError


def reference_artifact_inputs() -> tuple[dict, dict]:
    pointer_path = ROOT / "models/production_model.json"
    pointer = json.loads(pointer_path.read_bytes())
    artifact = Path(pointer["artifact_path"])
    if not artifact.resolve().is_relative_to((ROOT / "models").resolve()):
        raise PreflightError("Production artifact is outside protected model root")
    metadata_path = artifact / "metadata.json"
    metadata = json.loads(metadata_path.read_bytes())
    # Hash base learner only; do not deserialize or run it.
    provenance = {"production_pointer_sha256": sha256(pointer_path.read_bytes()),
        "base_learner_path": str((artifact / "model.joblib").relative_to(ROOT)),
        "base_learner_sha256": sha256((artifact / "model.joblib").read_bytes()),
        "metadata_path": str(metadata_path.relative_to(ROOT)),
        "metadata_sha256": sha256(metadata_path.read_bytes()),
        "production_scorer_fits_platt_at_scoring_time": True,
        "production_scorer_persists_platt_estimator": False,
        "not_recovered_historical_probabilities": True,
        "reference_components_fit_in_phase5a": False,
        "freeze_in_phase5b_before_forecasts_or_outcomes": True}
    return metadata, {"reference_bootstrap/production_metadata.json": metadata_path.read_bytes(),
                      "reference_bootstrap/artifact_provenance.json": json_bytes(provenance)}


def targeted_official_reads(source_payloads: dict) -> dict[str, bytes]:
    """At most four ordinary requests, no discovery crawler or blocked retries.

    Retain exact response bodies and allowlisted response metadata. Never retain
    cookies, authorization headers or connection details. These pages are
    supplementary metadata; parsing/provenance attestation is separate work.
    """
    events = json.loads(source_payloads["sources/events.json"])
    fights = json.loads(source_payloads["sources/fights.json"])
    fighters = {r["fighter_id"]: r for r in json.loads(source_payloads["sources/fighters.json"])}
    future = sorted((e for e in events if e["event_date"] >= now()[:10]), key=lambda r: (r["event_date"], r["event_id"]))
    urls = []
    if future:
        event = future[0]
        urls.append(event["source_url"])
        bouts = sorted((f for f in fights if f["event_id"] == event["event_id"]), key=lambda r: r["fight_id"])
        if bouts:
            bout = bouts[0]
            urls.append(bout["source_url"])
            urls.extend(fighters.get(bout[k], {}).get("source_url") for k in ("fighter_1_id", "fighter_2_id"))
    records, payloads = [], {}
    for index, url in enumerate(dict.fromkeys(u for u in urls if u), 1):
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or parsed.hostname not in {"ufcstats.com", "www.ufcstats.com"} or parsed.username or parsed.password:
            raise PreflightError("Only allowlisted ordinary official-source URLs are permitted")
        if index > 1:
            time.sleep(2)
        record = {"url": url, "request_at": now(), "purpose": "targeted_upcoming_metadata_only"}
        request = Request(url, headers={"User-Agent": "UFC-data-shadow-research/1.0", "Accept-Encoding": "identity"})
        try:
            with urlopen(request, timeout=20) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
                record.update(status=response.status, response_url=response.url,
                    headers={k: v for k,v in response.headers.items() if k.lower() in {"content-type", "content-length", "date", "last-modified", "etag", "retry-after"}})
        except HTTPError as exc:
            raw = exc.read(4 * 1024 * 1024 + 1)
            record.update(status=exc.code, response_url=exc.url,
                headers={k: v for k,v in exc.headers.items() if k.lower() in {"content-type", "date", "retry-after"}})
        except (URLError, TimeoutError):
            raw = b""
            record.update(status=None, error="ordinary_request_failed_no_retry")
        record["response_at"] = now()
        if len(raw) > 4 * 1024 * 1024:
            raise PreflightError("Official response exceeds bounded capture; no truncated body published")
        name = f"official_sources/{index:02d}.response"
        record.update(body_path=name, body_sha256=sha256(raw), body_bytes=len(raw),
                      metadata_attestation="unparsed_not_used_to_resolve_eligibility")
        payloads[name] = raw
        records.append(record)
        if record.get("status") != 200 or any(x in raw.lower() for x in (b"captcha", b"access denied", b"checking your browser")):
            record["stopped_on_access_or_response_problem"] = True
            break
    payloads["official_sources/manifest.json"] = json_bytes({"maximum_requests": 4,
        "records": records, "no_bypass_or_retry": True,
        "meaning": "supplemental contemporary responses; not historical recovery or certified metadata"})
    return payloads


def preservation_check(baseline: dict) -> dict:
    changed, missing = [], []
    for name, digest in baseline["files"].items():
        path = ROOT / name
        if not path.is_file():
            missing.append(name)
        elif sha256(path.read_bytes()) != digest:
            changed.append(name)
    groups = ["models", "data/raw", "data/holdouts", "data/audits"]
    groups += [str(p.relative_to(ROOT)) for p in (ROOT / "data/experiments").iterdir() if p.name != "phase5a_prospective_shadow"]
    new = sorted(str(p.relative_to(ROOT)) for g in groups for p in (ROOT/g).rglob("*")
                 if p.is_file() and "__pycache__" not in p.parts and str(p.relative_to(ROOT)) not in baseline["files"])
    if changed or missing or new:
        raise PreflightError("Protected artifacts changed/missing/new")
    return {"status": "VERIFIED", "files_checked": len(baseline["files"]),
        "changed": changed, "missing": missing, "new_protected_files": new,
        "meaning": "hash-only preservation, no protected outcome parsing", "checked_at": now()}


def rebuild(directory: Path, root_hash: str) -> dict:
    checksums = verify_checksums(directory, expected_checksums_sha256=root_hash)
    source_names = json.loads((directory / "training_manifest.json").read_bytes())["source_sha256"]
    sources = {name: (directory/name).read_bytes() for name in source_names}
    metadata = json.loads((directory / "reference_bootstrap/production_metadata.json").read_bytes())
    config = json.loads((directory / "effective_configuration.json").read_bytes())
    receipt = json.loads((directory / "run_receipt.json").read_bytes())
    rebuilt = build_payloads(sources, metadata, config, receipt["coherent_capture_completed_at"])
    mismatches = [n for n,b in rebuilt.items() if sha256(b) != checksums.get(n)]
    if mismatches:
        raise PreflightError("Deterministic rebuild mismatch: " + ", ".join(mismatches))
    coverage = json.loads((directory / "registry/coverage.json").read_bytes())
    verify_coverage(directory / "registry/records", coverage["registry_keys"])
    return {"status": "VERIFIED", "deterministic_payloads_checked": len(rebuilt), "mismatches": mismatches}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    capture = sub.add_parser("capture")
    capture.add_argument("--run-name", required=True)
    capture.add_argument("--preservation-baseline", type=Path, required=True)
    capture.add_argument("--official-pages", action="store_true")
    capture.add_argument("--test-receipt", type=Path, required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--run", type=Path, required=True)
    verify.add_argument("--checksums-sha256", required=True)
    reprepare = sub.add_parser("reprepare")
    reprepare.add_argument("--parent-run", type=Path, required=True)
    reprepare.add_argument("--parent-checksums-sha256", required=True)
    reprepare.add_argument("--run-name", required=True)
    reprepare.add_argument("--test-receipt", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "verify":
        print(json.dumps(rebuild(args.run, args.checksums_sha256), indent=2))
        return
    if Path(args.run_name).name != args.run_name or args.run_name in {".", ".."}:
        parser.error("run-name must be a new single directory name")
    destination = OUTPUT_ROOT / args.run_name
    if destination.exists():
        parser.error("Existing runs must never be overwritten")
    started = now()
    if args.command == "reprepare":
        verify_checksums(args.parent_run, expected_checksums_sha256=args.parent_checksums_sha256)
        baseline_raw = (args.parent_run / "preservation_baseline.json").read_bytes()
    else:
        baseline_raw = args.preservation_baseline.read_bytes()
    baseline = json.loads(baseline_raw)
    preservation_check(baseline)
    config = json.loads((ROOT / "configs/phase5_prospective_shadow_v1.json").read_bytes())
    metadata, artifact_inputs = reference_artifact_inputs()
    # Import connection helper only in the explicit authorized capture command.
    from warehouse.db import get_connection
    parent_provenance = None
    if args.command == "reprepare":
        parent_manifest = json.loads((args.parent_run / "training_manifest.json").read_bytes())
        sources = {n: (args.parent_run/n).read_bytes() for n in parent_manifest["source_sha256"]}
        capture_receipt = json.loads(sources["source_capture_receipt.json"])
        completed = parent_manifest["capture_completed_at"]
        metadata = json.loads((args.parent_run / "reference_bootstrap/production_metadata.json").read_bytes())
        artifact_inputs = {n: (args.parent_run/n).read_bytes() for n in
                          ("reference_bootstrap/production_metadata.json", "reference_bootstrap/artifact_provenance.json")}
        parent_provenance = {"parent_run": str(args.parent_run), "parent_checksums_sha256": args.parent_checksums_sha256,
            "meaning": "offline preparation of identical captured bytes; no new warehouse/page access"}
    else:
        try:
            sources, capture_receipt = capture_warehouse(get_connection, metadata, load_holdout_fight_ids())
        except Exception as exc:
            # Never emit driver exception text or a credential-bearing traceback.
            parser.exit(1, f"Warehouse capture failed ({type(exc).__name__}); no run published; driver details suppressed.\n")
        if args.official_pages:
            sources.update(targeted_official_reads(sources))
        completed = now()
    first = build_payloads(sources, metadata, config, completed)
    second = build_payloads(sources, metadata, config, completed)
    if first != second:
        raise PreflightError("Independent deterministic rebuilds differed before publication")
    validation = {"deterministic_rebuild": "exact_bytes_match", "deterministic_payload_count": len(first),
        "no_fit_or_real_prediction_calls": True, "frozen_outcomes_accessed": False,
        "warehouse_read_only": capture_receipt["transaction_read_only"],
        "warehouse_rollback_and_close": capture_receipt["rolled_back"] and capture_receipt["transaction_closed"],
        "challenger": json.loads(first["training_manifest.json"])["summary"],
        "reference": json.loads(first["reference_bootstrap/manifest.json"]),
        "registry": json.loads(first["registry/coverage.json"])}
    payloads = {**sources, **first, **artifact_inputs,
        "preregistered_protocol.md": (ROOT / "docs/phase5-prospective-shadow-protocol-v1.md").read_bytes(),
        "package_versions.json": json_bytes(versions()),
        "code_versions.json": json_bytes({path: sha256((ROOT/path).read_bytes()) for path in CODE_PATHS}),
        "preservation_baseline.json": baseline_raw,
        "preservation_check.json": json_bytes(preservation_check(baseline)),
        "validation_results.json": json_bytes(validation),
        "synthetic_test_receipt.json": args.test_receipt.read_bytes(),
        "run_receipt.json": json_bytes({"contract_version": VERSION, "preparation_started_at": started,
            "coherent_capture_completed_at": completed, "preparation_completed_at": now(),
            "warehouse_capture_started_at": capture_receipt["capture_started_at"],
            "warehouse_capture_finished_at": capture_receipt["capture_finished_at"],
            "authorization": "user explicitly authorized prospective Phase5A current capture/preparation only",
            "model_fit": False, "prior_fit": False, "calibrator_fit": False,
            "real_probabilities": False, "outcome_evaluation": False,
            "historical_comparison": "STILL_BLOCKED", "production_changes": False})}
    if parent_provenance is not None:
        payloads["parent_capture_provenance.json"] = json_bytes(parent_provenance)
    root_hash = publish(destination, payloads)
    verification = rebuild(destination, root_hash)
    print(json.dumps({"run": str(destination), "checksums_sha256": root_hash,
        "training_manifest_sha256": sha256(payloads["training_manifest.json"]),
        "challenger_ready": validation["challenger"]["ready"],
        "reference_bootstrap_ready": validation["reference"]["ready"],
        "registry": validation["registry"], "verification": verification}, indent=2))


if __name__ == "__main__":
    main()

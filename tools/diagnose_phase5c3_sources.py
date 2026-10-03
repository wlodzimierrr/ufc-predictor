"""Read-only Phase 5C.3 diagnosis of pinned capture and existing local files.

Prints descriptive coverage and integrity checks, never an evidence assertion.
No database, HTTP, scraper, loader, feature or model APIs are imported or run.
Usage: PYTHONDONTWRITEBYTECODE=1 python3 tools/diagnose_phase5c3_sources.py
"""

import csv
import hashlib
import importlib.util
import io
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
import re
from uuid import NAMESPACE_URL, uuid5

ROOT = Path(__file__).resolve().parents[1]
SESSION = ROOT / "data/experiments/phase5c1_shadow_workflow/20261003T052316Z_phase5c2_real_intake_v1"
PINS = {
    "activation/checksums.json": "21672682c5a7096fdc94ac8344a90755186c4cfadd92f9cb91ee2d5e4d98adfd",
    "capture_attempt/observation/capture_receipt.json": "c09d3adf1065edda3582eabd9df6126d842bfa19e84ef7e222bddc384c246a18",
    "journal/runs/real_intake_v1/checksums.json": "33d8e983476c408ee7a67f785c27856d58b3a0c9a0a3ea3fc2373a5bbe61d81c",
}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def url_id(url):
    # Same www-only normalization as the existing spider; HTTP/HTTPS stay distinct.
    return str(uuid5(NAMESPACE_URL, re.sub(r"(?<=://)www\.", "", url)))


def diagnose():
    inputs = {}

    def read(path):
        raw = path.read_bytes()
        inputs[str(path.relative_to(ROOT))] = sha(raw)
        return raw

    def csv_rows(relative):
        return list(csv.DictReader(io.StringIO(read(ROOT / relative).decode("utf-8"))))

    for rel, pin in PINS.items():
        if sha(read(SESSION / rel)) != pin:
            raise ValueError("Accepted artifact pin mismatch: " + rel)
    inventories = {}
    for rel in ("activation", "capture_attempt/observation", "journal/runs/real_intake_v1"):
        directory = SESSION / rel
        files = json.loads(read(directory / "checksums.json"))["files"]
        actual = {str(p.relative_to(directory)) for p in directory.rglob("*") if p.is_file()}
        if actual != set(files) | {"checksums.json"}:
            raise ValueError("Inventory differs: " + rel)
        for name, digest in files.items():
            if sha((directory / name).read_bytes()) != digest:
                raise ValueError("Inventory hash mismatch: " + name)
        inventories[rel] = len(files)

    observation = SESSION / "capture_attempt/observation"
    receipt = json.loads(read(observation / "capture_receipt.json"))
    tables = {n: json.loads(read(observation / e["body"])) for n, e in receipt["tables"].items()}
    events = {r["event_id"]: r for r in tables["events"]}
    fights = {r["fight_id"]: r for r in tables["fights"]}
    profiles = {r["fighter_id"]: r for r in tables["fighters"]}
    event_date = lambda fid: events[fights[fid]["event_id"]]["event_date"]
    queue = csv_rows("data/manifests/fight_stats_queue.csv")
    fetches = csv_rows("data/manifests/fetch_manifest.csv")
    local_fights = csv_rows("data/fights.csv")
    aggregate = csv_rows("data/fight_stats.csv")
    rounds = csv_rows("data/fight_stats_by_round.csv")
    local_events = csv_rows("data/events.csv")
    manifest_events = csv_rows("data/manifests/events_manifest.csv")
    coverage = csv_rows("data/reports/stats_coverage.csv")
    aggregate_ids = {r["fight_id"] for r in aggregate}
    round_ids = {r["fight_id"] for r in rounds}
    queued_ids = {r["fight_id"] for r in queue}
    captured = {url_id(r["source_url"]) for r in fetches
                if r["fetch_status"] in {"fetched", "unchanged", "updated"} and r["source_url"]}
    cutoff_date = receipt["observation_cutoff"][:10]
    expired = [r for r in fights.values() if r["result_type"] == "upcoming"
               and event_date(r["fight_id"]) < cutoff_date]
    by_id = defaultdict(list)
    for row in local_fights:
        by_id[row["fight_id"]].append(row)
    # Only the pure scalar transform module; no connection helper or loader import.
    transform_path = ROOT / "warehouse/transform.py"
    read(transform_path)
    spec = importlib.util.spec_from_file_location("phase5c3_scalar_transforms", transform_path)
    transforms = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(transforms)

    def scalar_row(row):
        return {k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items()}

    captured_stats = {r["fight_stat_id"]: r for r in tables["fight_stats_aggregate"]}
    aggregate_matches = sum(captured_stats.get(r["fight_stat_id"]) == scalar_row(transforms.transform_fight_stat(r))
                            for r in aggregate)
    cancelled_nc = []
    for fid, versions in by_id.items():
        latest = max(enumerate(versions), key=lambda pair: (pair[1]["scraped_at"], pair[0]))[1]
        if latest["event_status"] == "canceled" and not latest["fighter_1_outcome"] and not latest["fighter_2_outcome"]:
            cancelled_nc.append({"fight_id": fid, "event_date": event_date(fid) if fid in fights else None,
                                 "captured_result_type": fights.get(fid, {}).get("result_type"),
                                 "whole_transformed_row_matches_capture": scalar_row(transforms.transform_fight(latest)) == fights.get(fid),
                                 "local_scraped_at": latest["scraped_at"]})

    # Diagnose overwritten manifest references without exporting response bodies.
    path_hashes = {}
    body_states = Counter()
    for row in fetches:
        if row["fetch_status"] not in {"fetched", "unchanged", "updated"}:
            continue
        rel = row["storage_path"]
        path = ROOT / rel
        if path.is_absolute() and path.is_relative_to(ROOT) and ".." not in Path(rel).parts:
            if rel not in path_hashes:
                path_hashes[rel] = sha(path.read_bytes()) if path.is_file() else None
            body_states["missing" if path_hashes[rel] is None else
                        "hash_matches" if path_hashes[rel] == row["content_hash"] else "hash_differs"] += 1
        else:
            raise ValueError("Unsafe manifest path")

    examples = []
    for fid in ("6fe1d59a-6ae9-5436-bc78-767da12a4707", "129b49aa-9be9-5739-a710-4e63b5254405"):
        target = fights[fid]
        paths = [ROOT / "data/raw/ufcstats/fights" / (fid + ".html")]
        paths += [ROOT / "data/raw/ufcstats/fighters" / (target[f"fighter_{s}_id"] + ".html") for s in (1, 2)]
        bodies = []
        for path in paths:
            if not path.is_file():
                bodies.append({"path": str(path.relative_to(ROOT)), "present": False})
                continue
            raw = read(path)
            matches = [r for r in fetches if r["storage_path"] == str(path.relative_to(ROOT))
                       and r["content_hash"] == sha(raw)]
            bodies.append({"path": str(path.relative_to(ROOT)), "sha256": sha(raw),
                           "last_matching_fetch": max((r["fetched_at"] for r in matches), default=None),
                           "distinct_fight_detail_links": len(set(re.findall(rb"fight-details/([a-z0-9]+)", raw)))})
        examples.append({"fight_id": fid, "bodies": bodies})

    return {
        "purpose": "OFFLINE_DIAGNOSIS_ONLY_NOT_TRUSTED_EVIDENCE",
        "accepted_pins": PINS, "verified_inventory_file_counts": inventories,
        "input_sha256": inputs, "observation_cutoff": receipt["observation_cutoff"],
        "captured": {"rows": {n: len(r) for n, r in tables.items()},
                     "last_event_date_by_result": {k: max(event_date(r["fight_id"]) for r in fights.values()
                                                           if r["result_type"] == k) for k in ("win", "draw", "nc")},
                     "aggregate_last_event_date": max(event_date(r["fight_id"]) for r in tables["fight_stats_aggregate"]),
                     "aggregate_scraped_at_range": [min(r["scraped_at"] for r in tables["fight_stats_aggregate"]),
                                                    max(r["scraped_at"] for r in tables["fight_stats_aggregate"])],
                     "expired_announcements": len(expired),
                     "expired_by_date": dict(Counter(event_date(r["fight_id"]) for r in expired)),
                     "expired_by_url_scheme": dict(Counter((r["source_url"] or "NULL").split(":")[0] for r in expired))},
        "local": {"event_csv_rows": len(local_events), "event_manifest_rows": len(manifest_events),
                  "fight_csv_rows": len(local_fights), "aggregate_rows": len(aggregate), "round_rows": len(rounds),
                  "aggregate_unique_fights": len(aggregate_ids), "round_unique_fights": len(round_ids),
                  "aggregate_whole_transformed_rows_matching_capture": aggregate_matches,
                  "round_last_event_date": max(event_date(fid) for fid in round_ids if fid in fights),
                  "queue_rows": len(queue), "queue_without_aggregate": len(queued_ids - aggregate_ids),
                  "queue_without_round": len(queued_ids - round_ids),
                  "aggregate_requests_after_current_incremental_filter": sum(url_id(r["fight_url"]) not in aggregate_ids | captured for r in queue),
                  "round_requests_after_current_incremental_filter": sum(url_id(r["fight_url"]) not in round_ids for r in queue),
                  "missing_aggregate_skipped_by_manifest": len((queued_ids - aggregate_ids) & captured),
                  "round_only_fights": len(round_ids - aggregate_ids),
                  "round_only_by_date": dict(Counter(event_date(fid) for fid in round_ids - aggregate_ids if fid in fights)),
                  "queue_unknown_captured_fight_ids": sorted(queued_ids - set(fights)),
                  "queue_last_event_date": max(events[r["event_id"]]["event_date"] for r in queue if r["event_id"] in events),
                  "stats_coverage_report_rows": len(coverage),
                  "aggregate_unknown_fight_rows": sum(r["fight_id"] not in fights for r in aggregate),
                  "round_unknown_fight_rows": sum(r["fight_id"] not in fights for r in rounds),
                  "fight_unknown_event_rows": sum(r["event_id"] not in events for r in local_fights),
                  "fight_unknown_participant_rows": sum(r["fighter_1_id"] not in profiles or r["fighter_2_id"] not in profiles for r in local_fights),
                  "captured_fight_ids_absent_from_local_csv": len(set(fights) - set(by_id)),
                  "csv_ids_with_both_upcoming_and_completed": sum({"upcoming", "completed"} <= {r["event_status"] for r in v} for v in by_id.values()),
                  "cancelled_empty_outcomes_with_captured_disposition": cancelled_nc},
        "fetch_manifest": {"rows": len(fetches), "successful_reference_body_states": dict(body_states),
                           "failures_since_september": [dict(zip(("date", "entity_type", "http_status", "error", "count"), (*k, v)))
                               for k, v in sorted(Counter((r["fetched_at"][:10], r["entity_type"], r["http_status"], r["error_message"])
                                                     for r in fetches if r["fetch_status"] == "failed" and r["fetched_at"][:10] >= "2026-09-01").items())]},
        "example_source_bodies": examples,
    }


if __name__ == "__main__":
    print(json.dumps(diagnose(), sort_keys=True, indent=2))

"""Bounded source/warehouse audit. SELECT only in a read-only capture.

Does not read holdout outcomes, prediction-review joins, or model weights.
"""

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modeling.refit_preflight import ROOT, json_bytes, load_git_source, sha256, validate_source
from modeling.holdout import load_holdout_fight_ids
from warehouse.db import get_connection

TABLES = ("events", "fighters", "fights", "fight_stats_aggregate", "fight_stats_by_round",
          "fighter_snapshots", "bout_features")


def capture(*, cutoff: str, knowledge_cutoff: str, source_ref: str) -> dict:
    data, archive, _ = load_git_source(source_ref=source_ref, knowledge_cutoff=knowledge_cutoff)
    validate_source(data)
    excluded = sorted(load_holdout_fight_ids())
    evidence = {"archive": archive, "queries": {}, "scope": "source schemas and pre-cutoff history only"}
    c = get_connection()
    c.set_session(readonly=True, isolation_level="REPEATABLE READ")
    try:
        with c.cursor() as q:
            def query(name, sql, params=()):
                q.execute(sql, params)
                rows = [dict(zip([d[0] for d in q.description], row)) for row in q.fetchall()]
                evidence["queries"][name] = {"sql": sql, "parameters": params, "rows": rows}
                return rows
            query("capture", "SELECT transaction_timestamp() AS captured_at, "
                  "current_setting('transaction_read_only') AS read_only, "
                  "current_setting('TimeZone') AS timezone, version() AS server_version")
            query("schema", "SELECT table_name,column_name,data_type,is_nullable "
                  "FROM information_schema.columns WHERE table_schema='public' "
                  "AND table_name=ANY(%s) ORDER BY table_name,ordinal_position", (list(TABLES),))
            for table in TABLES:
                time = "computed_at" if table in ("fighter_snapshots", "bout_features") else "scraped_at"
                query(table + "_times", f"SELECT count(*) AS rows, count({time}) AS timestamped_rows, "
                      f"min({time}) AS earliest, max({time}) AS latest, "
                      f"count(*) FILTER (WHERE {time} < %s) AS timestamp_before_knowledge_cutoff FROM {table}",
                      (knowledge_cutoff,))
            query("pre_cutoff_results", "SELECT result_type,count(*) AS rows,min(e.event_date) AS earliest, "
                  "max(e.event_date) AS latest FROM fights f JOIN events e USING(event_id) "
                  "WHERE e.event_date < %s AND NOT(f.fight_id::text=ANY(%s)) GROUP BY 1 ORDER BY 1", (cutoff, excluded))
            query("pre_cutoff_stat_coverage", "SELECT count(*) AS bouts, count(*) FILTER(WHERE n=2) AS two_stat_rows, "
                  "count(*) FILTER(WHERE n=0) AS missing_stats FROM (SELECT f.fight_id,count(s.fight_stat_id) AS n "
                  "FROM fights f JOIN events e USING(event_id) LEFT JOIN fight_stats_aggregate s USING(fight_id) "
                  "WHERE e.event_date<%s AND NOT(f.fight_id::text=ANY(%s)) GROUP BY f.fight_id) t", (cutoff, excluded))
            query("pre_cutoff_features", "SELECT feature_version,count(*) AS rows,count(label) AS labeled_rows, "
                  "min(computed_at) AS earliest_computed_at,max(computed_at) AS latest_computed_at "
                  "FROM bout_features WHERE event_date<%s GROUP BY 1", (cutoff,))
            query("same_day_snapshot_elo", "SELECT count(*) AS inconsistent_fighter_dates FROM "
                  "(SELECT fighter_id,as_of_date FROM fighter_snapshots WHERE as_of_date<%s "
                  "GROUP BY fighter_id,as_of_date HAVING count(*)>1 AND min(elo_rating)<>max(elo_rating)) t", (cutoff,))
            current = query("current_pre_cutoff_bouts", "SELECT f.fight_id::text,f.event_id::text,e.event_date, "
                  "f.fighter_1_id::text,f.fighter_2_id::text,f.winner_fighter_id::text,f.result_type "
                  "FROM fights f JOIN events e USING(event_id) WHERE e.event_date<%s "
                  "AND NOT(f.fight_id::text=ANY(%s)) ORDER BY e.event_date,f.fight_id", (cutoff, excluded))
            current_profiles = query("profile_comparison", "SELECT fighter_id::text,dob,height_cm,reach_cm,stance "
                                     "FROM fighters WHERE fighter_id::text=ANY(%s) ORDER BY fighter_id",
                                     (sorted(data.fighter_by_id),))
            old = data.fight_by_id
            evidence["archive_vs_warehouse"] = {
                "warehouse_only_pre_cutoff_bouts": [r["fight_id"] for r in current if r["fight_id"] not in old],
                "archive_only_bouts": sorted(set(old) - {r["fight_id"] for r in current}),
                "changed_result_or_orientation": [r["fight_id"] for r in current if r["fight_id"] in old and any(
                    r[k] != old[r["fight_id"]][k] for k in ("fighter_1_id", "fighter_2_id", "winner_fighter_id", "result_type"))],
                "mutable_profile_field_changes": {key: sum(
                    (round(float(r[key]), 2) if key in ("height_cm", "reach_cm") and r[key] is not None else r[key]) !=
                    (round(data.fighter_by_id[r["fighter_id"]][key], 2)
                     if key in ("height_cm", "reach_cm") and data.fighter_by_id[r["fighter_id"]][key] is not None
                     else data.fighter_by_id[r["fighter_id"]][key]) for r in current_profiles)
                                                   for key in ("dob", "height_cm", "reach_cm", "stance")},
                "profiles_compared": len(current_profiles),
                "physical_comparison_precision": "2 decimals to match warehouse numeric scale"}
            evidence["archive_vs_warehouse"]["warehouse_only_date_results"] = dict(Counter(
                f"{r['event_date']}:{r['result_type']}" for r in current if r["fight_id"] not in old))
            # Only comparison aggregates are needed; don't publish duplicate source records.
            evidence["queries"]["current_pre_cutoff_bouts"]["rows_sha256"] = sha256(
                json_bytes(json.loads(json.dumps(current, default=str))))
            evidence["queries"]["current_pre_cutoff_bouts"].pop("rows")
            evidence["queries"]["profile_comparison"].pop("rows")
    finally:
        c.rollback()
        c.close()
    evidence["current_csvs"] = {}
    for name in ("events", "fights", "fighters", "fight_stats", "fight_stats_by_round"):
        path = ROOT / "data" / (name + ".csv")
        rows = list(csv.DictReader(path.open()))
        times = [r["scraped_at"] for r in rows if r.get("scraped_at")]
        evidence["current_csvs"][name] = {"sha256": sha256(path.read_bytes()), "rows": len(rows),
                                         "scraped_at_min": min(times), "scraped_at_max": max(times)}
    manifest = ROOT / "data/manifests/fetch_manifest.csv"
    by_entity = defaultdict(list)
    cutoff_time = datetime.fromisoformat(knowledge_cutoff)
    with manifest.open() as f:
        for row in csv.DictReader(f):
            if row["fetch_status"] != "failed" and row["storage_path"] and datetime.fromisoformat(
                    row["fetched_at"].replace("Z", "+00:00")) < cutoff_time:
                by_entity[row["entity_type"]].append(row)
    evidence["raw_pre_cutoff_captures"] = {"manifest_sha256": sha256(manifest.read_bytes()), "entities": {}}
    for entity, rows in by_entity.items():
        # Inspect only paths named in pre-cutoff manifests; never inventory caches.
        by_path = defaultdict(set)
        for row in rows:
            by_path[row["storage_path"]].add(row["content_hash"])
        matched = []
        for path, hashes in by_path.items():
            p = (ROOT / path).resolve()
            if p.is_relative_to(ROOT / "data/raw/ufcstats") and p.is_file() and sha256(p.read_bytes()) in hashes:
                matched.append(path)
        evidence["raw_pre_cutoff_captures"]["entities"][entity] = {
            "successful_manifest_rows": len(rows), "unique_urls": len({r["source_url"] for r in rows}),
            "unique_storage_paths": len(by_path), "currently_recoverable_hash_matching_paths": len(matched),
            "recoverable_paths": sorted(matched)}
    evidence["git_csv_history"] = subprocess.check_output(
        ["git", "log", "-8", "--format=%H %cI %s", "--", "data/events.csv", "data/fights.csv",
         "data/fighters.csv", "data/fight_stats.csv", "data/fight_stats_by_round.csv"], cwd=ROOT, text=True).splitlines()
    return json.loads(json.dumps(evidence, default=str))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--event-cutoff", required=True)
    p.add_argument("--knowledge-cutoff", required=True)
    p.add_argument("--source-ref", required=True)
    a = p.parse_args()
    if a.output.exists():
        p.error("Audit output already exists")
    evidence = capture(cutoff=a.event_cutoff, knowledge_cutoff=a.knowledge_cutoff, source_ref=a.source_ref)
    with a.output.open("xb") as f:
        f.write(json_bytes(evidence))
    print(json.dumps({"output": str(a.output), "sha256": sha256(a.output.read_bytes()),
                      "archive_vs_warehouse": evidence["archive_vs_warehouse"],
                      "raw_counts": {k: {j:v for j,v in r.items() if j != 'recoverable_paths'}
                                     for k,r in evidence['raw_pre_cutoff_captures']['entities'].items()}}, indent=2))


if __name__ == "__main__":
    main()

"""Offline Phase 4A preparation. No fitting, prediction, evaluation or DB access.

Git trees and hash-matched raw bodies are the only reconstruction sources.
Publication is all-or-nothing: essential gaps produce a frozen BLOCKED receipt,
never a reduced scoring cohort. Existing Phase 3 preparation stays unchanged.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
import io
import json
from pathlib import Path
import re
import subprocess
from uuid import NAMESPACE_URL, uuid5

import numpy as np
import pandas as pd

from features.data_loader import WarehouseData
from features.forecast_replay import DATE_SEMANTICS, reconstruct_forecast, utc_instant
from features.replay import index_source
from modeling.refit_preflight import (
    ALGORITHM_VERSION, DEBUT_COLS, FEATURE_ORDER, FEATURE_VERSION, ROOT,
    SOURCE_FILES, PreflightError, json_bytes, sha256, transform_archived_fight,
    validate_source,
)
from modeling.xgb_candidate_bundle import verify_checksums
from modeling.xgb_candidate_contract import MANIFEST_SHA256, feature_provenance, package_versions
from warehouse.transform import (
    _extract_bout_flags, transform_event, transform_fighter, transform_fight_stat,
)

HOLDOUT = ROOT / "data/holdouts/pre_event_2026_apr_aug"
CANDIDATE = ROOT / "data/experiments/phase3b_xgb_pre_april_2026/20261001_phase3b_retrospective_v1"
OUTPUT_ROOT = ROOT / "data/experiments/phase4a_pre_event_2026_scoring_inputs"
PINS = {
    "candidate_checksums": "41f8c8afb8fd091fa0b04823ad9175cedcfdbf85ccf3f62d77c71e0d9219279f",
    "forecast_manifest": "f28c9b052dbe883d05c00d3b46fe711997dfa39c7fca09d8a722c6fc2fb36f6a",
    "forecast_predictions": "81e133bcb88422d3e0ddae12ac79de4ce6b9f60ab4d009545d7bfb41ff1a522b",
    "production_pointer": "ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa",
}
TARGET_COLUMNS = [
    "fight_id", "event_id", "fighter_1_id", "fighter_2_id", "event_date", "scored_at",
    "scored_at_original", "fighter_1_name", "fighter_2_name", "event_name", "weight_class",
    "pre_event_evidence", "prediction_dir_mtime", "prediction_file", "model_name",
    "model_artifact", "prediction_source",
]
IDENTITY_COLUMNS = ["fight_id", "event_id", "fighter_1_id", "fighter_2_id", "event_date"]
FEATURE_METADATA = IDENTITY_COLUMNS + [
    "scored_at", "weight_class", "feature_version", "history_date_cutoff_exclusive",
    "feature_reference_date", "source_commit",
]
SCHEMA_REQUIRED = {
    "events": {"scraped_at", "event_id", "name", "date_formatted", "city", "state", "country"},
    "fighters": {"scraped_at", "fighter_id", "full_name", "height_cm", "reach_cm", "stance", "dob_formatted"},
    "fights": {"scraped_at", "fight_id", "event_id", "fighter_1_id", "fighter_2_id", "fighter_1_outcome",
               "fighter_2_outcome", "bout_type", "num_rounds", "finish_round", "finish_time_minute", "finish_time_second"},
    "fight_stats": {"scraped_at", "fight_stat_id", "fight_id", "fighter_id", "significant_strikes_landed",
                    "significant_strikes_attempted", "takedowns_landed", "takedowns_attempted"},
}
POLICY = {
    "version": "phase4a_source_v1", "mode": "qualified_archived_asof",
    "git_search": "all reachable commits affecting exactly four source paths; maximum 256 trees",
    "coherence": "one tree; required CSV schema, unique IDs/stat pairs, valid event/profile/participant references; orphan stats reported and unused",
    "availability": "max(author UTC, committer UTC); repository evidence, not independently trusted",
    "selection": "maximum (availability UTC, full commit SHA) among coherent schema-compatible trees <= scored_at",
    "fallback": "latest eligible coherent tree; rejected trees recorded; never mix Git components",
    "supplementation": "only missing target profiles and target flags; relevant fighter/fight/event fetch records, HTTP 200, successful capture <= scored_at, URL identity and exact surviving body hash",
    "raw_tie_break": "maximum (fetched_at UTC, content_hash, storage_path, job_run_id)",
    "raw_bounds": "only original target fighter/fight/event paths; at most 200000 manifest rows and 20000 relevant records",
    "experience": "new supplemental profile with no archived eligible history is an essential unresolved experience gap",
    "target_flags": "nonempty explicit recognized bout type for original event and fighter pair; absent title indication alone is unknown",
    "completeness": "not certified; report repository observation, scrape ranges, last resolved date, age and missing statistics",
    "outcome_independence": "metadata allowlist only; baseline probabilities, decisions, winners and performance ignored",
    "publication": "all 108 essential inputs required before any real feature construction; BLOCKED runs contain no features.csv",
}


def read_csv(raw: bytes) -> tuple[list[str], list[dict]]:
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
    columns = reader.fieldnames or []
    rows = list(reader)
    if not columns or len(columns) != len(set(columns)) or any(None in r or None in r.values() for r in rows):
        raise PreflightError("Malformed CSV schema or rows")
    return columns, rows


def csv_bytes(rows: list[dict], columns: list[str]) -> bytes:
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, fieldnames=columns, lineterminator="\n", extrasaction="raise")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode()


def load_targets(directory: Path = HOLDOUT) -> tuple[list[dict], dict]:
    """Parse only predictions/label-free identities; manifest summaries unused."""
    raw = (directory / "manifest.json").read_bytes()
    predictions = (directory / "predictions.csv").read_bytes()
    if sha256(raw) != PINS["forecast_manifest"] or sha256(predictions) != PINS["forecast_predictions"]:
        raise PreflightError("Frozen forecast pins differ")
    manifest = json.loads(raw)
    identity_raw = (directory / "identity-exclusions.json").read_bytes()
    entry = manifest["files"]["identity_exclusions"]
    if entry["path"] != "identity-exclusions.json" or sha256(identity_raw) != entry["sha256"]:
        raise PreflightError("Identity evidence pin differs")
    identities = {r["fight_id"]: {k: r[k] for k in IDENTITY_COLUMNS}
                  for r in json.loads(identity_raw)["identities"]}
    columns, original = read_csv(predictions)
    required = set(TARGET_COLUMNS) - {"scored_at_original"}
    if not required <= set(columns) or len(original) != 108:
        raise PreflightError("Expected exactly 108 forecasts with required metadata")
    targets = []
    for r in original:
        # The only columns crossing into source selection/construction.
        t = {k: r[k] for k in TARGET_COLUMNS if k != "scored_at_original"}
        if {k: t[k] for k in IDENTITY_COLUMNS} != identities.get(t["fight_id"]):
            raise PreflightError("Original identity/orientation differs from evidence")
        if any(not t[k].strip() for k in IDENTITY_COLUMNS + ["weight_class"]):
            raise PreflightError("Missing essential forecast identity/metadata")
        t["scored_at_original"] = t["scored_at"]
        t["scored_at"] = utc_instant(t["scored_at"]).isoformat()
        if utc_instant(t["scored_at_original"]) != utc_instant(t["scored_at"]):
            raise PreflightError("Scoring instant changed")
        if utc_instant(t["scored_at"]).date() >= date.fromisoformat(t["event_date"]):
            raise PreflightError("Forecast is not pre-event")
        targets.append(t)
    if len({r["fight_id"] for r in targets}) != 108 or any(r["fighter_1_id"] == r["fighter_2_id"] for r in targets):
        raise PreflightError("Duplicate forecast or invalid orientation")
    return targets, {"pins": PINS, "metadata_allowlist": TARGET_COLUMNS,
                     "identity_evidence_sha256": sha256(identity_raw), "rows": 108,
                     "events": len({t["event_id"] for t in targets}),
                     "ordering": "exact frozen predictions.csv row order",
                     "orientation_and_instants_verified": True}


def git(*args: str, repo: Path = ROOT) -> bytes:
    return subprocess.check_output(["git", *args], cwd=repo, stderr=subprocess.PIPE)


def resolve_blob(entry: dict, repo: Path = ROOT) -> bytes:
    """Fail closed on both immutable object identity and SHA-256."""
    blob = entry["blob"]
    if not re.fullmatch(r"[0-9a-f]{40}", blob):
        raise PreflightError("Invalid immutable blob ID")
    if git("rev-parse", f'{entry["commit"]}:{entry["git_path"]}', repo=repo).decode().strip() != blob:
        raise PreflightError("Commit/blob association differs")
    raw = git("cat-file", "blob", blob, repo=repo)
    if sha256(raw) != entry["sha256"]:
        raise PreflightError("Resolved source hash differs")
    return raw


@dataclass
class Snapshot:
    record: dict
    data: WarehouseData | None
    rows: dict[str, list[dict]]


def decode_snapshot(raws: dict[str, bytes]) -> tuple[WarehouseData, dict, dict]:
    rows, summaries = {}, {}
    for name, raw in raws.items():
        columns, values = read_csv(raw)
        if not SCHEMA_REQUIRED[name] <= set(columns):
            raise PreflightError(f"Missing required source schema: {name}")
        rows[name] = values
        scrapes = [r["scraped_at"] for r in values if r["scraped_at"]]
        summaries[name] = {"rows": len(values), "scraped_at_min": min(scrapes, default=None),
                           "scraped_at_max": max(scrapes, default=None), "columns": columns}
    for r in rows["fights"]:
        pair = (r["fighter_1_outcome"], r["fighter_2_outcome"])
        if pair not in {("W", "L"), ("L", "W"), ("D", "D"), ("NC", "NC")} and not (
                pair == ("", "") and r.get("event_status") == "upcoming"):
            raise PreflightError("Unrecognized archived result encoding")
    data = WarehouseData(events=[transform_event(r) for r in rows["events"]],
                         fighters=[transform_fighter(r) for r in rows["fighters"]],
                         fights=[transform_archived_fight(r) for r in rows["fights"]],
                         fight_stats=[transform_fight_stat(r) for r in rows["fight_stats"]])
    event_ids = {r["event_id"] for r in data.events}
    profile_ids = {r["fighter_id"] for r in data.fighters}
    broken_events = sum(f["event_id"] not in event_ids for f in data.fights)
    broken_profiles = sum(f[k] not in profile_ids for f in data.fights for k in ("fighter_1_id", "fighter_2_id"))
    if broken_events or broken_profiles:
        raise PreflightError(f"Incoherent tree: {broken_events} missing event references; {broken_profiles} missing participant profiles")
    fight_ids = {f["fight_id"] for f in data.fights}
    all_stat_ids = [s["fight_stat_id"] for s in data.fight_stats]
    all_pairs = [(s["fight_id"], s["fighter_id"]) for s in data.fight_stats]
    if len(set(all_stat_ids)) != len(all_stat_ids) or len(set(all_pairs)) != len(all_pairs):
        raise PreflightError("Duplicate statistics in source")
    orphan_stats = [s for s in data.fight_stats if s["fight_id"] not in fight_ids]
    data.fight_stats = [s for s in data.fight_stats if s["fight_id"] in fight_ids]
    validate_source(data)
    resolved = [f for f in data.fights if f["result_type"] in {"win", "draw", "nc"}]
    summary = {"components": summaries, "orphan_statistics_unused": len(orphan_stats),
               "last_resolved_event_date": max((f["event_date"].isoformat() for f in resolved), default=None),
               "resolved_fights_missing_any_participant_statistics": sum(
                   any((f["fight_id"], f[k]) not in data.stats_by_fight_fighter for k in ("fighter_1_id", "fighter_2_id")) for f in resolved),
               "complete": False, "limitation": "sparse coherent archive; no certified complete historical/source coverage"}
    return data, rows, summary


def source_structure(raws: dict[str, bytes]) -> dict:
    """Describe rejected archives too, without resolving duplicate revisions."""
    result, tables = {}, {}
    for name, raw in raws.items():
        columns, rows = read_csv(raw)
        tables[name] = rows
        keys = ("fight_id", "fighter_id") if name == "fight_stats" else (name[:-1] + "_id",)
        # fighters -> fighter_id, events -> event_id, fights -> fight_id.
        identities = [tuple(r.get(k) for k in keys) for r in rows]
        counts = {}
        for key in identities:
            counts[key] = counts.get(key, 0) + 1
        scrapes = [r.get("scraped_at") for r in rows if r.get("scraped_at")]
        result[name] = {"rows": len(rows), "columns": columns,
                        "duplicate_identity_keys": sum(n > 1 for n in counts.values()),
                        "duplicate_excess_rows": sum(n - 1 for n in counts.values()),
                        "scraped_at_min": min(scrapes, default=None), "scraped_at_max": max(scrapes, default=None)}
    event_ids = {r["event_id"] for r in tables["events"]}
    fighter_ids = {r["fighter_id"] for r in tables["fighters"]}
    result["missing_event_references"] = sum(r["event_id"] not in event_ids for r in tables["fights"])
    result["missing_participant_profile_references"] = sum(r[k] not in fighter_ids for r in tables["fights"] for k in ("fighter_1_id", "fighter_2_id"))
    return result


def discover_snapshots(repo: Path = ROOT) -> list[Snapshot]:
    commits = git("log", "--all", "--format=%H", "--", *SOURCE_FILES.values(), repo=repo).decode().splitlines()
    if not commits or len(commits) > 256:
        raise PreflightError("Source search empty or exceeds 256-tree bound")
    snapshots = []
    for commit in sorted(set(commits)):
        author, committer = git("show", "-s", "--format=%aI%n%cI", commit, repo=repo).decode().splitlines()
        record = {"commit": commit, "author_time": author, "committer_time": committer,
                  "availability_utc": max(utc_instant(author), utc_instant(committer)).isoformat(),
                  "evidence_kind": "repository_git_time", "independently_trusted": False, "files": {}}
        raws = {}
        try:
            for name, path in SOURCE_FILES.items():
                blob = git("rev-parse", f"{commit}:{path}", repo=repo).decode().strip()
                raw = git("cat-file", "blob", blob, repo=repo)
                entry = {"commit": commit, "git_path": path, "blob": blob, "sha256": sha256(raw)}
                if resolve_blob(entry, repo=repo) != raw:
                    raise PreflightError("Git resolver parity failed")
                record["files"][name] = entry
                raws[name] = raw
            record["structure"] = source_structure(raws)
            data, rows, summary = decode_snapshot(raws)
            record.update(coherent=True, summary=summary)
        except (ValueError, KeyError, TypeError, subprocess.CalledProcessError) as exc:
            record.update(coherent=False, rejection_reason=str(exc))
            data, rows = None, {}
        snapshots.append(Snapshot(record, data, rows))
    return snapshots


def select_snapshot(snapshots: list[Snapshot], scored_at: str) -> Snapshot | None:
    eligible = [s for s in snapshots if s.record["coherent"] and
                utc_instant(s.record["availability_utc"]) <= utc_instant(scored_at)]
    return max(eligible, key=lambda s: (utc_instant(s.record["availability_utc"]), s.record["commit"]), default=None)


def url_id(url: str) -> str:
    return str(uuid5(NAMESPACE_URL, re.sub(r"(?<=://)www\.", "", url)))


class MetadataHTML(HTMLParser):
    """Store only explicit bout title, URL identities and physical profile fields."""
    def __init__(self):
        super().__init__()
        self.links, self.titles, self.profile_items = [], [], []
        self.capture, self.buffer = None, []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "a" and attrs.get("href"):
            self.links.append(attrs["href"])
        if tag == "i" and "b-fight-details__fight-title" in attrs.get("class", "").split():
            self.capture, self.buffer = "title", []
        if tag == "li" and "b-list__box-list-item" in attrs.get("class", "").split():
            self.capture, self.buffer = "profile", []

    def handle_data(self, text):
        if self.capture:
            self.buffer.append(text)

    def handle_endtag(self, tag):
        if (tag == "i" and self.capture == "title") or (tag == "li" and self.capture == "profile"):
            text = " ".join(" ".join(self.buffer).split())
            if self.capture == "title":
                self.titles.append(text)
            elif re.match(r"^(Height|Reach|DOB|STANCE|Stance):", text):
                self.profile_items.append(text)
            self.capture, self.buffer = None, []


def parse_target_flags(raw: bytes, record: dict, target: dict) -> bool | None:
    parser = MetadataHTML()
    parser.feed(raw.decode("utf-8"))
    if url_id(record["source_url"]) != target["fight_id"]:
        return None
    participants = {url_id(u) for u in parser.links if "/fighter-details/" in u}
    events = {url_id(u) for u in parser.links if "/event-details/" in u}
    if participants != {target["fighter_1_id"], target["fighter_2_id"]} or events != {target["event_id"]}:
        return None
    if len(parser.titles) != 1 or "Bout" not in parser.titles[0]:
        return None
    wc, title, _ = _extract_bout_flags(parser.titles[0])
    return title if wc == target["weight_class"] else None


def parse_profile(raw: bytes, record: dict, fighter_id: str) -> dict:
    if url_id(record["source_url"]) != fighter_id:
        raise PreflightError("Supplemental profile URL identity differs")
    parser = MetadataHTML()
    parser.feed(raw.decode("utf-8"))
    fields = {v.split(":", 1)[0].lower(): v.split(":", 1)[1].strip() for v in parser.profile_items}
    if not {"height", "reach", "dob", "stance"} <= fields.keys():
        raise PreflightError("Raw profile lacks explicit physical-field structure")
    height = fields["height"]
    match = re.fullmatch(r"(\d+)'\s*(\d+)\"", height)
    if height != "--" and not match:
        raise PreflightError("Unknown raw height format")
    # Preserve repository scraper measurement semantics, including integer reach.
    height_cm = float((int(match[1]) * 12.0) * 2.54 + int(match[2]) * 2.54) if match else None
    reach = fields["reach"]
    reach_cm = int(float(reach.rstrip('"')) * 2.54) if reach != "--" else None
    dob = datetime.strptime(fields["dob"], "%b %d, %Y").date() if fields["dob"] != "--" else None
    return {"fighter_id": fighter_id, "height_cm": height_cm, "reach_cm": reach_cm,
            "dob": dob, "stance": fields["stance"] if fields["stance"] != "--" else None}


def relevant_fetches(targets: list[dict], repo: Path = ROOT) -> tuple[dict, dict]:
    keys = {(typ, t[k], f"data/raw/ufcstats/{folder}/{t[k]}.html") for t in targets
            for typ, k, folder in [("fight", "fight_id", "fights"), ("event", "event_id", "events"),
                                   ("fighter", "fighter_1_id", "fighters"), ("fighter", "fighter_2_id", "fighters")]}
    manifest_path = repo / "data/manifests/fetch_manifest.csv"
    relevant, count = {}, 0
    with manifest_path.open(newline="") as f:
        for r in csv.DictReader(f):
            count += 1
            if count > 200000:
                raise PreflightError("Fetch-manifest search exceeds fixed bound")
            key = (r["entity_type"], Path(r["storage_path"]).stem, r["storage_path"])
            if key in keys:
                relevant.setdefault(key, []).append(r)
    if sum(map(len, relevant.values())) > 20000:
        raise PreflightError("Relevant raw search exceeds fixed bound")
    body_hashes = {}
    for key in relevant:
        path = repo / key[2]
        if path.is_symlink() or not path.resolve().is_relative_to((repo / "data/raw/ufcstats").resolve()):
            raise PreflightError("Raw path escapes bounded storage")
        body_hashes[key] = sha256(path.read_bytes()) if path.is_file() else None
    return relevant, {"manifest_sha256": sha256(manifest_path.read_bytes()), "manifest_rows_scanned": count,
                      "target_paths": len(keys), "relevant_records": sum(map(len, relevant.values())),
                      "body_hashes": body_hashes}


def select_capture(records: list[dict], body_hash: str | None, *, identity: str, scored_at: str) -> tuple[dict | None, str]:
    eligible = [r for r in records if r["http_status"] == "200" and
                r["fetch_status"] in {"fetched", "updated", "unchanged"} and
                utc_instant(r["fetched_at"]) <= utc_instant(scored_at) and url_id(r["source_url"]) == identity]
    matching = [r for r in eligible if r["content_hash"] == body_hash]
    if not matching:
        return None, "body_missing_or_overwritten" if eligible else "no_eligible_capture"
    return max(matching, key=lambda r: (utc_instant(r["fetched_at"]), r["content_hash"],
                                       r["storage_path"], r["job_run_id"])), "exact_body_available_by_scoring"


def assess_inputs(targets: list[dict], snapshots: list[Snapshot], relevant: dict, raw_evidence: dict,
                  repo: Path = ROOT, *, frozen_run: Path | None = None) -> tuple[list[dict], dict[str, bytes], dict]:
    assignments, bodies, supplements = [], {}, {}
    for t in targets:
        source = select_snapshot(snapshots, t["scored_at"])
        cutoff = min(date.fromisoformat(t["event_date"]), utc_instant(t["scored_at"]).date())
        row = {"fight_id": t["fight_id"], "scored_at": t["scored_at"],
               "history_date_cutoff_exclusive": cutoff.isoformat(), "feature_reference_date": t["event_date"],
               "source_commit": source.record["commit"] if source else None, "gaps": [], "supplements": [], "raw_checks": []}
        local = {"profiles": {}, "title": None}
        if source is None:
            row["gaps"].append({"kind": "no_eligible_coherent_source"})
            assignments.append(row)
            supplements[t["fight_id"]] = local
            continue
        data = source.data
        prior = [f for f in data.fights if f["event_date"] < cutoff and f["result_type"] in {"win", "draw", "nc"}
                 and f["fight_id"] != t["fight_id"]]
        row.update(availability_utc=source.record["availability_utc"],
                   archive_age_days=(utc_instant(t["scored_at"]) - utc_instant(source.record["availability_utc"])).total_seconds() / 86400,
                   last_eligible_resolved_event_date=max((f["event_date"].isoformat() for f in prior), default=None),
                   known_history_lag_days=(cutoff - max(f["event_date"] for f in prior)).days if prior else None,
                   completeness="unverified_stale_archive", history_profiles=[])
        archived_target = next((r for r in source.rows["fights"] if r["fight_id"] == t["fight_id"]), None)
        if archived_target:
            same = archived_target["event_id"] == t["event_id"] and {
                archived_target["fighter_1_id"], archived_target["fighter_2_id"]} == {t["fighter_1_id"], t["fighter_2_id"]}
            event = data.event_by_id.get(t["event_id"])
            if not same or not event or event["event_date"].isoformat() != t["event_date"]:
                row["gaps"].append({"kind": "archived_target_identity_conflict"})
            elif archived_target["bout_type"].strip() and "Bout" in archived_target["bout_type"]:
                wc, title, _ = _extract_bout_flags(archived_target["bout_type"])
                if wc == t["weight_class"]:
                    local["title"] = title
                    row["target_flags_source"] = "selected_git_matchup_explicit_bout_type"
        for typ, key, folder in [("fight", "fight_id", "fights"), ("event", "event_id", "events"),
                                 ("fighter", "fighter_1_id", "fighters"), ("fighter", "fighter_2_id", "fighters")]:
            identity = t[key]
            raw_key = (typ, identity, f"data/raw/ufcstats/{folder}/{identity}.html")
            capture, status = select_capture(relevant.get(raw_key, []), raw_evidence["body_hashes"].get(raw_key),
                                             identity=identity, scored_at=t["scored_at"])
            row["raw_checks"].append({"entity_type": typ, "identity": identity, "status": status,
                                      "eligible_exact_record": capture})
            needs = typ == "fight" and local["title"] is None or typ == "fighter" and identity not in data.fighter_by_id
            if capture and needs:
                body_name = f'sources/raw/{typ}/{identity}-{capture["content_hash"]}.html'
                body = ((frozen_run / body_name) if frozen_run else (repo / capture["storage_path"])).read_bytes()
                if sha256(body) != capture["content_hash"]:
                    raise PreflightError("Raw body changed during preparation")
                bodies[body_name] = body
                component = {"entity_type": typ, "identity": identity, "body_path": body_name,
                             "sha256": sha256(body), "fetch_record": capture}
                try:
                    if typ == "fight":
                        title = parse_target_flags(body, capture, t)
                        if title is not None:
                            local["title"] = title
                            row["target_flags_source"] = "exact_raw_fight_body_and_fetch_record"
                            row["supplements"].append(component)
                    elif typ == "fighter":
                        local["profiles"][identity] = parse_profile(body, capture, identity)
                        row["supplements"].append(component)
                except (ValueError, KeyError) as exc:
                    row["gaps"].append({"kind": "supplement_parse_gap", "identity": identity, "detail": str(exc)})
        if local["title"] is None:
            row["gaps"].append({"kind": "unknown_target_title_status"})
        for fid in (t["fighter_1_id"], t["fighter_2_id"]):
            histories = [f for f in prior if fid in (f["fighter_1_id"], f["fighter_2_id"])]
            profile = data.fighter_by_id.get(fid) or local["profiles"].get(fid)
            if profile is None:
                row["gaps"].append({"kind": "missing_target_profile", "identity": fid})
            if fid in local["profiles"] and not histories:
                row["gaps"].append({"kind": "unresolved_experience_for_supplemented_profile", "identity": fid})
            row["history_profiles"].append({"fighter_id": fid, "archived_prior_count": len(histories),
                "archived_prior_fight_ids": [f["fight_id"] for f in sorted(histories, key=lambda f: (f["event_date"], f["fight_id"]))],
                "own_statistics_missing": sum((f["fight_id"], fid) not in data.stats_by_fight_fighter for f in histories),
                "opponent_statistics_missing": sum((f["fight_id"], f["fighter_2_id"] if f["fighter_1_id"] == fid else f["fighter_1_id"])
                                                   not in data.stats_by_fight_fighter for f in histories),
                "profile_field_missingness": {k: profile.get(k) is None for k in ["height_cm", "reach_cm", "dob", "stance"]} if profile else None})
        row["essential_inputs_ready"] = not row["gaps"]
        assignments.append(row)
        supplements[t["fight_id"]] = local
    return assignments, bodies, supplements


def verify_training_contract(candidate: Path = CANDIDATE) -> dict:
    if sha256((candidate / "checksums.json").read_bytes()) != PINS["candidate_checksums"]:
        raise PreflightError("Candidate checksum pin differs")
    components = verify_checksums(candidate)
    source_raw = (candidate / "source_manifest.json").read_bytes()
    if sha256(source_raw) != MANIFEST_SHA256:
        raise PreflightError("Training preparation contract pin differs")
    source = json.loads(source_raw)
    changed = [p for p, expected in source["code_sha256"].items() if sha256((ROOT / p).read_bytes()) != expected]
    metadata = json.loads((candidate / "metadata.json").read_bytes())
    if changed or metadata["input_feature_provenance"] != feature_provenance() or source["feature_order"] != FEATURE_ORDER:
        raise PreflightError(f"Training feature contract/code changed: {changed}")
    current = package_versions()
    saved = json.loads((candidate / "package_versions.json").read_bytes())
    if any(current[p] != saved[p] for p in ("numpy", "pandas", "scikit-learn", "xgboost", "joblib")):
        raise PreflightError("Candidate package versions differ")
    return {"training_contract_reference": feature_provenance(), "training_code_sha256": source["code_sha256"],
            "candidate_checksums_sha256": PINS["candidate_checksums"], "verified_components": len(components["files"]),
            "unchanged_training_code_verified": True,
            "training_manifest_role": "reference contract only; never the source of new scoring rows",
            "loader_reference_supplied": False, "candidate_prediction_called": False}


def validate_feature_frame(frame: pd.DataFrame, targets: list[dict]) -> dict:
    if list(frame.columns) != FEATURE_METADATA + FEATURE_ORDER or len(frame) != len(targets):
        raise PreflightError("Scoring coverage/feature order differs")
    for col in IDENTITY_COLUMNS + ["scored_at", "weight_class"]:
        if frame[col].astype(str).tolist() != [t[col] for t in targets]:
            raise PreflightError(f"Original target orientation/date/instant changed: {col}")
    if frame.fight_id.duplicated().any() or not frame.feature_version.eq(FEATURE_VERSION).all():
        raise PreflightError("Invalid scoring identity/version")
    values = frame[FEATURE_ORDER].to_numpy(dtype=float)
    if np.isinf(values).any() or not frame.scheduled_rounds.isna().all() or not frame[DEBUT_COLS].isna().all().all():
        raise PreflightError("Incompatible numeric/schedule/debut values")
    if not frame.both_debuting.isin([0., 1.]).all():
        raise PreflightError("Invalid debut indicator")
    for r, t in zip(frame.to_dict("records"), targets):
        cutoff = min(date.fromisoformat(t["event_date"]), utc_instant(t["scored_at"]).date()).isoformat()
        if r["history_date_cutoff_exclusive"] != cutoff or r["feature_reference_date"] != t["event_date"]:
            raise PreflightError("Incompatible knowledge/reference clocks")
    return {"rows": len(frame), "feature_order": FEATURE_ORDER,
            "missingness": {c: int(frame[c].isna().sum()) for c in FEATURE_ORDER}}


def construct_features(targets, assignments, snapshots, supplements) -> tuple[bytes, dict]:
    if any(r["gaps"] for r in assignments) or len(targets) != 108 or len(assignments) != 108:
        raise PreflightError("Essential gaps prevent complete 108-row reconstruction")
    by_commit = {s.record["commit"]: s for s in snapshots}
    rows, lineage = [], []
    for t, assignment in zip(targets, assignments):
        source = by_commit[assignment["source_commit"]]
        if select_snapshot(snapshots, t["scored_at"]) is not source:
            raise PreflightError("Source assignment differs from deterministic selection")
        data = source.data
        local = supplements[t["fight_id"]]
        isolated = WarehouseData(fights=data.fights, fighter_by_id={**data.fighter_by_id, **local["profiles"]},
                                 stats_by_fight_fighter=data.stats_by_fight_fighter)
        matchup = {k: t[k] for k in IDENTITY_COLUMNS + ["weight_class"]}
        matchup.update(event_date=date.fromisoformat(t["event_date"]), is_title_fight=local["title"])
        row, record = reconstruct_forecast(isolated, matchup, t["scored_at"])
        row.update(event_date=t["event_date"], source_commit=assignment["source_commit"])
        rows.append({k: row[k] for k in FEATURE_METADATA + FEATURE_ORDER})
        lineage.append(record)
    frame = pd.DataFrame(rows, columns=FEATURE_METADATA + FEATURE_ORDER)
    for col in FEATURE_ORDER:
        frame[col] = pd.to_numeric(frame[col], errors="raise").astype(float)
    summary = validate_feature_frame(frame, targets)
    raw = frame.to_csv(index=False, lineterminator="\n", float_format="%.17g").encode()
    return raw, {"forecast_lineage": lineage, **summary}


def comparison_protocol(targets: list[dict]) -> dict:
    return {
        "version": "phase4b_paired_108_v1", "population": "exact 108 accepted original pre-event forecasts",
        "fight_ids_in_order": [t["fight_id"] for t in targets], "event_ids": sorted({t["event_id"] for t in targets}),
        "qualification": "archived-as-of retrospective; outcomes previously inspected in prior phases; not blind or strict historical replay",
        "primary": "saved final candidate calibrated P(fighter_1 wins) vs exact original frozen calibrated baseline P(fighter_1 wins)",
        "baseline_pin": PINS["forecast_predictions"], "baseline_probability_column": "calibrated_prob_f1",
        "baseline_precision": "use frozen numeric values verbatim; historical rounding cannot be recovered; never recompute baseline",
        "primary_metrics": {"log_loss": "mean(-y*ln(clip(p))-(1-y)*ln(1-clip(p)))",
                            "brier": "mean((p-y)^2) using original unrounded stored p without clipping",
                            "log_loss_clip": [1e-8, 1 - 1e-8], "difference": "candidate minus baseline; negative favors candidate",
                            "scope": "all 108 binary resolved fights, paired; inclusive NO PICK rows included"},
        "secondary": "candidate raw probabilities: all-fight log loss and Brier with same clipping; diagnostic only, no alternative selection",
        "latent_label": "p >= 0.5 means fighter_1; ties select fighter_1",
        "no_pick": "inclusive 0.40 <= p <= 0.60", "actionable": "p < 0.40 or p > 0.60",
        "high_confidence": "inclusive p <= 0.30 or p >= 0.70; distinct from strongest-57 ranking",
        "report_groups": ["all fights latent accuracy", "each model's own actionable group",
                          "each model's own high-confidence group", "both models on baseline-selected actionable group",
                          "both models on baseline-selected high-confidence group", "both models on jointly actionable intersection"],
        "denominators": "for every group report total/selected/resolved/correct counts, accuracy=correct/resolved, coverage=selected/108; null accuracy for empty groups",
        "outcome_gate": "require exact oriented binary labels on all 108; otherwise fail primary comparison, report unresolved IDs and do not substitute another cohort",
        "calibration_bins": {"edges": [i / 10 for i in range(11)],
                             "assignment": "[0,.1),[.1,.2),...,[.9,1]; p=1 in last bin; full-precision saved values",
                             "report": "counts, mean probability, observed fraction fighter_1 wins; empty bins null; fixed ECE weighted by bin count/108"},
        "bootstrap": {"unit": "event_id", "pairing": "same sampled event multiplicities for both models; retain all fights of sampled events",
                      "event_order": "lexicographically sorted event IDs", "event_count": len({t["event_id"] for t in targets}),
                      "generator": "numpy.random.Generator(numpy.random.PCG64(20261002))",
                      "seed": 20261002, "replicates": 10000,
                      "sampling": "each replicate samples E event indices uniformly with replacement, size E, where E is original event count",
                      "estimator": "fight-weighted mean metric difference over concatenated sampled events, including repeats",
                      "interval": "two-sided percentile 95%; numpy.quantile at [.025,.975], method=linear; no BCa or alternate interval selection",
                      "scope": "primary log-loss and Brier paired differences; descriptive group accuracy/coverage otherwise",
                      "limitation": "only 13 event clusters; unstable intervals and limited generalization; no automatic promotion"},
        "prediction_lock": "before any Phase 4B outcome read, save exact oriented raw/calibrated candidate probabilities plus provenance; hash-lock prediction CSV/manifest and verify pins",
        "prohibited_after_outcome_open": ["threshold tuning", "feature/source changes", "recalibration", "model reselection", "baseline recomputation"],
        "promotion_authorized": False, "historical_146_or_strongest57_figures_transfer": False,
    }


def frozen_evidence(run: Path) -> tuple[list[Snapshot], dict, dict]:
    """Rebuild using the captured catalogue/logs and immutable source bodies."""
    manifest = json.loads((run / "source-manifest.json").read_bytes())
    snapshots = []
    for record in manifest["actual_scoring_sources"]:
        raws = {name: resolve_blob(entry) for name, entry in record["files"].items()}
        if source_structure(raws) != record["structure"]:
            raise PreflightError("Frozen source structure differs")
        if record["coherent"]:
            data, rows, summary = decode_snapshot(raws)
            if summary != record["summary"]:
                raise PreflightError("Frozen source coverage differs")
        else:
            try:
                decode_snapshot(raws)
            except (ValueError, KeyError, TypeError) as exc:
                if str(exc) != record["rejection_reason"]:
                    raise PreflightError("Frozen source rejection differs") from exc
            else:
                raise PreflightError("Rejected source unexpectedly compatible")
            data, rows = None, {}
        # Author/committer evidence remains linked to the exact Git commit.
        author, committer = git("show", "-s", "--format=%aI%n%cI", record["commit"]).decode().splitlines()
        if author != record["author_time"] or committer != record["committer_time"] or max(
                utc_instant(author), utc_instant(committer)).isoformat() != record["availability_utc"]:
            raise PreflightError("Frozen Git availability evidence differs")
        snapshots.append(Snapshot(record, data, rows))
    evidence = json.loads((run / "source-fetch-evidence.json").read_bytes())
    relevant = {}
    for r in evidence["relevant_records"]:
        key = (r["entity_type"], Path(r["storage_path"]).stem, r["storage_path"])
        relevant.setdefault(key, []).append(r)
    raw_evidence = {**evidence["search"], "body_hashes": {
        (r["entity_type"], r["identity"], r["path"]): r["sha256"] for r in evidence["observed_body_hashes"]}}
    return snapshots, relevant, raw_evidence


def prepare_payload(repo: Path = ROOT, *, frozen_run: Path | None = None) -> tuple[dict[str, bytes], dict]:
    targets, target_validation = load_targets()
    contract = verify_training_contract()
    if frozen_run is None:
        snapshots = discover_snapshots(repo)
        relevant, evidence = relevant_fetches(targets, repo)
    else:
        snapshots, relevant, evidence = frozen_evidence(frozen_run)
    assignments, bodies, supplements = assess_inputs(targets, snapshots, relevant, evidence, repo, frozen_run=frozen_run)
    gap_counts = {}
    for row in assignments:
        for gap in row["gaps"]:
            gap_counts[gap["kind"]] = gap_counts.get(gap["kind"], 0) + 1
    blocked = bool(gap_counts)
    status = "BLOCKED" if blocked else "READY_ARCHIVED_ASOF"
    code_files = ["modeling/scoring_inputs.py", "features/forecast_replay.py", "tools/prepare_phase4a_scoring_inputs.py"]
    code_hashes = {p: sha256((ROOT / p).read_bytes()) for p in code_files}
    raw_summary = {k: v for k, v in evidence.items() if k != "body_hashes"}
    manifest = {"schema_version": 1, "status": status, "source_policy": POLICY,
                "source_policy_document_sha256": sha256((ROOT / "docs/phase4a-source-policy.md").read_bytes()),
                "actual_scoring_sources": [s.record for s in snapshots], "raw_search": raw_summary,
                "strict_source_availability_certified": False,
                "availability_qualification": "exact Git versions before scoring in repository evidence; capture logs are not independently trusted timestamps",
                "complete_historical_availability_or_archive_claimed": False,
                "training_contract_reference_sha256": MANIFEST_SHA256,
                "training_contract_is_scoring_source": False, "reconstruction_code_sha256": code_hashes,
                "git_resolver": "modeling.scoring_inputs.resolve_blob checks commit:path -> blob and blob -> SHA-256; all catalogue blobs tested"}
    selection = {"schema_version": 1, "status": status, "policy": POLICY, "assignments": assignments,
                 "metadata_allowlist": TARGET_COLUMNS, "scoring_source_manifest_sha256": sha256(json_bytes(manifest))}
    compatibility = {"schema_version": 1, "status": "BLOCKED_NO_COMPLETE_SCORING_INPUT" if blocked else "VERIFIED",
                     **contract, "scoring_source_manifest_sha256": sha256(json_bytes(manifest)),
                     "source_selection_sha256": sha256(json_bytes(selection)), "reconstruction_code_sha256": code_hashes,
                     "feature_algorithm_version": ALGORITHM_VERSION, "date_semantics": DATE_SEMANTICS,
                     "input_provenance_kind": "new_forecast_reconstruction_from_actual_scoring_sources",
                     "real_vectors_validated": not blocked, "final_priors_applied_or_refit": False}
    lineage = {"schema_version": 1, "status": status, "date_semantics": DATE_SEMANTICS,
               "feature_order": FEATURE_ORDER, "feature_version": FEATURE_VERSION,
               "deferred_debut_columns": DEBUT_COLS, "scheduled_rounds": "unknown for target and all histories",
               "training_compatibility": "unchanged pinned feature functions; snapshots reference known target event date; history/Elo/opponent indexes capped separately",
               "fitting_excluded_ids": "may supply independent eligible inference history; never sourced from frozen outcomes",
               "gap_counts": gap_counts, "forecasts_with_essential_gaps": sum(bool(r["gaps"]) for r in assignments),
               "feature_missingness": None, "feature_construction_executed": not blocked}
    payload = {"targets.csv": csv_bytes(targets, TARGET_COLUMNS),
               "source-selection.json": json_bytes(selection), "source-manifest.json": json_bytes(manifest),
               "comparison-protocol.json": json_bytes(comparison_protocol(targets)),
               "package_versions.json": json_bytes(package_versions()), **bodies,
               "source-fetch-evidence.json": json_bytes({"search": raw_summary,
                   "relevant_records": [r for key in sorted(relevant) for r in relevant[key]],
                   "observed_body_hashes": [{"entity_type": key[0], "identity": key[1], "path": key[2], "sha256": value}
                                            for key, value in sorted(evidence["body_hashes"].items())]})}
    if not blocked:
        features, summary = construct_features(targets, assignments, snapshots, supplements)
        payload["features.csv"] = features
        lineage.update(summary, feature_missingness=summary["missingness"])
        compatibility["features_sha256"] = sha256(features)
    payload["feature-lineage.json"] = json_bytes(lineage)
    payload["compatibility.json"] = json_bytes(compatibility)
    validation = {"status": status, "target_validation": target_validation, "pins_verified": True,
                  "candidate_component_integrity": True, "training_contract_compatibility_verified": True,
                  "all_catalogue_git_blobs_resolved_and_hash_verified": True,
                  "forecasts_with_essential_gaps": lineage["forecasts_with_essential_gaps"], "gap_counts": gap_counts,
                  "completed_scoring_input_published": not blocked, "real_feature_rows_constructed": 0 if blocked else 108,
                  "outcomes_parsed": False, "warehouse_accessed": False, "model_fitted": False,
                  "priors_fitted_or_applied": False, "candidate_forecasts_scored": False,
                  "outcome_metrics_calculated": False}
    payload["validation_results.json"] = json_bytes(validation)
    if blocked:
        payload["INCOMPLETE.json"] = json_bytes({"status": "BLOCKED", "gap_counts": gap_counts,
            "meaning": "complete target/source/protocol evidence; no completed 108-row scoring input; do not score"})
    return payload, validation


def verify_run_checksums(run: Path) -> dict:
    checksums = json.loads((run / "checksums.json").read_bytes())
    if checksums.get("schema_version") != 1 or checksums.get("self_excluded") != "checksums.json":
        raise PreflightError("Invalid scoring run checksum contract")
    actual = {str(p.relative_to(run)) for p in run.rglob('*') if p.is_file()}
    if actual != set(checksums["files"]) | {"checksums.json"}:
        raise PreflightError("Unlisted/missing scoring artifact")
    for name, expected in checksums["files"].items():
        path = run / name
        if Path(name).is_absolute() or ".." in Path(name).parts or path.is_symlink() or not path.resolve().is_relative_to(run.resolve()):
            raise PreflightError("Scoring artifact path escapes run")
        if sha256(path.read_bytes()) != expected:
            raise PreflightError(f"Scoring artifact checksum differs: {name}")
    return checksums


def validated_loader_reference(run: Path) -> dict:
    """Narrow Phase 4B adapter: establish actual provenance before translating.

    Returns the loader's reference contract only after full archived-source
    rebuild parity. The receipt keeps actual scoring sources separate. This
    function does not call any prediction method or apply any priors.
    """
    verify_run_checksums(run)
    receipt = json.loads((run / "compatibility.json").read_bytes())
    if receipt.get("status") != "VERIFIED" or (run / "INCOMPLETE.json").exists() or not (run / "features.csv").is_file():
        raise PreflightError("Blocked/incomplete scoring input cannot supply loader provenance")
    rebuilt, validation = prepare_payload(frozen_run=run)
    if validation["status"] != "READY_ARCHIVED_ASOF":
        raise PreflightError("Scoring sources no longer satisfy all essential inputs")
    for name, raw in rebuilt.items():
        if name != "validation_results.json" and (run / name).read_bytes() != raw:
            raise PreflightError(f"Scoring reconstruction/compatibility parity differs: {name}")
    return {"loader_reference_contract": feature_provenance(),
            "actual_scoring_source_manifest_sha256": sha256((run / "source-manifest.json").read_bytes()),
            "scoring_features_sha256": sha256((run / "features.csv").read_bytes()),
            "compatibility_receipt_sha256": sha256((run / "compatibility.json").read_bytes()),
            "meaning": "validated translation to loader reference contract; training manifest is not scoring-source provenance"}


def preservation_check(baseline: dict, repo: Path = ROOT) -> dict:
    changed, missing = [], []
    for name, expected in baseline["files"].items():
        path = repo / name
        if not path.is_file():
            missing.append(name)
        elif sha256(path.read_bytes()) != expected:
            changed.append(name)
    strict = ["models", "data/holdouts", "data/audits", "data/experiments/phase3b_xgb_pre_april_2026",
              "data/experiments/phase3a_pre_april_2026_git_1f477d3",
              "data/experiments/phase3a_pre_april_2026_git_1f477d3_metadata_safe"]
    new = [str(p.relative_to(repo)) for group in strict for p in (repo / group).rglob('*')
           if p.is_file() and '__pycache__' not in p.parts and str(p.relative_to(repo)) not in baseline["files"]]
    pointer = sha256((repo / "models/production_model.json").read_bytes())
    result = {"status": "VERIFIED" if not changed and not missing and not new and pointer == PINS["production_pointer"] else "FAILED",
              "existing_files_checked": len(baseline["files"]), "changed": changed, "missing": missing,
              "new_protected_files": sorted(new), "production_pointer_sha256": pointer,
              "outcome_preservation_method": "hash-only; no parsing or interpretation"}
    if result["status"] != "VERIFIED":
        raise PreflightError(f"Preservation failed: {result}")
    return result


def publish(destination: Path, payload: dict[str, bytes]) -> dict:
    """Exclusive creation; immutable byte manifest; never overwrite a run."""
    if destination.parent.resolve() != OUTPUT_ROOT.resolve() or destination.exists() or destination.is_symlink():
        raise PreflightError("Publication requires new isolated Phase 4A run; refusing overwrite")
    destination.mkdir(parents=True, exist_ok=False)
    for name, raw in sorted(payload.items()):
        if Path(name).is_absolute() or ".." in Path(name).parts:
            raise PreflightError("Invalid artifact path")
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as f:
            f.write(raw)
    checksums = {"schema_version": 1, "self_excluded": "checksums.json",
                 "files": {name: sha256(raw) for name, raw in sorted(payload.items())}}
    with (destination / "checksums.json").open("xb") as f:
        f.write(json_bytes(checksums))
    for p in destination.rglob('*'):
        if p.is_file():
            p.chmod(0o444)
    return checksums

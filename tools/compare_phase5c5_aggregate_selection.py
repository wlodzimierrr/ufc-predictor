"""Read-only local selection comparison; constructs Requests without a crawler.

Run in the existing scraper environment with network/warehouse guards. This
reports eligibility, never recovered statistics or authoritative coverage.
The preserved shared mixin reproduces pre-Phase-5C.5 aggregate skip behavior.
"""

from collections import Counter, defaultdict
import csv
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scraper/UFC-Web-Scraping-main/ufc_scraper"))

from ufc_scraper.spiders.fight_stats import CrawlFightStats
from ufc_scraper.spiders.fight_stats_by_round import CrawlFightStatsByRound
from ufc_scraper.spiders.incremental import IncrementalCrawlMixin
from utils import get_uuid_string


class LegacyAggregateSelection(CrawlFightStats):
    _load_known_ids = IncrementalCrawlMixin._load_known_ids
    _load_captured_uuids = IncrementalCrawlMixin._load_captured_uuids


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def identity_set_hash(identities):
    """SHA-256 of UTF-8 sorted unique IDs, one per line, terminal newline."""
    return hashlib.sha256("".join(fid + "\n" for fid in sorted(set(identities))).encode()).hexdigest()


def compare():
    paths = [ROOT / name for name in ("data/fight_stats.csv", "data/fight_stats_by_round.csv",
                                     "data/manifests/fetch_manifest.csv", "data/manifests/fight_stats_queue.csv")]
    aggregate_path, round_path, _, queue_path = paths
    aggregate = read_csv(aggregate_path)
    queue = read_csv(queue_path)
    before = LegacyAggregateSelection(incremental="1", existing_csv=str(aggregate_path))
    after = CrawlFightStats(incremental="1", existing_csv=str(aggregate_path))
    rounds = CrawlFightStatsByRound(incremental="1", existing_csv=str(round_path))
    # Queue callbacks are never called and these Requests are never scheduled.
    before_urls = [request.url for request in before._start_from_queue()]
    after_urls = [request.url for request in after._start_from_queue()]
    queue_urls = [row["fight_url"] for row in queue if row.get("fight_url", "").strip()]
    before_ids = {get_uuid_string(url) for url in before_urls}
    after_ids = {get_uuid_string(url) for url in after_urls}
    queued_ids = {get_uuid_string(url) for url in queue_urls}
    missing = queued_ids - before.known_ids
    newly_eligible_missing = missing & (after_ids - before_ids)
    skipped = queued_ids - after_ids
    grouped = defaultdict(list)
    for row in aggregate:
        grouped[row.get("fight_id") or ""].append(row)
    incomplete = set(grouped) - after.known_ids
    one_side = {fid for fid in incomplete if len({r.get("fighter_id") for r in grouped[fid] if r.get("fighter_id")}) == 1}
    mismatched = sum(row.get("fight_id") != get_uuid_string(row["fight_url"])
                     for row in queue if row.get("fight_url", "").strip())
    return {
        "purpose": "OFFLINE_SELECTION_ELIGIBILITY_ONLY_NOT_RECOVERED_STATISTICS",
        "input_sha256": {str(path.relative_to(ROOT)): sha256(path) for path in paths},
        "queue": {"rows": len(queue), "nonblank_urls": len(queue_urls), "unique_url_ids": len(queued_ids),
                  "fight_id_url_id_mismatches": mismatched},
        "selection": {"before_eligible_entries": len(before_urls), "after_eligible_entries": len(after_urls),
                      "before_skipped_ids": len(queued_ids - before_ids), "after_skipped_ids": len(skipped),
                      "missing_aggregate_ids": len(missing),
                      "missing_suppressed_by_shared_manifest": len(missing & before.captured_uuids),
                      "previously_suppressed_missing_now_eligible": len(newly_eligible_missing),
                      "newly_eligible_missing_ids_sha256": identity_set_hash(newly_eligible_missing),
                      "remaining_skipped_ids_sha256": identity_set_hash(skipped),
                      "remaining_skipped_without_completion": len(skipped - after.known_ids),
                      "round_eligible_entries_unchanged": len(rounds.get_unknown_urls(queue_urls)),
                      "nonincremental_eligible_entries": len(CrawlFightStats(incremental="0", existing_csv=str(aggregate_path)).get_unknown_urls(queue_urls))},
        "parsed_output": {"rows": len(aggregate), "distinct_stated_fight_ids": len(grouped),
                          "complete_ids": len(after.known_ids), "incomplete_or_ambiguous_ids": len(incomplete),
                          "one_participant_ids": len(one_side), "other_incomplete_or_ambiguous_ids": len(incomplete - one_side),
                          "blank_fight_id_rows": len(grouped.get("", [])),
                          "remaining_skipped_row_count_distribution": dict(Counter(len(grouped[fid]) for fid in skipped)),
                          "remaining_skipped_participant_count_distribution": dict(Counter(len({r["fighter_id"] for r in grouped[fid]}) for fid in skipped))},
    }


if __name__ == "__main__":
    print(json.dumps(compare(), sort_keys=True, indent=2))

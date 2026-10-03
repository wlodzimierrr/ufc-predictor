"""Offline source diagnostics and regressions for subsequent scoped repairs.

Phase 5C.5 updates only the two aggregate-selection expectations. The saved
Phase 5C.3 report/JSON and other diagnostic defect reproductions are preserved.

Run with the already installed scraper environment:
  PYTHONDONTWRITEBYTECODE=1 scraper/UFC-Web-Scraping-main/.venv/bin/python \
    tools/tests/test_phase5c3_source_diagnostics.py
"""

import csv
import importlib.util
import logging
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scraper/UFC-Web-Scraping-main/ufc_scraper"))

from scrapy.http import HtmlResponse
from ufc_scraper.middlewares import RawCaptureMiddleware
from ufc_scraper.spiders.events import CrawlEvents
from ufc_scraper.spiders.fights import CrawlFights
from ufc_scraper.spiders.fight_stats import CrawlFightStats
from ufc_scraper.spiders.fight_stats_by_round import CrawlFightStatsByRound
from utils import get_uuid_string
from warehouse.transform import transform_fight


class SourceDiagnosisTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="phase5c3-diagnostic-")
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        for operation in ("socket.create_connection", "socket.socket.connect", "socket.socket.connect_ex"):
            guard = patch(operation, side_effect=AssertionError("Network forbidden in offline diagnosis"))
            guard.start()
            self.addCleanup(guard.stop)
        self.url = "http://www.ufcstats.com/fight-details/synthetic"
        self.fid = get_uuid_string(self.url)
        self.manifest = self.write_csv("manifest.csv", [
            {"source_url": self.url, "fetch_status": "fetched"},
        ])

    def write_csv(self, name, rows, fields=None):
        path = self.directory / name
        with path.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields or list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        return path

    def spider(self, cls, rows, fields=None, incremental="1"):
        parsed = self.write_csv(cls.name + ".csv", rows, fields)
        with patch.object(cls, "_resolve_manifest_path", return_value=self.manifest):
            return cls(incremental=incremental, existing_csv=str(parsed))

    def test_metadata_capture_cannot_skip_missing_aggregate_or_round(self):
        aggregate = self.spider(CrawlFightStats, [], ["fight_id"])
        rounds = self.spider(CrawlFightStatsByRound, [], ["fight_id"])
        self.assertEqual(aggregate.get_unknown_urls([self.url]), [self.url])
        self.assertEqual(rounds.get_unknown_urls([self.url]), [self.url])

    def test_failed_only_manifest_does_not_skip_aggregate(self):
        self.write_csv("manifest.csv", [{"source_url": self.url, "fetch_status": "failed"}])
        spider = self.spider(CrawlFightStats, [], ["fight_id"])
        self.assertEqual(spider.get_unknown_urls([self.url]), [self.url])

    def test_one_parsed_participant_cannot_skip_whole_aggregate_page(self):
        spider = self.spider(CrawlFightStats, [{"fight_id": self.fid, "fighter_id": "one-side-only"}])
        self.assertEqual(spider.get_unknown_urls([self.url]), [self.url])

    def test_known_upcoming_fight_can_be_refetched(self):
        spider = self.spider(CrawlFights, [{"fight_id": self.fid, "url": self.url, "event_status": "upcoming"}])
        self.assertEqual(spider.get_unknown_urls([self.url]), [self.url])

    def test_known_upcoming_event_can_be_refetched(self):
        url = "http://www.ufcstats.com/event-details/synthetic"
        self.write_csv("manifest.csv", [{"source_url": url, "fetch_status": "fetched"}])
        spider = self.spider(CrawlEvents, [{"event_id": get_uuid_string(url), "event_status": "upcoming"}])
        self.assertEqual(spider.get_unknown_urls([url]), [url])

    def test_any_completed_csv_version_skips_later_changed_record(self):
        rows = [{"fight_id": self.fid, "url": self.url, "event_status": status}
                for status in ("completed", "upcoming")]
        self.assertEqual(self.spider(CrawlFights, rows).get_unknown_urls([self.url]), [])
        self.assertEqual(self.spider(CrawlFights, rows, incremental="0").get_unknown_urls([self.url]), [self.url])

    def fight_row(self, status, first="", second=""):
        return {"fight_id": self.fid, "event_id": "synthetic-event", "fighter_1_id": "synthetic-one",
                "fighter_2_id": "synthetic-two", "event_status": status,
                "fighter_1_outcome": first, "fighter_2_outcome": second,
                "scraped_at": "2026-09-01 00:00:00 UTC", "url": self.url}

    def test_cancelled_empty_and_invalid_pairs_are_currently_misclassified_nc(self):
        # Diagnostic expectations describe the defect, not desired future policy.
        for row in (self.fight_row("canceled"), self.fight_row("completed"),
                    self.fight_row("completed", "W", "W")):
            with self.subTest(row=row):
                self.assertEqual(transform_fight(row)["result_type"], "nc")
        self.assertEqual(transform_fight(self.fight_row("upcoming"))["result_type"], "upcoming")
        self.assertEqual(transform_fight(self.fight_row("completed", "NC", "NC"))["result_type"], "nc")

    def test_changed_raw_body_overwrites_same_path_and_listing_urls_collide(self):
        middleware = RawCaptureMiddleware(self.directory)
        path = middleware._raw_path("fight", self.url)
        path.parent.mkdir(parents=True)
        old, new = b"synthetic initial page", b"synthetic changed page"
        import hashlib
        first, status = middleware._write_raw("fight", self.url, old, hashlib.sha256(old).hexdigest())
        self.assertEqual(status, "fetched")
        second, status = middleware._write_raw("fight", self.url, new, hashlib.sha256(new).hexdigest())
        self.assertEqual((first, second, status), (path, path, "updated"))
        self.assertEqual(path.read_bytes(), new)
        self.assertEqual(middleware._raw_path("event_listing", "http://ufcstats.com/statistics/events/completed?page=all"),
                         middleware._raw_path("event_listing", "http://ufcstats.com/statistics/events/upcoming"))

    def test_existing_round_only_raw_fixture_can_parse_two_aggregate_rows(self):
        # Existing local June 20 page; no acquisition, output file or feature build.
        fid = "347d3776-7af9-5de4-ac62-d97754f18b09"
        body = (ROOT / "data/raw/ufcstats/fights" / (fid + ".html")).read_bytes()
        # Resolve the actual preserved URL from the local queue, without running it.
        with (ROOT / "data/manifests/fight_stats_queue.csv").open() as stream:
            url = next(r["fight_url"] for r in csv.DictReader(stream) if r["fight_id"] == fid)
        response = HtmlResponse(url=url, body=body, encoding="utf-8")
        spider = self.spider(CrawlFightStats, [], ["fight_id"], incremental="0")
        parsed = list(spider._get_fight_stats(response))
        self.assertEqual(len(parsed), 2)
        self.assertEqual({r.fight_id for r in parsed}, {fid})
        self.assertEqual(len({r.fighter_id for r in parsed}), 2)

    def test_queue_uses_event_status_even_if_individual_fight_is_upcoming(self):
        path = ROOT / "scraper/UFC-Web-Scraping-main/build_fight_stats_queue.py"
        spec = importlib.util.spec_from_file_location("phase5c3_queue", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        local = self.write_csv("queue-input.csv", [{"fight_id": self.fid, "url": self.url,
            "event_id": "synthetic-event", "event_status": "upcoming"}])
        with patch.object(module, "_FIGHTS_CSV", local):
            self.assertIn(self.fid, module._load_fights_csv({"synthetic-event": "completed"}))
            self.assertNotIn(self.fid, module._load_fights_csv({"synthetic-event": "upcoming"}))


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    unittest.main(verbosity=2)

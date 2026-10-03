"""Defines the spider to crawl all fight URLs on ufcstats.com and parse fight statistics per fighter."""

import csv
from dataclasses import fields
from pathlib import Path
from typing import Any
from uuid import UUID

import scrapy
from scrapy.http import Request, Response

from entities import FightStats
from ufc_scraper.parsers.fight_stat_parser import FightStatParser
from ufc_scraper.spiders.incremental import IncrementalCrawlMixin
from utils import get_uuid_string


class CrawlFightStats(IncrementalCrawlMixin, scrapy.Spider):
    """Crawl all fight URLs and yield aggregate fight statistics per fighter.

    Seed priority:
      1. fight_url argument     — single-fight debug run (bypasses everything).
      2. fight_stats_queue.csv  — canonical queue built by build_fight_stats_queue.py;
                                   used automatically when the file exists.
      3. Event listing pages    — fallback 3-hop discovery (original behaviour).

    Usage examples:
        scrapy crawl crawl_fight_stats
        scrapy crawl crawl_fight_stats -a fight_url=<url>
    """

    name = "crawl_fight_stats"
    data_filename = "fight_stats.csv"
    id_column = "fight_id"

    # Event listing fallback — used only when fight_stats_queue.csv is absent.
    start_urls = ["http://www.ufcstats.com/statistics/events/completed?page=all"]

    # fight_stats.py: parents[5] == repo root (ufc-data/).
    _queue_path = Path(__file__).resolve().parents[5] / "data" / "manifests" / "fight_stats_queue.csv"

    def __init__(self, *args: Any, fight_url: str = "", **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Optional single-fight URL for debug runs.
        # Pass via: scrapy crawl crawl_fight_stats -a fight_url=<url>
        self._fight_url: str = fight_url.strip()

    def _load_captured_uuids(self) -> set[str]:
        """Shared metadata/round captures are not aggregate completion receipts."""
        return set()

    def _load_known_ids(self) -> set[str]:
        """Skip only unambiguous two-participant aggregate exports.

        Require the complete FightStats schema, native UUID5 identities and
        identity bindings, nonblank fields and nonnegative integer statistics.
        Every row for a fight must agree on its exact URL. Repeated participant
        rows must agree on every CSV field except scraped_at; conflicting
        revisions invalidate the fight, without choosing a preferred revision.
        Invalid rows invalidate both their stated and URL-derived fight IDs.

        This establishes local parsed-output coverage only. It does not prove
        freshness, correct statistics/results, source authority or UFC history
        completeness. Parser defaults can still be indistinguishable from zero.
        """
        if not self.incremental or not self.existing_csv.exists():
            return set()

        required = {field.name for field in fields(FightStats)}
        statistics = {field.name for field in fields(FightStats) if field.type is int}
        participants: dict[str, dict[str, tuple]] = {}
        urls: dict[str, set[str]] = {}
        invalid: set[str] = set()

        with self.existing_csv.open(newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            headers = reader.fieldnames or []
            if not required <= set(headers) or len(headers) != len(set(headers)):
                return set()

            for row in reader:
                fight_id = row.get("fight_id") or ""
                fighter_id = row.get("fighter_id") or ""
                url = row.get("url") or ""
                url_id = get_uuid_string(url) if url else ""
                try:
                    identities_valid = all(
                        str(UUID(value)) == value and UUID(value).version == 5
                        for value in (fight_id, fighter_id)
                    )
                except ValueError:
                    identities_valid = False

                valid = (
                    identities_valid
                    and None not in row
                    and all(row.get(name) and row[name].strip() for name in required)
                    and url == url.strip()
                    and url_id == fight_id
                    and row.get("fight_stat_id") == get_uuid_string(fight_id + fighter_id)
                    and all(row[name].isascii() and row[name].isdecimal() for name in statistics)
                )
                if not valid:
                    invalid.update((fight_id, url_id))
                    continue

                signature = tuple(row[name] for name in headers if name != "scraped_at")
                sides = participants.setdefault(fight_id, {})
                if fighter_id in sides and sides[fighter_id] != signature:
                    invalid.add(fight_id)
                sides[fighter_id] = signature
                urls.setdefault(fight_id, set()).add(url)

        return {
            fight_id for fight_id, sides in participants.items()
            if fight_id not in invalid and len(sides) == 2 and len(urls[fight_id]) == 1
        }

    def start_requests(self) -> Any:
        """Yield seed requests according to seed priority (see class docstring)."""
        if self._fight_url:
            yield Request(self._fight_url, callback=self._get_fight_stats)
            return

        if self._queue_path.exists():
            yield from self._start_from_queue()
            return

        self.logger.info(
            "fight_stats_queue.csv not found — seeding from event listing pages. "
            "Run build_fight_stats_queue.py to create the queue."
        )
        yield from super().start_requests()

    def _start_from_queue(self) -> Any:
        """Seed from fight_stats_queue.csv, applying incremental deduplication."""
        with self._queue_path.open(newline="", encoding="utf-8") as fh:
            all_urls = [
                row["fight_url"]
                for row in csv.DictReader(fh)
                if row.get("fight_url", "").strip()
            ]

        unknown = self.get_unknown_urls(all_urls)
        self.logger.info(
            "Fight stats queue: %d total | %d to fetch | %d skipped (incremental)",
            len(all_urls),
            len(unknown),
            len(all_urls) - len(unknown),
        )
        for url in unknown:
            yield Request(url, callback=self._get_fight_stats)

    def parse(self, response: Response) -> Any:
        """Parse an events listing page and schedule requests to event pages."""
        yield from response.follow_all(
            response.css("a[href*='event-details']::attr(href)").getall(),
            callback=self._get_fight_urls,
        )

    def _get_fight_urls(self, response: Response) -> Any:
        """Get all fight urls from an event page."""
        fight_urls = self.get_unknown_urls(
            response.css("a[href*='fight-details']::attr(href)").getall()
        )
        yield from response.follow_all(
            fight_urls,
            callback=self._get_fight_stats,
        )

    def _get_fight_stats(self, response: Response) -> Any:
        fight_id = get_uuid_string(response.url)

        if not response.css("thead.b-fight-details__table-head"):
            # Stats table absent — old fights or early-career bouts often have none.
            # Log with fight_id so the gap can be reconciled against the queue.
            self.logger.warning(
                "NO_STATS_PAGE | fight_id=%s | url=%s", fight_id, response.url
            )
            return

        try:
            fight_stat_parser = FightStatParser(response)
            fighter_1_stats, fighter_2_stats = tuple(fight_stat_parser.parse_response())
            yield fighter_1_stats
            yield fighter_2_stats
        except Exception as exc:
            self.logger.error(
                "Parse failure | fight_id=%s | url=%s | error=%s: %s",
                fight_id,
                response.url,
                type(exc).__name__,
                exc,
            )

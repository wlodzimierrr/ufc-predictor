"""Defines the spider to crawl all fight URLs on ufcstats.com and parse fight statistics per fighter."""

import csv
from pathlib import Path
from typing import Any

import scrapy
from scrapy.http import Request, Response

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

"""Defines the spider to crawl all fighter URLs ufcstats.com and parse fighter overview metrics."""

import csv
from pathlib import Path
from typing import Any

import scrapy
from scrapy.http import Request, Response

from ufc_scraper.parsers.fighter_info_parser import FighterInfoParser
from ufc_scraper.spiders.incremental import IncrementalCrawlMixin


class CrawlFighters(IncrementalCrawlMixin, scrapy.Spider):
    """Crawl all fighter URLs and yield fighter overview metrics.

    Seed priority:
      1. fighter_url argument  — single-profile debug run (bypasses everything).
      2. fighter_queue.csv     — canonical queue built by build_fighter_queue.py;
                                 used automatically when the file exists.
      3. A-Z listing pages     — fallback when no queue is present (original
                                 behaviour).

    Usage examples:
        scrapy crawl crawl_fighters                        # auto-detect seed
        scrapy crawl crawl_fighters -a fighter_url=<url>   # single-fighter debug
    """

    name = "crawl_fighters"
    data_filename = "fighters.csv"
    id_column = "fighter_id"

    # Fighter profile pages change infrequently (record updates aside).
    # HTTP caching avoids redundant fetches during iterative development runs
    # and is safe to enable here because the fighter spider is always run
    # separately from the event/fight/stats spiders.
    # Expiration is intentionally unbounded (None); invalidate manually by
    # deleting httpcache/ when a full profile refresh is required.
    custom_settings = {
        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_DIR": "httpcache",
        "HTTPCACHE_EXPIRATION_SECS": None,
    }

    # A-Z listing pages — used only when fighter_queue.csv is absent.
    start_urls = [
        f"http://ufcstats.com/statistics/fighters?char={letter}&page=all"
        for letter in "abcdefghijklmnopqrstuvwxyz"
    ]

    # fighters.py: parents[5] == repo root (ufc-data/).
    _queue_path = Path(__file__).resolve().parents[5] / "data" / "manifests" / "fighter_queue.csv"

    def __init__(self, *args: Any, fighter_url: str = "", **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Optional single-fighter URL for debug runs (bypasses queue and listing pages).
        # Pass via: scrapy crawl crawl_fighters -a fighter_url=<url>
        self._fighter_url: str = fighter_url.strip()

    def start_requests(self) -> Any:
        """Yield seed requests according to seed priority (see class docstring)."""
        if self._fighter_url:
            yield Request(self._fighter_url, callback=self._get_fighters)
            return

        if self._queue_path.exists():
            yield from self._start_from_queue()
            return

        # Fallback: original A-Z listing-page discovery.
        self.logger.info(
            "fighter_queue.csv not found — seeding from A-Z listing pages. "
            "Run build_fighter_queue.py to create the queue."
        )
        yield from super().start_requests()

    def _start_from_queue(self) -> Any:
        """Seed from fighter_queue.csv, applying incremental deduplication."""
        with self._queue_path.open(newline="", encoding="utf-8") as fh:
            all_urls = [
                row["fighter_url"]
                for row in csv.DictReader(fh)
                if row.get("fighter_url", "").strip()
            ]

        unknown = self.get_unknown_urls(all_urls)
        self.logger.info(
            "Fighter queue: %d total | %d to fetch | %d skipped (incremental)",
            len(all_urls),
            len(unknown),
            len(all_urls) - len(unknown),
        )
        for url in unknown:
            yield Request(url, callback=self._get_fighters)

    def parse(self, response: Response) -> Any:
        """Parse an A-Z fighter listing page and schedule requests to fighter pages."""
        fighter_urls = self.get_unknown_urls(
            response.css("a[href*='fighter-details']::attr(href)").getall()
        )
        yield from response.follow_all(
            fighter_urls,
            callback=self._get_fighters,
        )

    def _get_fighters(self, response: Response) -> Any:
        try:
            fighter_info_parser = FighterInfoParser(response)
            fighter = fighter_info_parser.parse_response()
            yield fighter
        except Exception as exc:
            self.logger.error(
                "Parse failure | url=%s | error=%s: %s",
                response.url,
                type(exc).__name__,
                exc,
            )

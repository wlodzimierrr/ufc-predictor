"""Defines the spider to crawl all fight URLs on ufcstats.com and parse fight statistics per round per fighter."""

import csv
from pathlib import Path
from typing import Any

import scrapy
from scrapy.http import Request, Response

from ufc_scraper.parsers.fight_stat_parser import FightStatByRoundParser
from ufc_scraper.spiders.incremental import IncrementalCrawlMixin
from utils import get_uuid_string


class CrawlFightStatsByRound(IncrementalCrawlMixin, scrapy.Spider):
    """Crawl all fight URLs and yield fight statistics per round per fighter.

    Seed priority:
      1. fight_url argument     — single-fight debug run (bypasses everything).
      2. fight_stats_queue.csv  — canonical queue; used automatically when present.
      3. Event listing pages    — fallback 3-hop discovery (original behaviour).

    Dedup note:
      CrawlFightStats and CrawlFightStatsByRound hit the same fight-detail URLs.
      The shared fetch_manifest.csv records fetches from *both* spiders.  If the
      aggregate spider ran first, its manifest entries would cause this spider to
      skip every fight in incremental mode via _load_captured_uuids().  To prevent
      that, _load_captured_uuids() is overridden to return an empty set here —
      deduplication is handled solely by known_ids (fight_stats_by_round.csv),
      which is scoped to this spider's own parsed output.

    Raw capture:
      RawCaptureMiddleware writes fight pages to data/raw/ufcstats/fights/ and
      uses SHA-256 deduplication — a page already captured by the aggregate spider
      returns status "unchanged" rather than being re-written.
    """

    name = "crawl_fight_stats_by_round"
    data_filename = "fight_stats_by_round.csv"
    id_column = "fight_id"

    # Event listing fallback — used only when fight_stats_queue.csv is absent.
    start_urls = ["http://www.ufcstats.com/statistics/events/completed?page=all"]

    # fight_stats_by_round.py: parents[5] == repo root (ufc-data/).
    _queue_path = Path(__file__).resolve().parents[5] / "data" / "manifests" / "fight_stats_queue.csv"

    def __init__(self, *args: Any, fight_url: str = "", **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._fight_url: str = fight_url.strip()

    # ------------------------------------------------------------------
    # Incremental dedup override
    # ------------------------------------------------------------------

    def _load_captured_uuids(self) -> set[str]:
        """Return empty set — do not use the shared fetch_manifest for dedup.

        The aggregate stats spider writes to the same fetch_manifest.csv using
        the same fight-detail URLs.  Inheriting the default implementation would
        cause every fight to appear already-captured and produce zero output when
        this spider runs after CrawlFightStats in incremental mode.

        Deduplication against this spider's own prior runs is fully handled by
        known_ids (fight_stats_by_round.csv rows keyed by fight_id).
        """
        return set()

    # ------------------------------------------------------------------
    # Seeding
    # ------------------------------------------------------------------

    def start_requests(self) -> Any:
        if self._fight_url:
            yield Request(self._fight_url, callback=self._get_fight_stats_by_round)
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
            yield Request(url, callback=self._get_fight_stats_by_round)

    def parse(self, response: Response) -> Any:
        """Parse an events listing page and schedule requests to event pages."""
        yield from response.follow_all(
            response.css("a[href*='event-details']::attr(href)").getall(),
            callback=self._get_fight_urls,
        )

    def _get_fight_urls(self, response: Response) -> Any:
        fight_urls = self.get_unknown_urls(
            response.css("a[href*='fight-details']::attr(href)").getall()
        )
        yield from response.follow_all(
            fight_urls,
            callback=self._get_fight_stats_by_round,
        )

    def _get_fight_stats_by_round(self, response: Response) -> Any:
        fight_id = get_uuid_string(response.url)

        if not response.css("thead.b-fight-details__table-head"):
            self.logger.warning(
                "NO_ROUND_TABLE | fight_id=%s | url=%s", fight_id, response.url
            )
            return

        try:
            fight_stat_by_round_parser = FightStatByRoundParser(response)
            for fight_stats in fight_stat_by_round_parser.parse_response():
                yield fight_stats
        except Exception as exc:
            self.logger.error(
                "Parse failure | fight_id=%s | url=%s | error=%s: %s",
                fight_id,
                response.url,
                type(exc).__name__,
                exc,
            )

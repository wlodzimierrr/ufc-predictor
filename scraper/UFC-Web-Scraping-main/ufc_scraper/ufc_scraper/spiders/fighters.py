"""Defines the spider to crawl all fighter URLs ufcstats.com and parse fighter overview metrics."""

from typing import Any

import scrapy
from scrapy.http import Response

from ufc_scraper.parsers.fighter_info_parser import FighterInfoParser
from ufc_scraper.spiders.incremental import IncrementalCrawlMixin


class CrawlFighters(IncrementalCrawlMixin, scrapy.Spider):
    """Crawl all fighter URLs and yield fighter overview metrics."""

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

    start_urls = [
        f"http://ufcstats.com/statistics/fighters?char={letter}&page=all"
        for letter in "abcdefghijklmnopqrstuvwxyz"
    ]

    def parse(self, response: Response) -> Any:
        """Parse the fighter listing page and schedule requests to fighter pages."""
        fighter_urls = self.get_unknown_urls(
            response.css("a[href*='fighter-details']::attr(href)").getall()
        )
        yield from response.follow_all(
            fighter_urls,
            callback=self._get_fighters,
        )

    def _get_fighters(self, response: Response) -> Any:
        fighter_info_parser = FighterInfoParser(response)
        fighter = fighter_info_parser.parse_response()

        yield (fighter)

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

    custom_settings = {
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
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

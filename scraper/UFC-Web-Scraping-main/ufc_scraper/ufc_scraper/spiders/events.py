"""Defines the spider to crawl all event URLs ufcstats.com and parse event overview metrics."""

from typing import Any

import scrapy
from scrapy.http import Response

from ufc_scraper.parsers.event_info_parser import EventInfoParser
from ufc_scraper.spiders.incremental import IncrementalCrawlMixin


class CrawlEvents(IncrementalCrawlMixin, scrapy.Spider):
    """Crawl all event URLs and yield event overview metrics."""

    name = "crawl_events"
    data_filename = "events.csv"
    id_column = "event_id"

    start_urls = ["http://www.ufcstats.com/statistics/events/completed?page=all"]

    def parse(self, response: Response) -> Any:
        """Parse the events listing page and schedule requests to event pages."""
        event_urls = self.get_unknown_urls(
            response.css("a[href*='event-details']::attr(href)").getall()
        )
        yield from response.follow_all(
            event_urls,
            callback=self._get_events,
        )

    def _get_events(self, response: Response) -> Any:
        event_info_parser = EventInfoParser(response)
        event = event_info_parser.parse_response()

        yield (event)

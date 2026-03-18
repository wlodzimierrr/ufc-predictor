"""Defines the spider to crawl all event URLs ufcstats.com and parse event overview metrics."""

import csv
from typing import Any

import scrapy
from scrapy.http import Response

from ufc_scraper.parsers.event_info_parser import EventInfoParser
from ufc_scraper.spiders.incremental import IncrementalCrawlMixin
from utils import get_uuid_string


class CrawlEvents(IncrementalCrawlMixin, scrapy.Spider):
    """Crawl all event URLs and yield event overview metrics.

    Seeds from both the completed-events listing and the upcoming-events
    listing so the full Phase 1 event scope is covered in a single run.
    Events discovered via the upcoming listing are tagged event_status=
    "upcoming"; all others are tagged "completed".

    Within-run deduplication ensures that an event appearing on both
    listing pages is only requested and yielded once, regardless of
    whether incremental mode is active.
    """

    name = "crawl_events"
    data_filename = "events.csv"
    id_column = "event_id"

    start_urls = [
        "http://www.ufcstats.com/statistics/events/completed?page=all",
        "http://www.ufcstats.com/statistics/events/upcoming",
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # UUIDs of event-detail URLs already scheduled in this run.
        # Prevents double-scheduling when the same event appears on both
        # the completed and upcoming listing pages.
        self._seen_event_uuids: set[str] = set()

    # ------------------------------------------------------------------
    # Incremental skip overrides
    # ------------------------------------------------------------------

    def _load_known_ids(self) -> set[str]:
        """Only skip completed events in incremental mode.

        Upcoming events are always re-fetched so that the completed
        transition (and the populated fight card that comes with it)
        is captured on the next run.
        """
        if not self.incremental or not self.existing_csv.exists():
            return set()
        with self.existing_csv.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            return {
                row[self.id_column].strip()
                for row in reader
                if row.get(self.id_column, "").strip()
                and row.get("event_status", "completed") == "completed"
            }

    def _load_captured_uuids(self) -> set[str]:
        """Exclude upcoming-event UUIDs from the manifest-based skip set.

        The manifest does not carry event_status, so we subtract the
        UUIDs of events the CSV knows as upcoming before returning.
        """
        captured = super()._load_captured_uuids()
        if not captured or not self.existing_csv.exists():
            return captured
        with self.existing_csv.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            upcoming_uuids = {
                row[self.id_column].strip()
                for row in reader
                if row.get(self.id_column, "").strip()
                and row.get("event_status", "") == "upcoming"
            }
        return captured - upcoming_uuids

    def parse(self, response: Response) -> Any:
        """Parse an events listing page and schedule requests to event pages."""
        event_status = "upcoming" if "upcoming" in response.url else "completed"

        # Filter by manifest/CSV (incremental), then deduplicate within run.
        unknown_urls = self.get_unknown_urls(
            response.css("a[href*='event-details']::attr(href)").getall()
        )
        new_urls = []
        for url in unknown_urls:
            uid = get_uuid_string(url)
            if uid not in self._seen_event_uuids:
                self._seen_event_uuids.add(uid)
                new_urls.append(url)

        yield from response.follow_all(
            new_urls,
            callback=self._get_events,
            cb_kwargs={"event_status": event_status},
        )

    def _get_events(self, response: Response, event_status: str = "completed") -> Any:
        event_info_parser = EventInfoParser(response)
        event = event_info_parser.parse_response(event_status=event_status)
        yield event

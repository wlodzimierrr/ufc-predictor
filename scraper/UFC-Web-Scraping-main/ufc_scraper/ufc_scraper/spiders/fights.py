"""Defines the spider to crawl all fight URLs ufcstats.com and parse fight overview metrics."""

import csv
from typing import Any

import scrapy
from scrapy.http import Response

from ufc_scraper.parsers.fight_info_parser import FightInfoParser
from ufc_scraper.spiders.incremental import IncrementalCrawlMixin
from utils import get_uuid_string


class CrawlFights(IncrementalCrawlMixin, scrapy.Spider):
    """Crawl all fight URLs and yield fight overview metrics.

    Seeds from both the completed-events listing and the upcoming-events
    listing so upcoming fight cards are captured before event day.

    Upcoming fights are re-fetched on every incremental run so that
    the completed transition (winner, finish method, etc.) is captured
    once the event has taken place.
    """

    name = "crawl_fights"
    data_filename = "fights.csv"
    id_column = "fight_id"

    start_urls = [
        "http://www.ufcstats.com/statistics/events/completed?page=all",
        "http://www.ufcstats.com/statistics/events/upcoming",
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._seen_event_uuids: set[str] = set()

    # ------------------------------------------------------------------
    # Incremental skip overrides
    # ------------------------------------------------------------------

    def _load_known_ids(self) -> set[str]:
        """Only skip completed fights in incremental mode.

        Upcoming fights are always re-fetched so that results are
        captured once the event completes.
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
        """Exclude upcoming-fight UUIDs from the manifest-based skip set.

        The fetch manifest does not carry event_status, so we subtract
        the UUIDs of fights the CSV knows as upcoming before returning.
        """
        captured = super()._load_captured_uuids()
        if not captured or not self.existing_csv.exists():
            return captured
        with self.existing_csv.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            upcoming_uuids = set()
            for row in reader:
                if row.get("event_status", "") == "upcoming" and row.get("url", ""):
                    upcoming_uuids.add(get_uuid_string(row["url"]))
        return captured - upcoming_uuids

    # ------------------------------------------------------------------
    # Spider callbacks
    # ------------------------------------------------------------------

    def parse(self, response: Response) -> Any:
        """Parse the events listing page and schedule requests to event pages."""
        event_status = "upcoming" if "upcoming" in response.url else "completed"
        yield from self._get_event_urls(response, event_status)

    def _get_event_urls(self, response: Response, event_status: str = "completed") -> Any:
        """Get all event urls from main event page."""
        urls = response.css("a[href*='event-details']::attr(href)").getall()
        new_urls = []
        for url in urls:
            uid = get_uuid_string(url)
            if uid not in self._seen_event_uuids:
                self._seen_event_uuids.add(uid)
                new_urls.append(url)
        yield from response.follow_all(
            new_urls,
            callback=self._get_fight_urls,
            cb_kwargs={"event_status": event_status},
        )

    def _get_fight_urls(self, response: Response, event_status: str = "completed") -> Any:
        """Get all fight urls from each event page."""
        # Completed events use <a href>, upcoming events use data-link on <tr>.
        href_urls = response.css("a[href*='fight-details']::attr(href)").getall()
        data_urls = response.css("tr[data-link*='fight-details']::attr(data-link)").getall()
        all_urls = list(dict.fromkeys(href_urls + data_urls))  # dedupe, preserve order
        fight_urls = self.get_unknown_urls(all_urls)
        yield from response.follow_all(
            fight_urls,
            callback=self._get_fights,
            cb_kwargs={"event_status": event_status},
        )

    def _get_fights(self, response: Response, event_status: str = "completed") -> Any:
        fight_info_parser = FightInfoParser(response)
        fight = fight_info_parser.parse_response(event_status=event_status)
        yield fight

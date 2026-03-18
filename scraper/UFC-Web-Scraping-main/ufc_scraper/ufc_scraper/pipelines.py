# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import csv
from datetime import datetime, timezone
from pathlib import Path

from entities import Event

# useful for handling different item types with a single interface


class UfcScraperPipeline:
    def process_item(self, item, spider):
        return item


class EventsManifestPipeline:
    """Maintains data/manifests/events_manifest.csv as a canonical event registry.

    One row per event, keyed by event_id.  Supports incremental refresh:
    - New events are inserted with a discovered_at timestamp.
    - Existing events are updated when event_status changes (e.g. upcoming
      → completed) or on every run to keep last_seen_at current.
    - discovered_at is set once and never overwritten.

    Non-Event items pass through unchanged.
    """

    _MANIFEST_FIELDS = [
        "event_id",
        "event_url",
        "event_name",
        "event_date",
        "event_status",
        "discovered_at",
        "last_seen_at",
    ]

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir
        self._manifest_path = data_dir / "manifests" / "events_manifest.csv"
        # Keyed by event_id; loaded from disk on open_spider.
        self._records: dict[str, dict] = {}

    @classmethod
    def from_crawler(cls, crawler):
        # pipelines.py: parents[4] == repo root (ufc-data/).
        data_dir = Path(__file__).resolve().parents[4] / "data"
        return cls(data_dir=data_dir)

    # ------------------------------------------------------------------
    # Scrapy lifecycle
    # ------------------------------------------------------------------

    def open_spider(self, spider) -> None:
        if spider.name != "crawl_events":
            return
        self._data_dir.mkdir(parents=True, exist_ok=True)
        (self._data_dir / "manifests").mkdir(parents=True, exist_ok=True)
        if self._manifest_path.exists():
            with self._manifest_path.open(newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    event_id = row.get("event_id", "").strip()
                    if event_id:
                        self._records[event_id] = dict(row)
            spider.logger.info(
                "EventsManifestPipeline loaded %d existing records from %s",
                len(self._records),
                self._manifest_path,
            )

    def process_item(self, item, spider):
        if not isinstance(item, Event):
            return item

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        event_id = item.event_id

        if event_id in self._records:
            existing = self._records[event_id]
            existing["event_status"] = item.event_status
            existing["event_name"] = item.name
            existing["event_date"] = item.date_formatted
            existing["last_seen_at"] = now
            # discovered_at is preserved as-is
        else:
            self._records[event_id] = {
                "event_id": event_id,
                "event_url": item.url,
                "event_name": item.name,
                "event_date": item.date_formatted,
                "event_status": item.event_status,
                "discovered_at": now,
                "last_seen_at": now,
            }

        return item

    def close_spider(self, spider) -> None:
        if spider.name != "crawl_events" or not self._records:
            return
        with self._manifest_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=self._MANIFEST_FIELDS)
            writer.writeheader()
            writer.writerows(self._records.values())
        spider.logger.info(
            "EventsManifestPipeline wrote %d records to %s",
            len(self._records),
            self._manifest_path,
        )

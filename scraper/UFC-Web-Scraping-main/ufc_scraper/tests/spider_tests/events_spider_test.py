import csv

from ufc_scraper.spiders.events import CrawlEvents
from utils import get_uuid_string


def test_events_spider_allows_incremental_recovery_for_missing_rows(
    monkeypatch, tmp_path
) -> None:
    present_url = "http://www.ufcstats.com/event-details/present"
    missing_url = "http://www.ufcstats.com/event-details/missing"
    upcoming_url = "http://www.ufcstats.com/event-details/upcoming"

    existing_csv = tmp_path / "events.csv"
    with existing_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["event_id", "event_status"])
        writer.writeheader()
        writer.writerow(
            {
                "event_id": get_uuid_string(present_url),
                "event_status": "completed",
            }
        )
        writer.writerow(
            {
                "event_id": get_uuid_string(upcoming_url),
                "event_status": "upcoming",
            }
        )

    manifest_csv = tmp_path / "fetch_manifest.csv"
    with manifest_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["source_url", "fetch_status"],
        )
        writer.writeheader()
        for url in (present_url, missing_url, upcoming_url):
            writer.writerow({"source_url": url, "fetch_status": "updated"})

    monkeypatch.setattr(CrawlEvents, "_resolve_manifest_path", lambda self: manifest_csv)

    spider = CrawlEvents(incremental="true", existing_csv=str(existing_csv))

    assert spider.captured_uuids == {get_uuid_string(present_url)}
    assert spider.get_unknown_urls([present_url, missing_url, upcoming_url]) == [
        missing_url,
        upcoming_url,
    ]

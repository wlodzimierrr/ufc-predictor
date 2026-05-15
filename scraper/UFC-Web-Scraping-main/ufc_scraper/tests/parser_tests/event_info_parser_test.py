from freezegun import freeze_time
import pytest

from scrapy.http import HtmlResponse, Request

from entities import Event
from ufc_scraper.parsers.event_info_parser import EventInfoParser
from tests import EVENT_RESPONSE_VALID_PATH
from tests.utils import load_html_response_from_file
from utils import get_uuid_string


@pytest.fixture
def event_info_parser_valid() -> EventInfoParser:
    event_response = load_html_response_from_file(EVENT_RESPONSE_VALID_PATH)

    return EventInfoParser(event_response)


@freeze_time("2000-01-01 00:00:00", tz_offset=0)
def test_event_info_parse_response_valid(
    event_info_parser_valid: EventInfoParser,
) -> None:
    parsed_response = event_info_parser_valid.parse_response()

    event_id = get_uuid_string("http://ufcstats.com/event_response_valid")
    fight_urls = [
        "http://www.ufcstats.com/fight-details/ebf7cea27b83c432",
        "http://www.ufcstats.com/fight-details/b8bf186f884678ea",
        "http://www.ufcstats.com/fight-details/b09f54654b0f95f5",
        "http://www.ufcstats.com/fight-details/cc11a0762f246fa4",
        "http://www.ufcstats.com/fight-details/108434acbbd75d26",
        "http://www.ufcstats.com/fight-details/781a2ea50f5ae68f",
        "http://www.ufcstats.com/fight-details/1d65c141776949d6",
        "http://www.ufcstats.com/fight-details/919e3c5cc6999cb3",
        "http://www.ufcstats.com/fight-details/af6eeea1cfe33240",
        "http://www.ufcstats.com/fight-details/df7928de73fc2fe8",
        "http://www.ufcstats.com/fight-details/5c783cd188425783",
        "http://www.ufcstats.com/fight-details/bde635a4f345bc3e",
        "http://www.ufcstats.com/fight-details/6114076c689e8457",
    ]
    fight_ids = [get_uuid_string(fight_url) for fight_url in fight_urls]
    fights = ", ".join(fight_ids)

    expected_response = Event(
        scraped_at="2000-01-01 00:00:00 UTC",
        event_id=event_id,
        url="http://www.ufcstats.com/event_response_valid",
        name="UFC 308: Topuria vs. Holloway",
        date="October 26, 2024",
        date_formatted="2024-10-26",
        city="Abu Dhabi",
        state="Abu Dhabi",
        country="United Arab Emirates",
        fights=fights,
        fight_urls=", ".join(fight_urls),
        event_status="upcoming",
    )

    assert parsed_response == expected_response


@freeze_time("2000-01-01 00:00:00", tz_offset=0)
def test_event_info_parse_response_allows_missing_fight_links() -> None:
    body = b"""
    <html>
      <body>
        <span class="b-content__title-highlight">Upcoming Test Event</span>
        <li class="b-list__box-list-item">Date:</li>
        <li class="b-list__box-list-item">April 01, 2026</li>
        <li class="b-list__box-list-item">Location:</li>
        <li class="b-list__box-list-item">London, England</li>
      </body>
    </html>
    """
    response = HtmlResponse(
        url="http://www.ufcstats.com/event-details/test-no-fights",
        request=Request(url="http://www.ufcstats.com/event-details/test-no-fights"),
        body=body,
    )

    parsed_response = EventInfoParser(response).parse_response(event_status="upcoming")

    assert parsed_response == Event(
        scraped_at="2000-01-01 00:00:00 UTC",
        event_id=get_uuid_string("http://www.ufcstats.com/event-details/test-no-fights"),
        url="http://www.ufcstats.com/event-details/test-no-fights",
        name="Upcoming Test Event",
        date="April 01, 2026",
        date_formatted="2026-04-01",
        city="London",
        state="",
        country="England",
        fights="",
        fight_urls="",
        event_status="upcoming",
    )


@freeze_time("2000-01-01 00:00:00", tz_offset=0)
def test_event_info_parse_response_reads_upcoming_data_links() -> None:
    fight_urls = [
        "http://www.ufcstats.com/fight-details/fight-one",
        "http://www.ufcstats.com/fight-details/fight-two",
    ]
    body = f"""
    <html>
      <body>
        <span class="b-content__title-highlight">Upcoming Card</span>
        <li class="b-list__box-list-item">Date:</li>
        <li class="b-list__box-list-item">April 18, 2026</li>
        <li class="b-list__box-list-item">Location:</li>
        <li class="b-list__box-list-item">Montreal, Quebec, Canada</li>
        <table>
          <tr data-link="{fight_urls[0]}"></tr>
          <tr data-link="{fight_urls[1]}"></tr>
        </table>
      </body>
    </html>
    """.encode("utf-8")
    response = HtmlResponse(
        url="http://www.ufcstats.com/event-details/test-upcoming-fights",
        request=Request(url="http://www.ufcstats.com/event-details/test-upcoming-fights"),
        body=body,
    )

    parsed_response = EventInfoParser(response).parse_response(event_status="upcoming")

    assert parsed_response.fights == ", ".join(get_uuid_string(url) for url in fight_urls)
    assert parsed_response.fight_urls == ", ".join(fight_urls)
    assert parsed_response.event_status == "upcoming"

"""Aggregate selection with synthetic exports/HTML; never start a crawler."""

import csv
from dataclasses import fields
import importlib.abc
import socket
import sys

import pytest
import scrapy
from scrapy.http import HtmlResponse

from entities import FightStats
from ufc_scraper.spiders.fight_stats import CrawlFightStats
from ufc_scraper.spiders.fight_stats_by_round import CrawlFightStatsByRound
from ufc_scraper.spiders.incremental import IncrementalCrawlMixin
from utils import get_uuid_string


URL = "http://www.ufcstats.com/fight-details/synthetic"
FIGHT_ID = get_uuid_string(URL)
FIGHTER_IDS = [get_uuid_string(f"http://ufcstats.com/fighter-details/side-{n}") for n in range(3)]
HEADERS = [field.name for field in fields(FightStats)]


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Network/real warehouse forbidden in aggregate selection tests")

    for name in ("create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex", "gethostbyaddr"):
        monkeypatch.setattr(socket, name, forbidden)
    for name in ("connect", "connect_ex", "sendto", "sendmsg"):
        if hasattr(socket.socket, name):
            monkeypatch.setattr(socket.socket, name, forbidden)

    class NoWarehouse(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "warehouse.db":
                forbidden()

    guard = NoWarehouse()
    sys.meta_path.insert(0, guard)
    yield
    sys.meta_path.remove(guard)


def aggregate_row(side=0, url=URL, **changes):
    fight_id = get_uuid_string(url)
    fighter_id = FIGHTER_IDS[side]
    row = {name: "0" for name in HEADERS}
    row.update(scraped_at="2026-09-01 00:00:00 UTC", fight_id=fight_id,
               fighter_id=fighter_id, fight_stat_id=get_uuid_string(fight_id + fighter_id), url=url)
    row.update(changes)
    return row


def write_csv(path, rows, headers=HEADERS):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    return path


@pytest.fixture
def make_spider(tmp_path, monkeypatch):
    parsed = tmp_path / "aggregates.csv"
    manifest = tmp_path / "manifest.csv"
    queue = tmp_path / "queue.csv"
    monkeypatch.setattr(CrawlFightStats, "_queue_path", queue)
    monkeypatch.setattr(IncrementalCrawlMixin, "_resolve_manifest_path", lambda self: manifest)

    def make(rows=(), incremental="1", status="fetched", **kwargs):
        write_csv(parsed, rows)
        write_csv(manifest, [{"source_url": URL, "fetch_status": status}], ["source_url", "fetch_status"])
        return CrawlFightStats(incremental=incremental, existing_csv=str(parsed), **kwargs)

    make.parsed, make.manifest, make.queue = parsed, manifest, queue
    return make


@pytest.mark.parametrize("status", ["fetched", "unchanged", "updated", "failed", "unknown", ""])
def test_shared_capture_never_supplies_missing_aggregate_completion(make_spider, status):
    spider = make_spider(status=status)
    assert spider.captured_uuids == set()
    assert spider.get_unknown_urls([URL]) == [URL]


def test_round_only_capture_cannot_supply_aggregate_completion(make_spider):
    aggregate = make_spider()
    rounds_file = write_csv(make_spider.parsed.parent / "rounds.csv",
                            [{"fight_id": FIGHT_ID, "fighter_id": FIGHTER_IDS[0], "round": "1"}],
                            ["fight_id", "fighter_id", "round"])
    rounds = CrawlFightStatsByRound(incremental="1", existing_csv=str(rounds_file))
    assert rounds.get_unknown_urls([URL]) == []  # Preserve its existing one-row rule.
    assert aggregate.get_unknown_urls([URL]) == [URL]


@pytest.mark.parametrize("rows", [[], [aggregate_row()], [aggregate_row()] * 3,
                                  [aggregate_row(), aggregate_row(scraped_at="2026-09-02 00:00:00 UTC")]])
def test_one_participant_and_duplicates_remain_eligible(make_spider, rows):
    assert make_spider(rows).get_unknown_urls([URL]) == [URL]


@pytest.mark.parametrize("status", ["fetched", "unchanged", "updated", "failed", ""])
@pytest.mark.parametrize("duplicate", [False, True])
def test_two_sides_skip_from_own_output_independent_of_manifest(make_spider, status, duplicate):
    rows = [aggregate_row(), aggregate_row(1)]
    if duplicate:
        rows += [aggregate_row(scraped_at="2026-09-02 00:00:00 UTC"), aggregate_row(1)]
    spider = make_spider(rows, status=status)
    assert spider.known_ids == {FIGHT_ID}
    assert spider.get_unknown_urls([URL]) == []
    assert spider._skipped_count == 1


@pytest.mark.parametrize("field", ["fight_id", "fighter_id", "fight_stat_id"])
@pytest.mark.parametrize("bad", ["", " ", "not-a-uuid", "00000000-0000-0000-0000-000000000000"])
def test_invalid_identity_cannot_complete_or_hide_behind_valid_pair(make_spider, field, bad):
    rows = [aggregate_row(), aggregate_row(1), aggregate_row(**{field: bad})]
    assert make_spider(rows).get_unknown_urls([URL]) == [URL]


@pytest.mark.parametrize("change", [
    {"fighter_id": FIGHTER_IDS[0].upper()},
    {"fighter_id": " " + FIGHTER_IDS[0]},
    {"fight_stat_id": get_uuid_string("unrelated-statistic")},
    {"fight_id": get_uuid_string("unrelated-fight")},
    {"url": "https://ufcstats.com/fight-details/synthetic"},
    {"url": ""}, {"url": " " + URL},
    {"total_strikes_landed": ""}, {"total_strikes_landed": "-1"},
    {"total_strikes_landed": "1.5"}, {"total_strikes_landed": "garbage"},
    {"total_strikes_landed": None},
])
def test_incomplete_or_misbound_output_remains_eligible(make_spider, change):
    rows = [aggregate_row(), aggregate_row(1, **change)]
    assert make_spider(rows).get_unknown_urls([URL]) == [URL]


@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_revisions_never_choose_first_last_or_newest(make_spider, reverse):
    rows = [aggregate_row(), aggregate_row(1),
            aggregate_row(total_strikes_landed="1", scraped_at="2026-09-02 00:00:00 UTC")]
    assert make_spider(rows[::-1] if reverse else rows).get_unknown_urls([URL]) == [URL]


def test_more_than_two_participants_is_ambiguous(make_spider):
    assert make_spider([aggregate_row(n) for n in range(3)]).get_unknown_urls([URL]) == [URL]


def test_different_exact_urls_are_not_reconciled(make_spider):
    # Existing UUID utility equates www; selection must not reconcile differing rows.
    rows = [aggregate_row(), aggregate_row(1, url=URL.replace("www.", ""))]
    assert make_spider(rows).get_unknown_urls([URL]) == [URL]


def test_http_https_and_other_detail_ids_remain_distinct(make_spider):
    spider = make_spider([aggregate_row(), aggregate_row(1)])
    aliases = [URL.replace("http:", "https:"), URL + "-other"]
    assert spider.get_unknown_urls([URL, *aliases]) == aliases


@pytest.mark.parametrize("shape", ["missing", "empty", "header-only", "missing-column", "duplicate-header", "short-row", "long-row"])
def test_missing_empty_and_malformed_files_supply_no_completion(make_spider, shape):
    make_spider([aggregate_row(), aggregate_row(1)])
    path = make_spider.parsed
    if shape == "missing":
        path.unlink()
    elif shape == "empty":
        path.write_text("")
    elif shape == "header-only":
        write_csv(path, [])
    elif shape == "missing-column":
        write_csv(path, [{"fight_id": FIGHT_ID, "fighter_id": side} for side in FIGHTER_IDS[:2]],
                  ["fight_id", "fighter_id"])
    elif shape == "duplicate-header":
        write_csv(path, [aggregate_row(), aggregate_row(1)], HEADERS + ["fighter_id"])
    else:
        with path.open("a") as stream:
            stream.write(",," + FIGHT_ID + "\n" if shape == "short-row" else
                         ",".join(aggregate_row().values()) + ",unexpected\n")
    spider = CrawlFightStats(incremental="1", existing_csv=str(path))
    assert spider.get_unknown_urls([URL]) == [URL]


@pytest.mark.parametrize("incremental", ["0", "false", "1", "true"])
def test_queue_and_listing_discovery_use_same_rule(make_spider, incremental):
    complete = URL + "-complete"
    partial = URL + "-partial"
    urls = [URL, partial, complete]
    spider = make_spider([aggregate_row(url=partial), aggregate_row(url=complete),
                          aggregate_row(1, url=complete)], incremental=incremental)
    write_csv(make_spider.queue, [{"fight_url": url} for url in urls], ["fight_url"])
    expected = urls[:2] if incremental in {"1", "true"} else urls
    assert [r.url for r in spider.start_requests()] == expected
    assert all(r.callback == spider._get_fight_stats for r in spider._start_from_queue())
    make_spider.queue.unlink()
    seeds = list(spider.start_requests())
    assert [r.url for r in seeds] == spider.start_urls
    listing = HtmlResponse(url=seeds[0].url, encoding="utf-8",
                           body=b'<a href="http://ufcstats.com/event-details/synthetic">event</a>')
    cards = list(spider.parse(listing))
    assert len(cards) == 1 and cards[0].callback == spider._get_fight_urls
    card = HtmlResponse(url=cards[0].url, encoding="utf-8",
                        body="".join(f'<a href="{url}">fight</a>' for url in urls).encode())
    assert [r.url for r in spider._get_fight_urls(card)] == expected


@pytest.mark.parametrize("queue_exists", [False, True])
@pytest.mark.parametrize("incremental", ["0", "1"])
def test_single_fight_override_bypasses_completion_queue_and_listing(make_spider, queue_exists, incremental):
    spider = make_spider([aggregate_row(), aggregate_row(1)], incremental=incremental, fight_url=" " + URL + " ")
    if queue_exists:
        make_spider.queue.write_text("unreadable-queue-for-this-test\n")
    requests = list(spider.start_requests())
    assert [r.url for r in requests] == [URL]
    assert requests[0].callback == spider._get_fight_stats
    assert spider._skipped_count == 0


def test_existing_empty_queue_has_priority_over_listing(make_spider):
    spider = make_spider()
    write_csv(make_spider.queue, [], ["fight_url"])
    assert list(spider.start_requests()) == []


def test_shared_mixin_still_skips_successful_capture_without_output(make_spider):
    class OtherConsumer(IncrementalCrawlMixin, scrapy.Spider):
        name = "synthetic_other_consumer"
        id_column = "fight_id"

    make_spider()
    spider = OtherConsumer(incremental="1", existing_csv=str(make_spider.parsed))
    assert spider.captured_uuids == {FIGHT_ID}
    assert spider.get_unknown_urls([URL]) == []


def test_guards_reject_network_and_real_warehouse():
    with pytest.raises(AssertionError, match="forbidden"):
        socket.create_connection(("synthetic.invalid", 443))
    with pytest.raises(AssertionError, match="forbidden"):
        socket.getaddrinfo("synthetic.invalid", 443)
    with pytest.raises(AssertionError, match="forbidden"):
        importlib.import_module("warehouse.db")

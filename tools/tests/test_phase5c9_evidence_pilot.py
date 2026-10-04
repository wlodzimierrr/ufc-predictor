"""Offline mocks for the single Phase 5C.9 acquisition attempt."""

import json
from pathlib import Path
import socket
import subprocess
import sys
from urllib.robotparser import RobotFileParser

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import collect_phase5c9_allen_duncan as pilot  # noqa: E402
from scrapy.http import Response  # noqa: E402
from ufc_scraper.raw_capture_v2 import CaptureError  # noqa: E402

URL = "http://www.ufcstats.com/event-details/7f98d9d5a10fa25c"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("network forbidden in pilot mocks")
    for name in ("create_connection", "getaddrinfo", "gethostbyname"):
        monkeypatch.setattr(socket, name, denied)
    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, denied)


def make(tmp_path):
    (tmp_path / "ledger").mkdir()
    (tmp_path / "support").mkdir()
    return pilot.Pilot(tmp_path, {"initial_requests": [["event", URL]],
        "target_detail_url": "http://www.ufcstats.com/fight-details/7db1a3dac7e343e7",
        "target_profile_link_urls": ["http://www.ufcstats.com/fighter-details/2f181c0467965b98",
                                     "http://www.ufcstats.com/fighter-details/a93f94c923c3a9cb"]})


@pytest.mark.parametrize("status,body,expected", [
    (403, b"denied", "ACCESS_DENIED"), (429, b"", "RATE_LIMIT"),
    (200, b"CAPTCHA", "ACCESS_CHALLENGE_OR_RATE_LIMIT"),
    (200, b"Checking your browser /__c", "ACCESS_CHALLENGE_OR_RATE_LIMIT"),
    (200, b"verify you are human", "ACCESS_CHALLENGE_OR_RATE_LIMIT"),
    (503, b"service unavailable", "ESSENTIAL_HTTP_ERROR"),
])
def test_first_response_stops_and_preserves_failure(tmp_path, status, body, expected):
    p = make(tmp_path)
    request = p.request(p.queue.popleft())
    assert request.url == "http://www.ufcstats.com/robots.txt"
    pilot.CaptureAt200.process_request(pilot.CaptureAt200.__new__(pilot.CaptureAt200), request, None)
    p.response(request, Response(request.url, status=status, body=body))
    assert p.reason == expected
    assert p.count == 1
    result = json.loads((tmp_path / "ledger/001.result.json").read_bytes())
    assert (tmp_path / result["support_body"]).read_bytes() == body
    assert result["receipt"] is None
    assert not (tmp_path / "v2").exists()


@pytest.mark.parametrize("destination", [
    "https://foreign.example/robots.txt", "http://user:password@www.ufcstats.com/robots.txt",
    "http://www.ufcstats.com/robots.txt?token=secret", "/other-path", "/robots.txt#fragment",
])
def test_unsafe_redirect_is_preserved_without_following(tmp_path, destination):
    p = make(tmp_path)
    request = p.request(p.queue.popleft())
    pilot.CaptureAt200.process_request(pilot.CaptureAt200.__new__(pilot.CaptureAt200), request, None)
    p.response(request, Response(request.url, status=302, body=b"redirect",
                                headers={"Location": destination}))
    assert p.reason == "UNSAFE_OR_UNAPPROVED_REDIRECT"
    result = (tmp_path / "ledger/001.result.json").read_text()
    assert destination not in result
    assert p.count == 1


def test_permitted_redirect_keeps_http_and_https_distinct(tmp_path):
    p = make(tmp_path)
    request = p.request(p.queue.popleft())
    pilot.CaptureAt200.process_request(pilot.CaptureAt200.__new__(pilot.CaptureAt200), request, None)
    destination = "https://ufcstats.com/robots.txt"
    p.response(request, Response(request.url, status=301, body=b"redirect",
                                headers={"Location": destination}))
    assert p.reason is None
    second = p.request(p.queue.popleft())
    assert second.meta["pilot_chain"] == [request.url, destination]
    assert p.count == 2
    assert pilot.redirect_url(URL, URL.replace("http:", "https:"), [URL]) != URL
    with pytest.raises(ValueError):
        pilot.redirect_url(URL.replace("http:", "https:"), URL, [URL.replace("http:", "https:")])


def test_robots_disallow_stops_before_content(tmp_path):
    p = make(tmp_path)
    r = p.request(p.queue.popleft())
    pilot.CaptureAt200.process_request(pilot.CaptureAt200.__new__(pilot.CaptureAt200), r, None)
    p.response(r, Response(r.url, body=b"User-agent: *\nDisallow: /\n"))
    assert p.request(p.queue.popleft()) is None
    assert p.count == 1
    assert p.reason == "ROBOTS_DISALLOWED"


def test_network_exception_has_no_invented_response(tmp_path):
    p = make(tmp_path)
    policy = RobotFileParser()
    policy.parse(["User-agent: *", "Allow: /"])
    p.robots[pilot.origin(URL)] = policy
    r = p.request(p.queue.popleft())
    pilot.CaptureAt200.process_request(pilot.CaptureAt200.__new__(pilot.CaptureAt200), r, None)
    p.exception(r, TimeoutError("Cookie: secret"))
    result = json.loads((tmp_path / "ledger/001.result.json").read_bytes())
    resolved = p.store.resolve_receipt(result["receipt"], require_success=False)
    assert resolved.body is None
    assert resolved.receipt["http_status"] is None
    assert resolved.receipt["exception_category"] == "timeout"
    assert "secret" not in json.dumps(resolved.receipt)
    assert p.reason == "NETWORK_FAILURE"


@pytest.mark.parametrize("status,body", [(403, b"access denied"),
    (200, b"Checking your browser /__c"), (302, b"redirect")])
def test_content_receipts_keep_accepted_boundary_and_failed_disposition(tmp_path, status, body):
    p = make(tmp_path)
    policy = RobotFileParser()
    policy.parse(["User-agent: *", "Allow: /"])
    p.robots[pilot.origin(URL)] = policy
    r = p.request(p.queue.popleft())
    pilot.CaptureAt200.process_request(pilot.CaptureAt200.__new__(pilot.CaptureAt200), r, None)
    p.response(r, Response(r.url, status=status, body=body))
    result = json.loads((tmp_path / "ledger/001.result.json").read_bytes())
    resolved = p.store.resolve_receipt(result["receipt"],
        expected_receipt_sha256=result["receipt_sha256"], require_success=False)
    assert resolved.body == body
    assert resolved.receipt["boundary"] == "scrapy_downloader_priority_200_v1"
    assert resolved.receipt["disposition"] == "FAILED"
    with pytest.raises(CaptureError):
        p.store.resolve_receipt(result["receipt"])


@pytest.mark.parametrize("budget", ["count", "elapsed", "window"])
def test_limits_refuse_additional_attempts(tmp_path, monkeypatch, budget):
    p = make(tmp_path)
    if budget == "count":
        p.count = 64
    elif budget == "elapsed":
        p.monotonic_start -= 601
    else:
        monkeypatch.setattr(pilot, "CLOSES", pilot.datetime(2000, 1, 1, tzinfo=pilot.timezone.utc))
    assert p.request(p.queue.popleft()) is None
    assert not list((tmp_path / "ledger").iterdir())


def test_exclusive_attempt_and_changed_target(tmp_path):
    p = make(tmp_path)
    policy = RobotFileParser()
    policy.parse(["User-agent: *", "Allow: /"])
    p.robots[pilot.origin(URL)] = policy
    r = p.request(p.queue.popleft())
    pilot.CaptureAt200.process_request(pilot.CaptureAt200.__new__(pilot.CaptureAt200), r, None)
    p.response(r, Response(r.url, body=b"<html>Date: October 11, 2026</html>"))
    assert p.reason == "CHANGED_OR_UNVERIFIABLE_TARGET_DATE_OR_CARD"
    path = tmp_path / "attempt-start.json"
    pilot.publish(path, {"consumed": True})
    with pytest.raises(FileExistsError):
        pilot.publish(path, {"retry": True})


def test_real_engine_with_mocked_handler_stops_after_first_block(tmp_path):
    # Separate reactor lifetime; actual priority-200 wrapper, fake public response.
    script = '''
import socket, sys, json
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import collect_phase5c9_allen_duncan as p
from scrapy.http import Response
from twisted.internet.defer import succeed
def denied(*args, **kwargs): raise AssertionError("network forbidden")
socket.socket.connect = denied
socket.create_connection = denied
socket.getaddrinfo = denied
def mocked(self, request, spider):
    return succeed(Response(request.url, status=403, body=b"access denied"))
p.OneAttemptHTTP.download_request = mocked
bundle = Path(sys.argv[2]); (bundle / "ledger").mkdir(); (bundle / "support").mkdir()
state = p.Pilot(bundle, {"initial_requests": [["event", sys.argv[3]]]})
process = p.CrawlerProcess(p.SETTINGS, install_root_handler=False)
process.crawl(p.EvidenceSpider, pilot=state)
process.start(install_signal_handlers=False)
result = json.loads((bundle / "acquisition-result.json").read_bytes())
assert result["attempt_count"] == 1, result
assert result["stop_reason"] == "ACCESS_DENIED", result
row = json.loads((bundle / "ledger/001.result.json").read_bytes())
assert row["receipt"] is None
assert (bundle / row["support_body"]).read_bytes() == b"access denied"
print("mock engine passed")
'''
    result = subprocess.run([sys.executable, "-c", script, str(Path(pilot.__file__).parent),
                             str(tmp_path), URL], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "mock engine passed" in result.stdout

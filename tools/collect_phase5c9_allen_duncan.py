"""One frozen evidence-only pilot. No settings/project/pipeline loading.

Responses and exceptions enter the accepted writer at actual downloader priority
200. Robots bytes use a separate sidecar: /robots.txt is outside the accepted
writer's URL schema. No accepted receipt is invented for supporting reads.
The external ledger records attempts before download, including incomplete ones.
Run only against this session's exclusive, pre-frozen bundle; never resume it.
"""

from collections import deque
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.parse import urljoin, urlsplit
from urllib.robotparser import RobotFileParser

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scraper/UFC-Web-Scraping-main/ufc_scraper"))
from scrapy import Request, Spider  # noqa: E402
from scrapy.core.downloader.handlers.http11 import HTTP11DownloadHandler  # noqa: E402
from scrapy.crawler import CrawlerProcess  # noqa: E402
from scrapy.utils.defer import maybe_deferred_to_future  # noqa: E402
from ufc_scraper.raw_capture_v2 import (  # noqa: E402
    CaptureError, CaptureStoreV2, ImmutableRawCaptureMiddlewareV2,
    _exception_category, _public_url,
)

USER_AGENT = "ufc-data-phase5c9-evidence-pilot/1.0"
CLOSES = datetime(2026, 10, 9, tzinfo=timezone.utc)
SETTINGS = {
    "DOWNLOADER_MIDDLEWARES_BASE": {},
    "DOWNLOADER_MIDDLEWARES": {"collect_phase5c9_allen_duncan.CaptureAt200": 200},
    "DOWNLOAD_HANDLERS": {
        "http": "collect_phase5c9_allen_duncan.OneAttemptHTTP",
        "https": "collect_phase5c9_allen_duncan.OneAttemptHTTP",
    },
    "SPIDER_MIDDLEWARES_BASE": {}, "SPIDER_MIDDLEWARES": {},
    "EXTENSIONS_BASE": {}, "EXTENSIONS": {}, "ITEM_PIPELINES": {},
    "CONCURRENT_REQUESTS": 1, "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    "DOWNLOAD_TIMEOUT": 15, "DOWNLOAD_MAXSIZE": 2097152,
    "DOWNLOAD_FAIL_ON_DATALOSS": True,
    "DOWNLOAD_TLS_CONTEXT_FACTORY": "scrapy.core.downloader.contextfactory.BrowserLikeContextFactory",
    "RETRY_ENABLED": False, "REDIRECT_ENABLED": False,
    "METAREFRESH_ENABLED": False, "HTTPCACHE_ENABLED": False,
    "COOKIES_ENABLED": False, "HTTPPROXY_ENABLED": False,
    "ROBOTSTXT_OBEY": False,  # Explicit ledger-counted robots gate below.
    "DNSCACHE_ENABLED": False, "LOG_ENABLED": False,
    "TELNETCONSOLE_ENABLED": False,
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
}


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def publish_bytes(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fchmod(stream.fileno(), 0o444)
        os.fsync(stream.fileno())
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def publish(path, value):
    publish_bytes(path, (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode())


def public_url(url):
    parts = urlsplit(url)
    if parts.path == "/robots.txt" and not parts.query and not parts.fragment:
        _public_url(f"{parts.scheme}://{parts.netloc}/")
        return url
    return _public_url(url)


def origin(url):
    p = urlsplit(public_url(url))
    return f"{p.scheme}://{p.netloc}"


def redirect_url(url, location, chain):
    candidate = public_url(urljoin(url, location))
    old, new = urlsplit(url), urlsplit(candidate)
    # Only host-family/scheme redirects for the same exact path/query. No ID
    # replacement, HTTPS downgrade, foreign host, credentials or loop.
    if (old.path != new.path or old.query != new.query
            or old.scheme == "https" and new.scheme != "https"
            or candidate in chain or len(chain) >= 4):
        raise ValueError("unapproved_redirect")
    return candidate


def stop_reason(status, body, headers):
    text = body.lower()
    if status == 429:
        return "RATE_LIMIT"
    if status in {401, 403, 407, 451}:
        return "ACCESS_DENIED"
    markers = (b"checking your browser", b"captcha", b"access denied",
               b"verify you are human", b"challenge-platform", b"/__c",
               b"just a moment", b"rate limit", b"too many requests")
    if any(marker in text for marker in markers):
        return "ACCESS_CHALLENGE_OR_RATE_LIMIT"
    if status >= 400:
        return "ESSENTIAL_HTTP_ERROR"
    if headers.get("Age", "0") not in {"", "0"}:
        return "UPSTREAM_CACHE_AGE_UNVERIFIED"
    return None


class OneAttemptHTTP(HTTP11DownloadHandler):
    def __init__(self, settings, crawler):
        super().__init__(settings, crawler)
        self._pool.persistent = False
        self._pool.retryAutomatically = False


class Pilot:
    def __init__(self, bundle, plan):
        self.bundle, self.plan = bundle, plan
        self.store = CaptureStoreV2(bundle / "v2")
        self.count = 0
        self.started = now()
        self.monotonic_start = time.monotonic()
        self.last_finished = None
        self.reason = None
        self.robots = {}
        self.queue = deque((role, url, [url]) for role, url in plan["initial_requests"])

    def remaining(self):
        return 600 - (time.monotonic() - self.monotonic_start)

    def request(self, task):
        role, url, chain = task
        public_url(url)
        if self.count >= 64 or self.remaining() <= 0:
            self.reason = "BUDGET_EXHAUSTED"
            return None
        if datetime.now(timezone.utc) >= CLOSES:
            self.reason = "PROSPECTIVE_WINDOW_CLOSED"
            return None
        if role != "robots":
            policy = self.robots.get(origin(url))
            if policy is None:
                self.queue.appendleft(task)
                robot_url = origin(url) + "/robots.txt"
                return self.request(("robots", robot_url, [robot_url]))
            if not policy.can_fetch(USER_AGENT, url):
                self.reason = "ROBOTS_DISALLOWED"
                return None
        self.count += 1
        request = Request(url, dont_filter=True, headers={
            "User-Agent": USER_AGENT, "Accept": "text/html,text/plain",
            "Accept-Encoding": "identity", "Cache-Control": "no-cache, no-store",
            "Pragma": "no-cache", "Connection": "close",
        }, meta={"pilot_sequence": self.count, "pilot_role": role,
                 "pilot_chain": chain, "download_timeout": min(15, self.remaining()),
                 "dont_retry": True, "dont_redirect": True, "dont_cache": True})
        publish(self.bundle / "ledger" / f"{self.count:03d}.request.json", {
            "sequence": self.count, "role": role, "url": url, "chain": chain,
            "scheduled_at": now(), "state": "ATTEMPT_RESERVED_BEFORE_DOWNLOAD",
        })
        return request

    def response(self, request, response):
        clock = request.meta[ImmutableRawCaptureMiddlewareV2._REQUEST_CLOCK]
        path, support = None, None
        observed = now()
        if request.meta["pilot_role"] == "robots":
            support = f'support/{request.meta["pilot_sequence"]:03d}.body'
            publish_bytes(self.bundle / support, response.body)
        else:
            path = self.store.capture_response(
                response.url, response.body, response.status,
                request_url=request.url, request_started_at=clock,
                from_cache="cached" in response.flags,
            )
        headers = {}
        for key in ("Date", "Age", "Cache-Control", "Content-Encoding"):
            if key in response.headers:
                headers[key] = response.headers[key].decode("ascii", errors="replace")
        reason = stop_reason(response.status, response.body, headers)
        location = None
        if "cached" in response.flags:
            reason = "CACHED_RESPONSE_REFUSED"
        if headers.get("Content-Encoding", "identity").lower() not in {"identity", ""}:
            reason = reason or "UNEXPECTED_CONTENT_ENCODING"
        if not reason and 300 <= response.status < 400:
            try:
                location = redirect_url(request.url, response.headers["Location"].decode("ascii"),
                                        request.meta["pilot_chain"])
            except (CaptureError, KeyError, ValueError, UnicodeError):
                reason = "UNSAFE_OR_UNAPPROVED_REDIRECT"
        elif not reason and not 200 <= response.status < 300:
            reason = "ESSENTIAL_HTTP_ERROR"
        publish(self.bundle / "ledger" / f'{request.meta["pilot_sequence"]:03d}.result.json', {
            "sequence": request.meta["pilot_sequence"], "request_url": request.url,
            "source_url": response.url, "status": response.status,
            "middleware_request_started_at": clock, "ledger_completed_at": now(),
            "receipt": path, "receipt_sha256": sha((self.store.root / path).read_bytes()) if path else None,
            "support_body": support, "pilot_response_observed_at": observed,
            "accepted_capture_state": "RECEIPT_PUBLISHED" if path else "SUPPORT_URL_OUTSIDE_ACCEPTED_SCHEMA",
            "body_sha256": sha(response.body), "body_bytes": len(response.body),
            "safe_response_headers": headers, "permitted_redirect_url": location,
            "location_present": "Location" in response.headers,
            "pilot_stop_reason": reason,
        })
        self.last_finished = time.monotonic()
        self.reason = reason
        if reason:
            return
        role, chain = request.meta["pilot_role"], request.meta["pilot_chain"]
        if location:
            self.queue.appendleft((role, location, [*chain, location]))
        elif role == "robots":
            policy = RobotFileParser()
            policy.parse(response.body.decode("utf-8").splitlines())
            delay = policy.crawl_delay(USER_AGENT)
            rate = policy.request_rate(USER_AGENT)
            if delay is not None and delay > 2 or rate is not None and rate.seconds / rate.requests > 2:
                self.reason = "ROBOTS_REQUIRES_SLOWER_PILOT"
                return
            for url in chain:
                self.robots[origin(url)] = policy
        elif role == "event":
            from parsel import Selector
            s = Selector(body=response.body)
            dates = [" ".join(row.xpath('.//text()').getall()).split()
                     for row in s.css("li.b-list__box-list-item")
                     if row.css("i").xpath('normalize-space(.)').get() == "Date:"]
            cards = s.css('tr[data-link]')
            target = [row for row in cards if row.attrib.get("data-link") == self.plan["target_detail_url"]]
            expected = self.plan["target_profile_link_urls"]
            if dates != [["Date:", "October", "10,", "2026"]] or len(target) != 1:
                self.reason = "CHANGED_OR_UNVERIFIABLE_TARGET_DATE_OR_CARD"
            elif sorted(target[0].css('a[href*="fighter-details/"]::attr(href)').getall()) != sorted(expected):
                self.reason = "CHANGED_TARGET_ROSTER"
        elif role == "fight":
            from parsel import Selector
            participants = Selector(body=response.body).css(
                '.b-fight-details__person-link::attr(href)').getall()
            if participants != self.plan["target_profile_link_urls"]:
                self.reason = "CHANGED_TARGET_ROSTER_OR_ORIENTATION"

    def exception(self, request, exception):
        clock = request.meta.get(ImmutableRawCaptureMiddlewareV2._REQUEST_CLOCK)
        path = None if request.meta["pilot_role"] == "robots" else self.store.capture_exception(
            request.url, exception, request_started_at=clock)
        publish(self.bundle / "ledger" / f'{request.meta["pilot_sequence"]:03d}.result.json', {
            "sequence": request.meta["pilot_sequence"], "request_url": request.url,
            "status": None, "response_present": False, "receipt": path,
            "receipt_sha256": sha((self.store.root / path).read_bytes()) if path else None,
            "exception_category": _exception_category(exception),
            "middleware_request_started_at": clock,
            "accepted_capture_state": "RECEIPT_PUBLISHED" if path else "SUPPORT_URL_OUTSIDE_ACCEPTED_SCHEMA",
            "ledger_completed_at": now(), "pilot_stop_reason": "NETWORK_FAILURE",
        })
        self.reason = "NETWORK_FAILURE"
        self.last_finished = time.monotonic()


class CaptureAt200(ImmutableRawCaptureMiddlewareV2):
    @classmethod
    def from_crawler(cls, crawler):
        return cls.__new__(cls)

    def process_response(self, request, response, spider):
        spider.pilot.response(request, response)
        return response

    def process_exception(self, request, exception, spider):
        spider.pilot.exception(request, exception)


class EvidenceSpider(Spider):
    name = "phase5c9_allen_duncan_only"

    def __init__(self, pilot, **kwargs):
        super().__init__(**kwargs)
        self.pilot = pilot

    async def start(self):
        from twisted.internet import reactor
        from twisted.internet.task import deferLater
        p = self.pilot
        try:
            while p.queue and not p.reason:
                if p.last_finished is not None:
                    pause = max(0, 2 - (time.monotonic() - p.last_finished))
                    if pause >= p.remaining():
                        p.reason = "BUDGET_EXHAUSTED"
                        break
                    await maybe_deferred_to_future(deferLater(reactor, pause, lambda: None))
                request = p.request(p.queue.popleft())
                if request is None:
                    break
                download = self.crawler.engine.download(request)
                download.addTimeout(min(15, max(0.001, p.remaining())), reactor)
                await maybe_deferred_to_future(download)
        except Exception:
            p.reason = p.reason or "INTERRUPTED_OR_INTERNAL_FAILURE"
        finally:
            publish(p.bundle / "acquisition-result.json", {
                "started_at": p.started, "completed_at": now(), "attempt_count": p.count,
                "stop_reason": p.reason or "DECLARED_READS_FINISHED_UNQUALIFIED",
                "elapsed_seconds": time.monotonic() - p.monotonic_start,
                "authorization_consumed": True,
            })
        if False:
            yield None


def main(bundle):
    # Exclusivity is consumed before validation, including validation failure.
    publish(bundle / "attempt-start.json", {"started_at": now(), "authorization_consumed": True})
    freeze = json.loads((bundle / "freeze.json").read_bytes())
    for name, expected in freeze["repository_code_sha256"].items():
        if sha((ROOT / name).read_bytes()) != expected:
            raise ValueError("frozen_code_mismatch")
    for name, expected in freeze["bundle_sha256"].items():
        if sha((bundle / name).read_bytes()) != expected:
            raise ValueError("frozen_bundle_mismatch")
    if json.loads((bundle / "settings.json").read_bytes()) != SETTINGS:
        raise ValueError("frozen_settings_mismatch")
    if datetime.now(timezone.utc) >= CLOSES:
        publish(bundle / "acquisition-result.json", {"completed_at": now(), "attempt_count": 0,
                "stop_reason": "PROSPECTIVE_WINDOW_CLOSED", "authorization_consumed": True})
        return
    plan = json.loads((bundle / "plan.json").read_bytes())
    pilot = Pilot(bundle, plan)
    sys.modules["collect_phase5c9_allen_duncan"] = sys.modules[__name__]
    process = CrawlerProcess(SETTINGS, install_root_handler=False)
    process.crawl(EvidenceSpider, pilot=pilot)
    process.start(install_signal_handlers=False)
    if not (bundle / "acquisition-result.json").exists():
        publish(bundle / "acquisition-result.json", {"completed_at": now(), "attempt_count": pilot.count,
                "stop_reason": "ENGINE_START_OR_INTERRUPTION_FAILURE", "authorization_consumed": True})


if __name__ == "__main__":
    try:
        main(Path(sys.argv[1]))
    except Exception:
        print('{"status":"REFUSED_OR_INTERRUPTED","retry_authorized":false}')
        raise SystemExit(2) from None

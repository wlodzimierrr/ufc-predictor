# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import csv
import hashlib
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from scrapy import signals

from utils import get_uuid_string

# useful for handling different item types with a single interface


class RawCaptureMiddleware:
    """Downloader middleware that persists raw HTML and records fetch metadata.

    For every HTTP response whose URL matches a known UFC entity type, this
    middleware:
      - writes the raw HTML body to data/raw/ufcstats/<entity_type>/<id>.html;
      - appends one manifest row to data/manifests/fetch_manifest.csv.

    For network-level failures (connection errors, timeouts) it appends a
    failure row to the manifest without writing a raw file.

    Seed / discovery pages (A-Z fighter listing, event listing index) are
    passed through unchanged with no manifest entry — they are not entity
    detail pages.

    Registration priority: 200.  RetryMiddleware runs at 550, and because
    process_response is called in *decreasing* priority order, 550 executes
    before 200.  This means only the final response (after any retries) is
    captured here; transient retried responses are never written to disk.
    """

    _ENTITY_PATTERNS: dict[str, str] = {
        "event-details": "event",
        "fight-details": "fight",
        "fighter-details": "fighter",
        "statistics/events/completed": "event_listing",
    }

    _ENTITY_SUBDIRS: dict[str, str] = {
        "event": "events",
        "fight": "fights",
        "fighter": "fighters",
        "event_listing": "event_listing",
    }

    _MANIFEST_FIELDS: list[str] = [
        "job_run_id",
        "entity_type",
        "source_url",
        "fetched_at",
        "http_status",
        "content_hash",
        "storage_path",
        "fetch_status",
        "error_message",
    ]

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir
        self._job_run_id = str(uuid.uuid4())
        self._manifest_path = data_dir / "manifests" / "fetch_manifest.csv"
        self._lock = threading.Lock()
        self._stats: dict[str, int] = {
            "fetched": 0,
            "unchanged": 0,
            "updated": 0,
            "failed": 0,
        }

    @classmethod
    def from_crawler(cls, crawler):
        # Resolve the repo-root data/ directory from this source file's
        # location so the path is independent of the working directory.
        # middlewares.py: parents[4] == repo root (ufc-data/).
        data_dir = Path(__file__).resolve().parents[4] / "data"
        instance = cls(data_dir=data_dir)
        crawler.signals.connect(instance._spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(instance._spider_closed, signal=signals.spider_closed)
        return instance

    # ------------------------------------------------------------------
    # Scrapy lifecycle hooks
    # ------------------------------------------------------------------

    def _spider_opened(self, spider) -> None:
        """Create output directories and write the manifest header if needed."""
        (self._data_dir / "manifests").mkdir(parents=True, exist_ok=True)
        for subdir in self._ENTITY_SUBDIRS.values():
            (self._data_dir / "raw" / "ufcstats" / subdir).mkdir(
                parents=True, exist_ok=True
            )
        if not self._manifest_path.exists():
            with self._manifest_path.open("w", newline="", encoding="utf-8") as fh:
                csv.DictWriter(fh, fieldnames=self._MANIFEST_FIELDS).writeheader()
        spider.logger.info(
            "RawCaptureMiddleware active | job_run_id=%s | manifest=%s",
            self._job_run_id,
            self._manifest_path,
        )

    def _spider_closed(self, spider) -> None:
        spider.logger.info(
            "RawCaptureMiddleware summary | job_run_id=%s | fetched=%d | "
            "unchanged=%d | updated=%d | failed=%d",
            self._job_run_id,
            self._stats["fetched"],
            self._stats["unchanged"],
            self._stats["updated"],
            self._stats["failed"],
        )

    def process_response(self, request, response, spider):
        entity_type = self._classify_url(response.url)
        if entity_type is None:
            # Seed / discovery URL — pass through, do not capture.
            return response

        now = _utc_now()

        if response.status >= 400:
            self._stats["failed"] += 1
            self._append_manifest(
                entity_type=entity_type,
                source_url=response.url,
                fetched_at=now,
                http_status=response.status,
                content_hash="",
                storage_path="",
                fetch_status="failed",
                error_message=f"HTTP {response.status}",
            )
            return response

        content = response.body
        content_hash = hashlib.sha256(content).hexdigest()
        raw_path, fetch_status = self._write_raw(entity_type, response.url, content, content_hash)

        self._stats[fetch_status] += 1
        self._append_manifest(
            entity_type=entity_type,
            source_url=response.url,
            fetched_at=now,
            http_status=response.status,
            content_hash=content_hash,
            storage_path=str(raw_path.relative_to(self._data_dir.parent)),
            fetch_status=fetch_status,
            error_message="",
        )
        return response

    def process_exception(self, request, exception, spider):
        """Record network-level failures after retries are exhausted."""
        entity_type = self._classify_url(request.url) or "unknown"
        self._stats["failed"] += 1
        self._append_manifest(
            entity_type=entity_type,
            source_url=request.url,
            fetched_at=_utc_now(),
            http_status=0,
            content_hash="",
            storage_path="",
            fetch_status="failed",
            error_message=f"{type(exception).__name__}: {exception}",
        )
        return None  # Allow Scrapy's normal exception handling to continue.

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @classmethod
    def _classify_url(cls, url: str) -> str | None:
        for pattern, entity_type in cls._ENTITY_PATTERNS.items():
            if pattern in url:
                return entity_type
        return None

    def _raw_path(self, entity_type: str, url: str) -> Path:
        base = self._data_dir / "raw" / "ufcstats" / self._ENTITY_SUBDIRS[entity_type]
        if entity_type == "event_listing":
            date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
            return base / f"event_listing_{date_str}.html"
        return base / f"{get_uuid_string(url)}.html"

    def _write_raw(
        self, entity_type: str, url: str, content: bytes, content_hash: str
    ) -> tuple[Path, str]:
        path = self._raw_path(entity_type, url)
        if path.exists():
            if hashlib.sha256(path.read_bytes()).hexdigest() == content_hash:
                return path, "unchanged"
            path.write_bytes(content)
            return path, "updated"
        path.write_bytes(content)
        return path, "fetched"

    def _append_manifest(self, **kwargs) -> None:
        row = {field: kwargs.get(field, "") for field in self._MANIFEST_FIELDS}
        row["job_run_id"] = self._job_run_id
        with self._lock:
            with self._manifest_path.open("a", newline="", encoding="utf-8") as fh:
                csv.DictWriter(fh, fieldnames=self._MANIFEST_FIELDS).writerow(row)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class UfcScraperSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # maching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class UfcScraperDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)

"""Helpers for incremental crawls against existing CSV exports."""

from __future__ import annotations

import csv
from pathlib import Path

from utils import get_uuid_string

TRUE_VALUES = {"1", "true", "yes", "y", "on"}

# fetch_status values that mean a page was successfully captured and does
# not need to be re-fetched in an incremental run.
_CAPTURED_STATUSES = {"fetched", "unchanged", "updated"}


class IncrementalCrawlMixin:
    """Skip already-captured pages on recrawl using manifest state and CSV IDs.

    Priority of skip sources (incremental mode only):
    1. fetch_manifest.csv — canonical; covers any page captured by
       RawCaptureMiddleware regardless of whether parsing succeeded.
    2. Existing parsed CSV — fallback for seeds that predate manifest
       capture or were loaded before T1.2.2 was deployed.

    A URL is skipped if its UUID appears in either source.
    Failed-only manifest entries are NOT skipped; they will be retried.
    """

    data_filename: str = ""
    id_column: str = ""

    def __init__(
        self,
        *args,
        incremental: str = "false",
        existing_csv: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.incremental = str(incremental).lower() in TRUE_VALUES
        self.existing_csv = self._resolve_existing_csv_path(existing_csv)
        self.known_ids = self._load_known_ids()
        self.captured_uuids = self._load_captured_uuids()
        self._skipped_count = 0

    # ------------------------------------------------------------------
    # Path resolution
    # ------------------------------------------------------------------

    def _resolve_existing_csv_path(self, existing_csv: str | None) -> Path:
        if existing_csv:
            return Path(existing_csv).expanduser().resolve()

        repo_root = Path(__file__).resolve().parents[5]
        return repo_root / "data" / self.data_filename

    def _resolve_manifest_path(self) -> Path:
        repo_root = Path(__file__).resolve().parents[5]
        return repo_root / "data" / "manifests" / "fetch_manifest.csv"

    # ------------------------------------------------------------------
    # ID / UUID loading
    # ------------------------------------------------------------------

    def _load_known_ids(self) -> set[str]:
        if not self.incremental or not self.existing_csv.exists():
            return set()

        with self.existing_csv.open(newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            return {
                row[self.id_column].strip()
                for row in reader
                if row.get(self.id_column, "").strip()
            }

    def _load_captured_uuids(self) -> set[str]:
        """Return UUID set of pages successfully captured in fetch_manifest.csv.

        Only rows with fetch_status in {fetched, unchanged, updated} are
        included.  Failed rows are excluded so they will be retried.
        """
        if not self.incremental:
            return set()

        manifest = self._resolve_manifest_path()
        if not manifest.exists():
            return set()

        with manifest.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            return {
                get_uuid_string(row["source_url"])
                for row in reader
                if row.get("fetch_status") in _CAPTURED_STATUSES
                and row.get("source_url")
            }

    # ------------------------------------------------------------------
    # URL filtering
    # ------------------------------------------------------------------

    def get_unknown_urls(self, urls: list[str]) -> list[str]:
        if not self.incremental:
            return urls
        if not self.known_ids and not self.captured_uuids:
            return urls

        unknown = []
        for url in urls:
            uid = get_uuid_string(url)
            if uid in self.known_ids or uid in self.captured_uuids:
                self._skipped_count += 1
            else:
                unknown.append(url)
        return unknown

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def closed(self, reason: str) -> None:
        """Log incremental skip summary when the spider closes."""
        self.logger.info(
            "IncrementalCrawlMixin summary | skipped=%d | reason=%s",
            self._skipped_count,
            reason,
        )
        # Chain to any further closed() in the MRO (e.g. scrapy.Spider).
        super_closed = getattr(super(), "closed", None)
        if callable(super_closed):
            super_closed(reason)

"""Helpers for incremental crawls against existing CSV exports."""

from __future__ import annotations

import csv
from pathlib import Path

from utils import get_uuid_string

TRUE_VALUES = {"1", "true", "yes", "y", "on"}


class IncrementalCrawlMixin:
    """Load known record IDs from an existing CSV and skip them on recrawl."""

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

    def _resolve_existing_csv_path(self, existing_csv: str | None) -> Path:
        if existing_csv:
            return Path(existing_csv).expanduser().resolve()

        repo_root = Path(__file__).resolve().parents[5]
        return repo_root / "data" / self.data_filename

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

    def get_unknown_urls(self, urls: list[str]) -> list[str]:
        if not self.known_ids:
            return urls

        return [
            url for url in urls if get_uuid_string(url) not in self.known_ids
        ]

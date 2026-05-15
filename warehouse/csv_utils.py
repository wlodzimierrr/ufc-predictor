"""Helpers for reading scraper CSV outputs safely."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator


def iter_data_rows(path: Path) -> Iterator[dict[str, str]]:
    """Yield DictReader rows while skipping embedded repeated header rows.

    Some scraper update flows can append a fresh CSV header into the middle of an
    existing file. Those rows look like:
        {"event_id": "event_id", "name": "name", ...}
    and would otherwise crash downstream transforms.
    """
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if _is_repeated_header_row(row):
                continue
            yield row


def _is_repeated_header_row(row: dict[str, str] | None) -> bool:
    if not row:
        return False
    matches = sum(1 for k, v in row.items() if v == k)
    return matches >= max(3, len(row) // 3)

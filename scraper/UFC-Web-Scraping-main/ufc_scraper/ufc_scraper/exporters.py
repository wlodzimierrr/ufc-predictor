"""Custom feed exporters for scraper output."""

from __future__ import annotations

from scrapy.exporters import CsvItemExporter


class AppendSafeCsvItemExporter(CsvItemExporter):
    """CSV exporter that avoids writing a second header in append mode.

    Scrapy's built-in ``CsvItemExporter`` writes the header line once per export
    session. When the feed storage is opened in append mode (used by ``-o``),
    that creates an embedded duplicate header on every incremental run.

    If the target file already contains data, suppress the header for this
    exporter instance. Fresh files and overwrite runs still get the header.
    """

    def __init__(self, file, *args, **kwargs):
        has_existing_data = False
        tell = getattr(file, "tell", None)
        if callable(tell):
            try:
                has_existing_data = tell() > 0
            except OSError:
                has_existing_data = False

        if has_existing_data:
            kwargs["include_headers_line"] = False

        super().__init__(file, *args, **kwargs)

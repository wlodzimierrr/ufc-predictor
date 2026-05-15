from pathlib import Path

from ufc_scraper.exporters import AppendSafeCsvItemExporter


def test_append_safe_csv_exporter_skips_header_when_appending(tmp_path: Path) -> None:
    path = tmp_path / "events.csv"
    path.write_bytes(b"event_id,name\r\n1,Old Event\r\n")

    with path.open("ab") as f:
        exporter = AppendSafeCsvItemExporter(f)
        exporter.export_item({"event_id": "2", "name": "New Event"})
        exporter.finish_exporting()

    assert path.read_text(encoding="utf-8").splitlines() == [
        "event_id,name",
        "1,Old Event",
        "2,New Event",
    ]


def test_append_safe_csv_exporter_writes_header_for_new_file(tmp_path: Path) -> None:
    path = tmp_path / "events.csv"

    with path.open("wb") as f:
        exporter = AppendSafeCsvItemExporter(f)
        exporter.export_item({"event_id": "1", "name": "Only Event"})
        exporter.finish_exporting()

    assert path.read_text(encoding="utf-8").splitlines() == [
        "event_id,name",
        "1,Only Event",
    ]

"""Mock-only loader tests; database imports, CSV reads and networking are guarded."""

from copy import deepcopy
import importlib.util
from pathlib import Path
import socket
import sys
from types import ModuleType
from unittest.mock import MagicMock, Mock

import pytest

from warehouse import strict_fight_outcomes_v2 as strict


def fight_row(fight_id, status="completed", first="W", second="L"):
    return {"fight_id": fight_id, "event_id": "synthetic-event",
            "fighter_1_id": "synthetic-one", "fighter_2_id": "synthetic-two",
            "event_status": status, "fighter_1_outcome": first, "fighter_2_outcome": second,
            "scraped_at": "2026-08-16 19:52:20 UTC", "url": "https://synthetic.invalid/fight"}


@pytest.fixture
def mocked_loader(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Real database/network/file access forbidden in mocked loader tests")

    for name in ("socket", "create_connection", "getaddrinfo"):
        monkeypatch.setattr(socket, name, forbidden)

    # Replace warehouse.db before executing the loader module. The real helper
    # (including its dotenv import and psycopg2 connection) is never imported.
    database = ModuleType("warehouse.db")
    database.get_connection = Mock(side_effect=forbidden)
    database.upsert = Mock(side_effect=forbidden)
    monkeypatch.setitem(sys.modules, "warehouse.db", database)
    path = Path(__file__).resolve().parents[1] / "load_fights.py"
    spec = importlib.util.spec_from_file_location("phase5c4_mocked_fight_loader", path)
    loader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loader)

    conn = MagicMock(name="synthetic_connection")
    conn.__enter__.return_value = conn

    def finish_transaction(exc_type, exc, tb):
        if exc_type is None:
            conn.commit()
        return False

    conn.__exit__.side_effect = finish_transaction
    conn.cursor.side_effect = forbidden
    database.get_connection.side_effect = None
    database.get_connection.return_value = conn
    database.upsert.side_effect = None
    database.upsert.return_value = 5
    monkeypatch.setattr(loader, "_known_event_ids", Mock(return_value={"synthetic-event"}))
    monkeypatch.setattr(loader, "_known_fighter_ids", Mock(return_value={"synthetic-one", "synthetic-two"}))
    monkeypatch.setattr(loader, "iter_data_rows", Mock(side_effect=forbidden))
    return loader, conn, database


def test_valid_batch_reaches_one_upsert_after_complete_validation(mocked_loader):
    loader, conn, database = mocked_loader
    rows = [fight_row("upcoming", "upcoming", "", ""),
            fight_row("first-wins"), fight_row("second-wins", "completed", "L", "W"),
            fight_row("draw", "completed", "D", "D"),
            fight_row("nc", "completed", "NC", "NC")]
    # Historical compatibility must also work through the normal ingestion path.
    del rows[1]["event_status"]
    before = deepcopy(rows)
    seen = []

    def source(path):
        for row in rows:
            seen.append(row["fight_id"])
            yield row

    def write(connection, table, admitted, *, pk_columns):
        assert seen == [row["fight_id"] for row in rows]
        assert connection is conn
        assert table == "fights"
        assert pk_columns == ["fight_id"]
        assert admitted == [strict.transform_fight(row) for row in rows]
        return len(admitted)

    loader.iter_data_rows.side_effect = source
    database.upsert.side_effect = write
    loader.load_fights()
    assert loader.transform_fight is strict.transform_fight
    database.get_connection.assert_called_once_with()
    database.upsert.assert_called_once()
    conn.__enter__.assert_called_once()
    conn.__exit__.assert_called_once_with(None, None, None)
    conn.commit.assert_called_once_with()
    conn.close.assert_called_once_with()
    assert rows == before


@pytest.mark.parametrize("status,first,second,reason", [
    ("canceled", "", "", "canceled_status"),
    ("completed", "", "", "completed_without_outcomes"),
    (None, "", "", "missing_status_without_outcomes"),
    ("completed", "W", "", "partial_outcome_pair"),
    ("completed", "W", "W", "contradictory_outcome_pair"),
    ("completed", "WIN", "L", "unsupported_outcome_token"),
    ("upcoming", "W", "L", "upcoming_with_outcomes"),
    ("unknown", "W", "L", "unsupported_event_status"),
])
def test_late_invalid_row_prevents_all_upserts_and_commits(mocked_loader, status, first, second, reason):
    loader, conn, database = mocked_loader
    # More than the DB helper's 500-row chunk size: no early chunk may be written.
    rows = [fight_row(f"valid-{index}") for index in range(501)]
    rows.append(fight_row("late-invalid", status, first, second))
    before = deepcopy(rows)
    seen = []

    def source(path):
        for row in rows:
            seen.append(row["fight_id"])
            yield row

    loader.iter_data_rows.side_effect = source
    with pytest.raises(strict.FightOutcomeValidationError) as caught:
        loader.load_fights()
    assert caught.value.reason == reason
    assert caught.value.fight_id == "late-invalid"
    assert seen == [row["fight_id"] for row in rows]
    database.upsert.assert_not_called()
    conn.__enter__.assert_not_called()
    conn.__exit__.assert_not_called()
    conn.commit.assert_not_called()
    conn.close.assert_called_once_with()
    assert rows == before


def test_offline_guards_reject_real_network_and_unconfigured_source(mocked_loader):
    loader, conn, database = mocked_loader
    assert loader.get_connection is database.get_connection
    assert loader.upsert is database.upsert
    for operation in (lambda: socket.socket(),
                      lambda: socket.create_connection(("synthetic.invalid", 5432)),
                      lambda: socket.getaddrinfo("synthetic.invalid", 5432)):
        with pytest.raises(AssertionError, match="forbidden"):
            operation()
    with pytest.raises(AssertionError, match="forbidden"):
        loader.load_fights()
    database.upsert.assert_not_called()
    conn.commit.assert_not_called()
    conn.close.assert_called_once_with()

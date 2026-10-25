"""Connection configuration compatibility without opening a database connection."""

from unittest.mock import Mock

from warehouse import db


def test_database_url_takes_precedence_without_requiring_split_variables(monkeypatch):
    for key in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    uri = "postgresql://runtime:encoded%40password@warehouse:5432/ufc?sslmode=require"
    monkeypatch.setenv("DATABASE_URL", uri)
    connect = Mock(return_value=object())
    monkeypatch.setattr(db.psycopg2, "connect", connect)
    assert db.get_connection() is connect.return_value
    connect.assert_called_once_with(uri)


def test_existing_split_connection_configuration_remains_supported(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    for key, value in {
        "POSTGRES_HOST": "warehouse",
        "POSTGRES_PORT": "5544",
        "POSTGRES_DB": "ufc",
        "POSTGRES_USER": "runtime",
        "POSTGRES_PASSWORD": "synthetic-password",
    }.items():
        monkeypatch.setenv(key, value)
    connect = Mock()
    monkeypatch.setattr(db.psycopg2, "connect", connect)
    db.get_connection()
    connect.assert_called_once_with(
        host="warehouse", port=5544, dbname="ufc", user="runtime",
        password="synthetic-password",
    )

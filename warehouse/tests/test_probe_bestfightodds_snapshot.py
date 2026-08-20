"""Tests for the bounded BestFightOdds raw snapshot probe."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.probe_bestfightodds_snapshot import (
    DEFAULT_USER_AGENT,
    FetchResponse,
    ProbeTarget,
    probe_bestfightodds_snapshot,
    resolve_target,
    write_snapshot,
)


def test_resolve_target_accepts_explicit_event_identifier():
    target = resolve_target(event_id="ufc-330-4237")

    assert target.url == "https://www.bestfightodds.com/events/ufc-330-4237"
    assert target.target_type == "event"
    assert target.identifier == "ufc-330-4237"


def test_resolve_target_accepts_explicit_fighter_identifier():
    target = resolve_target(fighter_id="islam-makhachev-5541")

    assert target.url == "https://www.bestfightodds.com/fighters/islam-makhachev-5541"
    assert target.target_type == "fighter"
    assert target.identifier == "islam-makhachev-5541"


@pytest.mark.parametrize(
    "url,expected_url,target_type,identifier",
    [
        ("https://www.bestfightodds.com/events/ufc-330-4237", "https://www.bestfightodds.com/events/ufc-330-4237", "event", "ufc-330-4237"),
        ("https://bestfightodds.com/fighters/Georges-St-Pierre-80", "https://bestfightodds.com/fighters/Georges-St-Pierre-80", "fighter", "Georges-St-Pierre-80"),
        ("https://www.bestfightodds.com/archive", "https://www.bestfightodds.com/archive", "archive", "archive"),
        ("https://www.bestfightodds.com/", "https://www.bestfightodds.com/", "home", "home"),
    ],
)
def test_resolve_target_accepts_allowlisted_urls(url, expected_url, target_type, identifier):
    target = resolve_target(url=url)

    assert target.url == expected_url
    assert target.target_type == target_type
    assert target.identifier == identifier


@pytest.mark.parametrize(
    "url",
    [
        "http://www.bestfightodds.com/events/ufc-330-4237",
        "https://example.com/events/ufc-330-4237",
        "https://www.bestfightodds.com/api/ggd",
        "https://www.bestfightodds.com/cnadm/matchups/43741",
    ],
)
def test_resolve_target_rejects_urls_outside_allowlist(url):
    with pytest.raises(ValueError):
        resolve_target(url=url)


def test_resolve_target_requires_exactly_one_target():
    with pytest.raises(ValueError):
        resolve_target()
    with pytest.raises(ValueError):
        resolve_target(url="https://www.bestfightodds.com/archive", event_id="ufc-330-4237")


def test_probe_refuses_request_cap_above_one(tmp_path):
    with pytest.raises(ValueError, match="hard-limited to 1"):
        probe_bestfightodds_snapshot(
            event_id="ufc-330-4237",
            raw_dir=tmp_path,
            request_cap=2,
        )


def test_probe_refuses_delay_below_two_seconds(tmp_path):
    with pytest.raises(ValueError, match="at least 2.0"):
        probe_bestfightodds_snapshot(
            event_id="ufc-330-4237",
            raw_dir=tmp_path,
            request_interval_seconds=1.9,
        )


def test_dry_run_does_not_call_fetcher_or_write_files(tmp_path):
    def fail_fetcher(*args):
        raise AssertionError("dry-run should not fetch")

    result = probe_bestfightodds_snapshot(
        event_id="ufc-330-4237",
        raw_dir=tmp_path,
        dry_run=True,
        fetcher=fail_fetcher,
    )

    assert result.dry_run is True
    assert result.url == "https://www.bestfightodds.com/events/ufc-330-4237"
    assert result.html_path is None
    assert result.metadata_path is None
    assert list(tmp_path.iterdir()) == []


def test_write_snapshot_uses_deterministic_metadata_name_and_content(tmp_path):
    target = ProbeTarget(
        url="https://www.bestfightodds.com/events/ufc-330-4237",
        target_type="event",
        identifier="ufc-330-4237",
    )
    response = FetchResponse(
        final_url=target.url,
        http_status=200,
        body=b"<html>BestFightOdds snapshot</html>",
    )
    fetched_at = datetime(2026, 8, 19, 12, 30, tzinfo=timezone.utc)

    result = write_snapshot(
        raw_dir=tmp_path,
        target=target,
        response=response,
        fetched_at=fetched_at,
        user_agent=DEFAULT_USER_AGENT,
        request_cap=1,
        request_interval_seconds=2.0,
        timeout_seconds=15.0,
        max_retries=1,
        retry_backoff_seconds=2.0,
    )

    assert result.html_path is not None
    assert result.metadata_path is not None
    assert result.html_path.name.startswith("20260819T123000Z_event_ufc-330-4237_")
    assert result.html_path.suffix == ".html"
    assert result.metadata_path.name == result.html_path.name.replace(".html", ".metadata.json")
    assert result.html_path.read_bytes() == response.body

    metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))
    assert metadata["source"] == "bestfightodds_raw_snapshot_probe"
    assert metadata["source_url"] == target.url
    assert metadata["http_status"] == 200
    assert metadata["content_sha256"] == result.content_sha256
    assert metadata["content_bytes"] == len(response.body)
    assert metadata["request_cap"] == 1
    assert metadata["request_interval_seconds"] == 2.0
    assert metadata["parsing_performed"] is False
    assert metadata["loaded_into_fight_odds"] is False


def test_probe_with_injected_fetcher_writes_snapshot(tmp_path):
    calls = []

    def fake_fetcher(url, user_agent, timeout_seconds, max_retries, retry_backoff_seconds):
        calls.append((url, user_agent, timeout_seconds, max_retries, retry_backoff_seconds))
        return FetchResponse(final_url=url, http_status=200, body=b"<html>ok</html>")

    result = probe_bestfightodds_snapshot(
        url="https://www.bestfightodds.com/archive",
        raw_dir=tmp_path,
        fetcher=fake_fetcher,
        fetched_at=datetime(2026, 8, 19, 12, 45, tzinfo=timezone.utc),
    )

    assert calls == [
        (
            "https://www.bestfightodds.com/archive",
            DEFAULT_USER_AGENT,
            15.0,
            1,
            2.0,
        )
    ]
    assert result.dry_run is False
    assert result.html_path is not None
    assert result.html_path.exists()
    assert result.metadata_path is not None
    assert result.metadata_path.exists()

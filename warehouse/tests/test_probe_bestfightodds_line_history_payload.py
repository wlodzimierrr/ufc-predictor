"""Tests for the bounded BestFightOdds line-history payload probe."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.probe_bestfightodds_line_history_payload import (
    DEFAULT_USER_AGENT,
    FetchResponse,
    PayloadTarget,
    probe_bestfightodds_line_history_payload,
    resolve_payload_target,
    write_payload,
)


def test_resolve_payload_target_accepts_bfo_js_asset():
    target = resolve_payload_target("https://www.bestfightodds.com/js/bfo.min.js?v=0.4.10")

    assert target.url == "https://www.bestfightodds.com/js/bfo.min.js?v=0.4.10"
    assert target.target_type == "js_asset"
    assert target.identifier == "bfo.min.js"


def test_resolve_payload_target_accepts_reviewed_api_path():
    target = resolve_payload_target("https://www.bestfightodds.com/api/history?li=43741-1")

    assert target.url == "https://www.bestfightodds.com/api/history?li=43741-1"
    assert target.target_type == "detail_payload"
    assert target.identifier.startswith("api/history_")


@pytest.mark.parametrize(
    "url",
    [
        "http://www.bestfightodds.com/js/bfo.min.js?v=0.4.10",
        "https://example.com/api/history?li=43741-1",
        "https://www.bestfightodds.com/events/ufc-330-4237",
        "https://www.bestfightodds.com/fighters/ian-machado-garry-15690",
        "https://www.bestfightodds.com/archive",
        "https://www.bestfightodds.com/cnadm/matchups/43741",
    ],
)
def test_resolve_payload_target_rejects_non_payload_urls(url):
    with pytest.raises(ValueError):
        resolve_payload_target(url)


def test_payload_probe_refuses_request_cap_above_one(tmp_path):
    with pytest.raises(ValueError, match="hard-limited to 1"):
        probe_bestfightodds_line_history_payload(
            url="https://www.bestfightodds.com/js/bfo.min.js?v=0.4.10",
            raw_dir=tmp_path,
            request_cap=2,
        )


def test_payload_probe_refuses_delay_below_two_seconds(tmp_path):
    with pytest.raises(ValueError, match="at least 2.0"):
        probe_bestfightodds_line_history_payload(
            url="https://www.bestfightodds.com/js/bfo.min.js?v=0.4.10",
            raw_dir=tmp_path,
            request_interval_seconds=1.9,
        )


def test_payload_dry_run_does_not_call_fetcher_or_write_files(tmp_path):
    def fail_fetcher(*args):
        raise AssertionError("dry-run should not fetch")

    result = probe_bestfightodds_line_history_payload(
        url="https://www.bestfightodds.com/js/bfo.min.js?v=0.4.10",
        raw_dir=tmp_path,
        dry_run=True,
        fetcher=fail_fetcher,
    )

    assert result.dry_run is True
    assert result.url == "https://www.bestfightodds.com/js/bfo.min.js?v=0.4.10"
    assert result.payload_path is None
    assert result.metadata_path is None
    assert list(tmp_path.iterdir()) == []


def test_write_payload_uses_deterministic_metadata_name_and_content(tmp_path):
    target = PayloadTarget(
        url="https://www.bestfightodds.com/js/bfo.min.js?v=0.4.10",
        target_type="js_asset",
        identifier="bfo.min.js",
    )
    response = FetchResponse(
        final_url=target.url,
        http_status=200,
        body=b"console.log('BestFightOdds');",
        content_type="application/javascript",
    )
    fetched_at = datetime(2026, 8, 19, 12, 30, tzinfo=timezone.utc)

    result = write_payload(
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

    assert result.payload_path is not None
    assert result.metadata_path is not None
    assert result.payload_path.name.startswith("20260819T123000Z_js_asset_bfo.min.js_")
    assert result.payload_path.suffix == ".js"
    assert result.metadata_path.name.endswith(".metadata.json")
    assert result.payload_path.read_bytes() == response.body

    metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))
    assert metadata["source"] == "bestfightodds_line_history_payload_probe"
    assert metadata["source_url"] == target.url
    assert metadata["http_status"] == 200
    assert metadata["content_sha256"] == result.content_sha256
    assert metadata["content_bytes"] == len(response.body)
    assert metadata["request_cap"] == 1
    assert metadata["request_interval_seconds"] == 2.0
    assert metadata["parsing_performed"] is False
    assert metadata["loaded_into_fight_odds"] is False
    assert metadata["scheduled_or_continuous_fetch"] is False


def test_payload_probe_with_injected_fetcher_writes_payload(tmp_path):
    calls = []

    def fake_fetcher(url, user_agent, timeout_seconds, max_retries, retry_backoff_seconds):
        calls.append((url, user_agent, timeout_seconds, max_retries, retry_backoff_seconds))
        return FetchResponse(
            final_url=url,
            http_status=200,
            body=b'{"ok":true}',
            content_type="application/json",
        )

    result = probe_bestfightodds_line_history_payload(
        url="https://www.bestfightodds.com/api/history?li=43741-1",
        raw_dir=tmp_path,
        fetcher=fake_fetcher,
        fetched_at=datetime(2026, 8, 19, 12, 45, tzinfo=timezone.utc),
    )

    assert calls == [
        (
            "https://www.bestfightodds.com/api/history?li=43741-1",
            DEFAULT_USER_AGENT,
            15.0,
            1,
            2.0,
        )
    ]
    assert result.dry_run is False
    assert result.payload_path is not None
    assert result.payload_path.exists()
    assert result.payload_path.suffix == ".json"
    assert result.metadata_path is not None
    assert result.metadata_path.exists()

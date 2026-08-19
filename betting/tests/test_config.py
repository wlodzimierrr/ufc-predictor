"""Tests for betting configuration defaults and overrides."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting.config import (
    BettingConfig,
    RiskConfig,
    apply_cli_overrides,
    default_config,
    load_config_file,
)


def test_default_risk_config_uses_conservative_caps():
    config = default_config()

    assert config.risk.kelly_fraction == pytest.approx(0.25)
    assert config.risk.min_edge == pytest.approx(0.03)
    assert config.risk.min_ev == pytest.approx(0.01)
    assert config.risk.max_single_bet_fraction == pytest.approx(0.02)
    assert config.risk.max_event_fraction == pytest.approx(0.06)
    assert config.risk.medium_tier_cap == pytest.approx(0.01)
    assert config.risk.high_tier_cap == pytest.approx(0.02)
    assert config.risk.max_odds_age_hours_current == 48
    assert config.risk.drawdown_protection_threshold is None


def test_default_toss_up_cap_equals_zero():
    assert default_config().risk.toss_up_tier_cap == pytest.approx(0.0)


def test_json_config_file_overrides_nested_risk(tmp_path):
    path = tmp_path / "betting.json"
    path.write_text(
        json.dumps({"risk": {"min_edge": 0.05}, "report_dir": "tmp/reports"}),
        encoding="utf-8",
    )

    config = load_config_file(path)

    assert config.risk.min_edge == pytest.approx(0.05)
    assert config.risk.max_single_bet_fraction == pytest.approx(0.02)
    assert config.report_dir == "tmp/reports"


def test_toml_config_file_overrides_flat_risk(tmp_path):
    path = tmp_path / "betting.toml"
    path.write_text(
        "\n".join([
            "kelly_fraction = 0.10",
            "max_event_fraction = 0.04",
        ]),
        encoding="utf-8",
    )

    config = load_config_file(path)

    assert config.risk.kelly_fraction == pytest.approx(0.10)
    assert config.risk.max_event_fraction == pytest.approx(0.04)


def test_cli_overrides_accept_argparse_namespace():
    base = BettingConfig()
    namespace = argparse.Namespace(min_ev=0.02, report_dir=None)

    config = apply_cli_overrides(base, namespace)

    assert config.risk.min_ev == pytest.approx(0.02)
    assert config.report_dir == base.report_dir


def test_risk_config_rejects_invalid_fraction():
    with pytest.raises(ValueError):
        RiskConfig(max_single_bet_fraction=1.1)

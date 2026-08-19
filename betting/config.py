"""Configuration defaults for betting value and risk management."""

from __future__ import annotations

import json
import tomllib
from dataclasses import asdict, dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Mapping


def _validate_fraction(name: str, value: float) -> None:
    if not 0 <= value <= 1:
        raise ValueError(f"{name} must be between 0 and 1")


@dataclass(frozen=True)
class RiskConfig:
    """Conservative deterministic staking and decision thresholds."""

    kelly_fraction: float = 0.25
    min_edge: float = 0.03
    min_ev: float = 0.01
    max_single_bet_fraction: float = 0.02
    max_event_fraction: float = 0.06
    medium_tier_cap: float = 0.01
    high_tier_cap: float = 0.02
    toss_up_tier_cap: float = 0.00
    max_odds_age_hours_current: int = 48
    drawdown_protection_threshold: float | None = None

    def __post_init__(self) -> None:
        _validate_fraction("kelly_fraction", self.kelly_fraction)
        _validate_fraction("min_edge", self.min_edge)
        _validate_fraction("min_ev", self.min_ev)
        _validate_fraction("max_single_bet_fraction", self.max_single_bet_fraction)
        _validate_fraction("max_event_fraction", self.max_event_fraction)
        _validate_fraction("medium_tier_cap", self.medium_tier_cap)
        _validate_fraction("high_tier_cap", self.high_tier_cap)
        _validate_fraction("toss_up_tier_cap", self.toss_up_tier_cap)
        if self.max_odds_age_hours_current <= 0:
            raise ValueError("max_odds_age_hours_current must be positive")
        if self.drawdown_protection_threshold is not None:
            _validate_fraction(
                "drawdown_protection_threshold",
                self.drawdown_protection_threshold,
            )

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "RiskConfig":
        """Create risk config from a mapping of known fields."""
        return cls(**_known_dataclass_values(cls, values))

    def with_overrides(self, values: Mapping[str, Any]) -> "RiskConfig":
        """Return a copy with known fields overridden."""
        return replace(self, **_known_dataclass_values(type(self), values))

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary suitable for logs or reports."""
        return asdict(self)


@dataclass(frozen=True)
class BettingConfig:
    """Top-level betting subsystem configuration."""

    risk: RiskConfig = field(default_factory=RiskConfig)
    report_dir: str = "data/reports"
    recommendations_report: str = "betting_recommendations.csv"
    event_summary_report: str = "betting_event_summary.csv"
    backtest_fights_report: str = "betting_backtest_fights.csv"
    backtest_events_report: str = "betting_backtest_events.csv"
    backtest_summary_report: str = "betting_backtest_summary.csv"

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "BettingConfig":
        """Create config from nested or flat mapping values."""
        risk_values = values.get("risk", {})
        if risk_values is None:
            risk_values = {}
        if not isinstance(risk_values, Mapping):
            raise TypeError("risk config must be a mapping")

        top_level = _known_dataclass_values(cls, values, exclude={"risk"})
        risk = RiskConfig().with_overrides({**_risk_values(values), **risk_values})
        return cls(risk=risk, **top_level)

    def with_overrides(self, values: Mapping[str, Any]) -> "BettingConfig":
        """Return a copy with known top-level and risk fields overridden."""
        risk_values = values.get("risk", {})
        if risk_values is None:
            risk_values = {}
        if not isinstance(risk_values, Mapping):
            raise TypeError("risk config must be a mapping")

        top_level = _known_dataclass_values(type(self), values, exclude={"risk"})
        risk = self.risk.with_overrides({**_risk_values(values), **risk_values})
        return replace(self, risk=risk, **top_level)

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary suitable for logs or reports."""
        return asdict(self)


def default_config() -> BettingConfig:
    """Return deterministic betting defaults."""
    return BettingConfig()


def load_config_file(path: str | Path) -> BettingConfig:
    """Load betting config overrides from a small JSON or TOML file."""
    config_path = Path(path)
    suffix = config_path.suffix.lower()

    if suffix == ".json":
        values = json.loads(config_path.read_text(encoding="utf-8"))
    elif suffix == ".toml":
        values = tomllib.loads(config_path.read_text(encoding="utf-8"))
    else:
        raise ValueError("config file must be .json or .toml")

    if not isinstance(values, Mapping):
        raise TypeError("config file must contain a mapping")
    return BettingConfig.from_mapping(values)


def apply_cli_overrides(
    config: BettingConfig,
    overrides: Mapping[str, Any] | object | None,
) -> BettingConfig:
    """Apply CLI-style overrides from a mapping or argparse namespace."""
    if overrides is None:
        return config

    if isinstance(overrides, Mapping):
        values = dict(overrides)
    else:
        values = vars(overrides)

    clean_values = {key: value for key, value in values.items() if value is not None}
    return config.with_overrides(clean_values)


def _risk_field_names() -> set[str]:
    return {field.name for field in fields(RiskConfig)}


def _risk_values(values: Mapping[str, Any]) -> dict[str, Any]:
    risk_field_names = _risk_field_names()
    return {
        key: value
        for key, value in values.items()
        if key in risk_field_names and value is not None
    }


def _known_dataclass_values(
    cls: type,
    values: Mapping[str, Any],
    exclude: set[str] | None = None,
) -> dict[str, Any]:
    exclude = exclude or set()
    field_names = {field.name for field in fields(cls)} - exclude
    return {
        key: value
        for key, value in values.items()
        if key in field_names and value is not None
    }

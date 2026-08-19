"""Betting value and risk management subsystem.

This package is intentionally independent from the production modeling workflow.
Prediction code should not need to import betting modules to train or score fights.
"""

from __future__ import annotations

from betting.config import (
    BettingConfig,
    RiskConfig,
    apply_cli_overrides,
    default_config,
    load_config_file,
)
from betting.reasons import (
    ALL_REASON_CODES,
    BET_CAP_REASON_CODES,
    PASS_REASON_CODES,
    ReasonCode,
    format_reason_codes,
)

__all__ = [
    "BettingConfig",
    "RiskConfig",
    "ALL_REASON_CODES",
    "BET_CAP_REASON_CODES",
    "PASS_REASON_CODES",
    "ReasonCode",
    "apply_cli_overrides",
    "default_config",
    "format_reason_codes",
    "load_config_file",
]

"""Import smoke tests for the betting package."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def test_betting_modules_import():
    modules = [
        "betting",
        "betting.backtest",
        "betting.config",
        "betting.odds",
        "betting.reasons",
        "betting.recommend",
        "betting.risk",
        "betting.value",
    ]

    for module in modules:
        importlib.import_module(module)

"""Odds conversion and market probability helpers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Mapping


NumberLike = Decimal | int | float | str


@dataclass(frozen=True)
class NoVigProbabilityRow:
    """Derived no-vig probability for one side of a two-way market."""

    fighter_id: str
    opponent_fighter_id: str
    decimal_odds: Decimal
    implied_probability: Decimal
    no_vig_implied_probability: Decimal
    overround: Decimal
    bookmaker: str | None = None
    market: str = "moneyline"
    line_type: str | None = None
    odds_timestamp: object | None = None


@dataclass(frozen=True)
class NoVigResult:
    """Valid or invalid no-vig calculation result."""

    valid: bool
    rows: tuple[NoVigProbabilityRow, ...] = ()
    reason: str | None = None

    @classmethod
    def invalid(cls, reason: str) -> "NoVigResult":
        return cls(valid=False, reason=reason)


def parse_optional_decimal(value: object) -> Decimal | None:
    """Parse optional CSV-style decimal values."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal value: {value}") from exc


def parse_optional_american_odds(value: object) -> int | None:
    """Parse optional CSV-style American odds."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return validate_american_odds(value)


def validate_american_odds(american_odds: NumberLike) -> int:
    """Validate and return American odds as an integer."""
    parsed = _to_decimal(american_odds, "American odds")
    if parsed != parsed.to_integral_value():
        raise ValueError(f"American odds must be an integer: {american_odds}")
    value = int(parsed)
    if value == 0:
        raise ValueError("American odds cannot be zero")
    return value


def american_to_decimal_odds(american_odds: NumberLike) -> Decimal:
    """Convert American odds to decimal odds."""
    odds = validate_american_odds(american_odds)
    if odds > 0:
        return Decimal("1") + (Decimal(odds) / Decimal("100"))
    return Decimal("1") + (Decimal("100") / abs(Decimal(odds)))


def validate_decimal_odds(decimal_odds: NumberLike) -> Decimal:
    """Validate and return decimal odds."""
    odds = _to_decimal(decimal_odds, "decimal odds")
    if odds <= Decimal("1"):
        raise ValueError("decimal odds must be greater than 1.0")
    return odds


def decimal_to_american_odds(decimal_odds: NumberLike) -> Decimal:
    """Convert decimal odds to exact American odds without presentation rounding."""
    odds = validate_decimal_odds(decimal_odds)
    net_decimal = odds - Decimal("1")
    if odds >= Decimal("2"):
        american_odds = net_decimal * Decimal("100")
    else:
        american_odds = -(Decimal("100") / net_decimal)
    if american_odds == american_odds.to_integral_value():
        return american_odds.quantize(Decimal("1"))
    return american_odds


def normalize_decimal_odds(
    *,
    american_odds: NumberLike | None = None,
    decimal_odds: NumberLike | None = None,
) -> Decimal:
    """Return decimal odds from either supplied decimal or American odds."""
    if decimal_odds is not None:
        return validate_decimal_odds(decimal_odds)
    if american_odds is not None:
        return american_to_decimal_odds(american_odds)
    raise ValueError("either American or decimal odds must be supplied")


def implied_probability(decimal_odds: NumberLike) -> Decimal:
    """Convert decimal odds to raw implied probability."""
    decimal_odds = validate_decimal_odds(decimal_odds)
    return Decimal("1") / decimal_odds


def calculate_no_vig_probabilities(sides: list[Mapping[str, object]]) -> NoVigResult:
    """Calculate no-vig probabilities for exactly two reciprocal market sides."""
    if len(sides) < 2:
        return NoVigResult.invalid("missing_side")
    if len(sides) > 2:
        return NoVigResult.invalid("too_many_sides")

    try:
        prepared = [_prepare_market_side(side) for side in sides]
    except ValueError as exc:
        return NoVigResult.invalid(str(exc))

    fighter_ids = [side["fighter_id"] for side in prepared]
    if len(set(fighter_ids)) < 2:
        return NoVigResult.invalid("duplicate_side")

    first, second = prepared
    if (
        first["opponent_fighter_id"] != second["fighter_id"]
        or second["opponent_fighter_id"] != first["fighter_id"]
    ):
        return NoVigResult.invalid("non_reciprocal_sides")

    for metadata_key in ("bookmaker", "market", "line_type", "odds_timestamp"):
        first_value = first.get(metadata_key)
        second_value = second.get(metadata_key)
        if first_value is not None and second_value is not None and first_value != second_value:
            return NoVigResult.invalid(f"mismatched_{metadata_key}")

    overround = sum((side["implied_probability"] for side in prepared), Decimal("0"))
    if overround <= 0:
        return NoVigResult.invalid("invalid_overround")

    rows = tuple(
        NoVigProbabilityRow(
            fighter_id=side["fighter_id"],
            opponent_fighter_id=side["opponent_fighter_id"],
            decimal_odds=side["decimal_odds"],
            implied_probability=side["implied_probability"],
            overround=overround,
            no_vig_implied_probability=side["implied_probability"] / overround,
            bookmaker=side.get("bookmaker"),
            market=side.get("market") or "moneyline",
            line_type=side.get("line_type"),
            odds_timestamp=side.get("odds_timestamp"),
        )
        for side in prepared
    )
    return NoVigResult(valid=True, rows=rows)


def _prepare_market_side(side: Mapping[str, object]) -> dict[str, object]:
    fighter_id = _required_side_value(side, "fighter_id")
    opponent_fighter_id = _required_side_value(side, "opponent_fighter_id")
    if fighter_id == opponent_fighter_id:
        raise ValueError("duplicate_side")

    decimal_odds = normalize_decimal_odds(
        american_odds=side.get("american_odds"),
        decimal_odds=side.get("decimal_odds"),
    )
    raw_implied_probability = implied_probability(decimal_odds)
    return {
        "fighter_id": fighter_id,
        "opponent_fighter_id": opponent_fighter_id,
        "decimal_odds": decimal_odds,
        "implied_probability": raw_implied_probability,
        "bookmaker": _optional_side_value(side, "bookmaker"),
        "market": _optional_side_value(side, "market") or "moneyline",
        "line_type": _optional_side_value(side, "line_type"),
        "odds_timestamp": side.get("odds_timestamp"),
    }


def _required_side_value(side: Mapping[str, object], key: str) -> str:
    value = _optional_side_value(side, key)
    if value is None:
        raise ValueError(f"missing_{key}")
    return value


def _optional_side_value(side: Mapping[str, object], key: str) -> str | None:
    value = side.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_decimal(value: NumberLike, label: str) -> Decimal:
    try:
        return Decimal(str(value).strip())
    except InvalidOperation as exc:
        raise ValueError(f"invalid {label}: {value}") from exc


__all__ = [
    "NoVigProbabilityRow",
    "NoVigResult",
    "american_to_decimal_odds",
    "calculate_no_vig_probabilities",
    "decimal_to_american_odds",
    "implied_probability",
    "normalize_decimal_odds",
    "parse_optional_american_odds",
    "parse_optional_decimal",
    "validate_american_odds",
    "validate_decimal_odds",
]

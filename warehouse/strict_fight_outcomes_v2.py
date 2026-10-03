"""Strict outcome admission for future fight ingestion, before legacy transforms.

Decision table (tokens are stripped, case-sensitive; null/blank means empty):
    event_status       outcome pair       result_type   winner
    upcoming           empty / empty      upcoming      None
    completed/missing  W / L              win           fighter_1_id
    completed/missing  L / W              win           fighter_2_id
    completed/missing  D / D              draw          None
    completed/missing  NC / NC            nc            None

Only established source statuses upcoming, completed and canceled are known.
Missing status supports explicit resolved historical pairs only. Finish methods
never supply outcomes. The checksum-pinned legacy transform stays unchanged for
historical reconstruction and diagnostics; only admitted rows reach it here.

Rejection precedence and stable reasons:
    canceled_status: canceled, regardless of outcomes.
    unsupported_event_status: any other nonempty status.
    unsupported_outcome_token: any nonempty token outside W, L, D, NC.
    upcoming_with_outcomes: upcoming with either outcome nonempty.
    completed_without_outcomes: completed with both outcomes empty.
    missing_status_without_outcomes: missing status with both outcomes empty.
    partial_outcome_pair: exactly one outcome empty.
    contradictory_outcome_pair: any remaining pair outside the four above.
"""

from __future__ import annotations

from typing import Any

from warehouse.transform import transform_fight as _legacy_transform_fight

ADAPTER_VERSION = "strict_fight_outcomes_v2"
_OUTCOME_TOKENS = frozenset({"W", "L", "D", "NC"})
_RESOLVED_PAIRS = frozenset({("W", "L"), ("L", "W"), ("D", "D"), ("NC", "NC")})


def _token(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).strip() or None


class FightOutcomeValidationError(ValueError):
    """Outcome admission failed; reason and fight_id are stable caller fields."""

    def __init__(self, reason: str, fight_id: Any, event_status: str | None,
                 outcomes: tuple[str | None, str | None]) -> None:
        self.reason = reason
        self.fight_id = fight_id
        self.event_status = event_status
        self.outcomes = outcomes
        super().__init__(
            f"Fight {fight_id!r}: {reason} "
            f"(event_status={event_status!r}, outcomes={outcomes!r})"
        )


def transform_fight(row: dict) -> dict:
    """Validate disposition, then delegate a copy without mutating source fields.

    Raises FightOutcomeValidationError before legacy field transformation for
    every rejected disposition. Other field validation remains legacy behavior.
    """
    status = _token(row.get("event_status"))
    outcomes = (_token(row.get("fighter_1_outcome")),
                _token(row.get("fighter_2_outcome")))

    def reject(reason: str) -> None:
        raise FightOutcomeValidationError(reason, row.get("fight_id"), status, outcomes)

    if status == "canceled":
        reject("canceled_status")
    if status not in {None, "upcoming", "completed"}:
        reject("unsupported_event_status")
    if any(token is not None and token not in _OUTCOME_TOKENS for token in outcomes):
        reject("unsupported_outcome_token")

    if status == "upcoming":
        if any(outcomes):
            reject("upcoming_with_outcomes")
    elif not any(outcomes):
        reject("completed_without_outcomes" if status == "completed"
               else "missing_status_without_outcomes")
    elif None in outcomes:
        reject("partial_outcome_pair")
    elif outcomes not in _RESOLVED_PAIRS:
        reject("contradictory_outcome_pair")

    return _legacy_transform_fight(dict(row))

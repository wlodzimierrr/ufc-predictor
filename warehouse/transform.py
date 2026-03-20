"""Pure transformation functions: raw CSV row dicts → DB-ready dicts.

No DB calls, no file I/O. All normalization rules are from docs/normalization-rules.md.
"""

from __future__ import annotations

import datetime
from typing import Any

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _str(val: Any) -> str | None:
    """Strip whitespace; return None for empty strings."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _int(val: Any, default: int = 0) -> int:
    """Parse int; return default for empty/None."""
    if val is None or str(val).strip() == "":
        return default
    return int(val)


def _float(val: Any) -> float | None:
    """Parse float; return None for empty/None."""
    if val is None or str(val).strip() == "":
        return None
    return float(val)


def _date(val: Any) -> datetime.date | None:
    """Parse ISO date string 'YYYY-MM-DD'; return None for empty."""
    s = _str(val)
    if not s:
        return None
    return datetime.date.fromisoformat(s)


def _timestamp(val: Any) -> datetime.datetime | None:
    """Parse ISO timestamp string; return None for empty."""
    s = _str(val)
    if not s:
        return None
    # Remove trailing ' UTC' suffix produced by the scraper
    s = s.replace(" UTC", "+00:00")
    return datetime.datetime.fromisoformat(s)


# ---------------------------------------------------------------------------
# Weight class / title fight extraction
# ---------------------------------------------------------------------------

# Ordered longest-match-first so e.g. "Women's Strawweight" beats "Strawweight"
_WEIGHT_CLASS_MAP: list[tuple[str, str]] = [
    ("Women's Strawweight", "women_strawweight"),
    ("Women's Flyweight", "women_flyweight"),
    ("Women's Bantamweight", "women_bantamweight"),
    ("Women's Featherweight", "women_featherweight"),
    ("Light Heavyweight", "light_heavyweight"),
    ("Super Heavyweight", "super_heavyweight"),
    ("Heavyweight", "heavyweight"),
    ("Featherweight", "featherweight"),
    ("Lightweight", "lightweight"),
    ("Welterweight", "welterweight"),
    ("Middleweight", "middleweight"),
    ("Bantamweight", "bantamweight"),
    ("Flyweight", "flyweight"),
    ("Strawweight", "strawweight"),
    ("Open Weight", "open_weight"),
    ("Catch Weight", "catch_weight"),
]

_FINISH_METHOD_MAP: dict[str, str] = {
    "decision": "decision",
    "ko/tko": "ko_tko",
    "submission": "submission",
    "tko - doctor's stoppage": "doctor_stoppage",
    "overturned": "overturned",
    "could not continue": "could_not_continue",
    "dq": "dq",
    "other": "other",
}


def _extract_bout_flags(bout_type: str | None) -> tuple[str | None, bool, bool]:
    """Return (weight_class, is_title_fight, is_interim_title) from bout_type."""
    if not bout_type:
        return None, False, False

    is_title = "Title Bout" in bout_type or "Tournament Title Bout" in bout_type
    is_interim = "Interim" in bout_type

    weight_class = None
    for substring, value in _WEIGHT_CLASS_MAP:
        if substring in bout_type:
            weight_class = value
            break

    return weight_class, is_title, is_interim


def _map_finish_method(primary: str | None) -> str | None:
    if not primary:
        return None
    return _FINISH_METHOD_MAP.get(primary.strip().lower())


# ---------------------------------------------------------------------------
# Public transform functions
# ---------------------------------------------------------------------------

def transform_event(row: dict) -> dict:
    """Transform a raw events CSV row into a DB-ready dict."""
    return {
        "event_id": row["event_id"],
        "event_name": _str(row["name"]),
        "event_date": _date(row["date_formatted"]),
        "city": _str(row["city"]),
        "state": _str(row["state"]),
        "country": _str(row["country"]),
        "event_status": _str(row.get("event_status")) or "completed",
        "source_url": row.get("url"),
        "scraped_at": _timestamp(row["scraped_at"]),
    }


def transform_fight(row: dict) -> dict:
    """Transform a raw fights CSV row into a DB-ready dict."""
    outcome_1 = _str(row.get("fighter_1_outcome", ""))
    outcome_2 = _str(row.get("fighter_2_outcome", ""))

    if outcome_1 == "W" and outcome_2 == "L":
        result_type = "win"
        winner_fighter_id = row["fighter_1_id"]
    elif outcome_1 == "L" and outcome_2 == "W":
        result_type = "win"
        winner_fighter_id = row["fighter_2_id"]
    elif outcome_1 == "D" and outcome_2 == "D":
        result_type = "draw"
        winner_fighter_id = None
    else:  # NC / NC
        result_type = "nc"
        winner_fighter_id = None

    bout_type = _str(row.get("bout_type"))
    weight_class, is_title_fight, is_interim_title = _extract_bout_flags(bout_type)

    finish_minute = _int(row.get("finish_time_minute"), default=0)
    finish_second = _int(row.get("finish_time_second"), default=0)
    finish_time_seconds = finish_minute * 60 + finish_second if (finish_minute or finish_second) else None

    return {
        "fight_id": row["fight_id"],
        "event_id": row["event_id"],
        "fighter_1_id": row["fighter_1_id"],
        "fighter_2_id": row["fighter_2_id"],
        "winner_fighter_id": winner_fighter_id,
        "result_type": result_type,
        "weight_class": weight_class,
        "is_title_fight": is_title_fight,
        "is_interim_title": is_interim_title,
        "scheduled_rounds": _int(row.get("num_rounds"), default=None),
        "finish_method": _map_finish_method(row.get("primary_finish_method")),
        "finish_detail": _str(row.get("secondary_finish_method")),
        "finish_round": _int(row.get("finish_round"), default=None) if _str(row.get("finish_round")) else None,
        "finish_time_seconds": finish_time_seconds,
        "referee": _str(row.get("referee")),
        "source_url": row.get("url"),
        "scraped_at": _timestamp(row["scraped_at"]),
    }


def transform_fighter(row: dict) -> dict:
    """Transform a raw fighters CSV row into a DB-ready dict."""
    return {
        "fighter_id": row["fighter_id"],
        "full_name": _str(row["full_name"]),
        "first_name": _str(row.get("first_name")),
        "last_name": _str(row.get("last_names")),
        "nickname": _str(row.get("nickname")),
        "height_cm": _float(row.get("height_cm")),
        "weight_lbs": _float(row.get("weight_lbs")),
        "reach_cm": _float(row.get("reach_cm")),
        "stance": _str(row.get("stance")),
        "dob": _date(row.get("dob_formatted")),
        "source_url": row.get("url"),
        "scraped_at": _timestamp(row["scraped_at"]),
    }


def transform_fight_stat(row: dict, *, by_round: bool = False) -> dict:
    """Transform a raw fight_stats CSV row into a DB-ready dict.

    Works for both fight_stats_aggregate (by_round=False) and
    fight_stats_by_round (by_round=True).
    """
    ctrl_min = _int(row.get("control_time_minutes"), default=0)
    ctrl_sec = _int(row.get("control_time_seconds"), default=0)
    control_time_seconds = ctrl_min * 60 + ctrl_sec

    out: dict = {
        "fight_id": row["fight_id"],
        "fighter_id": row["fighter_id"],
        "knockdowns": _int(row.get("knockdowns")),
        "total_strikes_landed": _int(row.get("total_strikes_landed")),
        "total_strikes_attempted": _int(row.get("total_strikes_attempted")),
        "sig_strikes_landed": _int(row.get("significant_strikes_landed")),
        "sig_strikes_attempted": _int(row.get("significant_strikes_attempted")),
        "sig_strikes_head_landed": _int(row.get("significant_strikes_landed_head")),
        "sig_strikes_head_attempted": _int(row.get("significant_strikes_attempted_head")),
        "sig_strikes_body_landed": _int(row.get("significant_strikes_landed_body")),
        "sig_strikes_body_attempted": _int(row.get("significant_strikes_attempted_body")),
        "sig_strikes_leg_landed": _int(row.get("significant_strikes_landed_leg")),
        "sig_strikes_leg_attempted": _int(row.get("significant_strikes_attempted_leg")),
        "sig_strikes_distance_landed": _int(row.get("significant_strikes_landed_distance")),
        "sig_strikes_distance_attempted": _int(row.get("significant_strikes_attempted_distance")),
        "sig_strikes_clinch_landed": _int(row.get("significant_strikes_landed_clinch")),
        "sig_strikes_clinch_attempted": _int(row.get("significant_strikes_attempted_clinch")),
        "sig_strikes_ground_landed": _int(row.get("significant_strikes_landed_ground")),
        "sig_strikes_ground_attempted": _int(row.get("significant_strikes_attempted_ground")),
        "takedowns_landed": _int(row.get("takedowns_landed")),
        "takedowns_attempted": _int(row.get("takedowns_attempted")),
        "control_time_seconds": control_time_seconds,
        "submissions_attempted": _int(row.get("submissions_attempted")),
        "reversals": _int(row.get("reversals")),
        "source_url": row.get("url"),
        "scraped_at": _timestamp(row["scraped_at"]),
    }

    if by_round:
        out["fight_stat_by_round_id"] = row["fight_stat_by_round_id"]
        out["round"] = _int(row["round"])
    else:
        out["fight_stat_id"] = row["fight_stat_id"]

    return out

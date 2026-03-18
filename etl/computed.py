"""
etl/computed.py

Fills in the derived/computed columns on deliveries and aggregates on innings.

Operates purely in-memory on lists of dicts — no DB access.

Computed columns on deliveries:
    phase                   already set by parser (_phase)
    innings_score_at_ball   cumulative runs_total AFTER this ball
    wickets_fallen_at_ball  cumulative wickets AFTER this ball
    legal_balls_bowled      legal deliveries bowled up to and including this ball
    balls_remaining         legal balls left after this ball (120 - legal_balls_bowled)
    required_run_rate       (target - score) / (balls_remaining / 6)  [2nd innings only]

Aggregate columns on innings:
    total_runs
    total_wickets
    total_overs_faced       e.g. 19.3
"""

from __future__ import annotations
from typing import Optional


LEGAL_BALLS_PER_INNINGS = 120  # 20 overs × 6 balls


def enrich(
    innings_list: list[dict],
    deliveries_list: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Main entry point. Enriches both innings and delivery dicts in-place.
    Returns the same objects for convenience.
    """
    # Group deliveries by innings_number for easier processing
    by_innings: dict[int, list[dict]] = {}
    for d in deliveries_list:
        by_innings.setdefault(d["innings_number"], []).append(d)

    # Build a quick lookup of target by innings_number
    target_by_innings: dict[int, Optional[int]] = {}
    for inn in innings_list:
        target_by_innings[inn["innings_number"]] = inn.get("target_runs")

    for inn in innings_list:
        inn_num = inn["innings_number"]
        inn_deliveries = by_innings.get(inn_num, [])
        target = target_by_innings.get(inn_num)
        is_super_over = inn.get("is_super_over", False)

        _enrich_deliveries(inn_deliveries, target, is_super_over)
        _enrich_innings(inn, inn_deliveries)

    return innings_list, deliveries_list


# ── Delivery enrichment ──────────────────────────────────────────────────────

def _enrich_deliveries(
    deliveries: list[dict],
    target: Optional[int],
    is_super_over: bool,
) -> None:
    total_legal = 6 if is_super_over else LEGAL_BALLS_PER_INNINGS

    cumulative_runs = 0
    cumulative_wickets = 0
    legal_balls_bowled = 0

    for d in deliveries:
        cumulative_runs    += d["runs_total"]
        cumulative_wickets += 1 if d["is_wicket"] else 0

        is_wide   = d["extras_wides"] > 0
        is_noball = d["extras_noballs"] > 0
        is_legal  = not is_wide and not is_noball
        if is_legal:
            legal_balls_bowled += 1

        remaining = max(0, total_legal - legal_balls_bowled)

        d["innings_score_at_ball"]  = cumulative_runs
        d["wickets_fallen_at_ball"] = cumulative_wickets
        d["legal_balls_bowled"]     = legal_balls_bowled
        d["balls_remaining"]        = remaining

        if target is not None and remaining > 0:
            runs_needed = target - cumulative_runs
            overs_remaining = remaining / 6
            d["required_run_rate"] = round(runs_needed / overs_remaining, 2) if overs_remaining > 0 else None
        else:
            d["required_run_rate"] = None


# ── Innings aggregation ──────────────────────────────────────────────────────

def _enrich_innings(inn: dict, deliveries: list[dict]) -> None:
    if not deliveries:
        inn["total_runs"]        = 0
        inn["total_wickets"]     = 0
        inn["total_overs_faced"] = 0.0
        return

    total_runs     = sum(d["runs_total"] for d in deliveries)
    total_wickets  = sum(1 for d in deliveries if d["is_wicket"])
    legal_bowled   = sum(
        1 for d in deliveries
        if not d["extras_wides"] and not d["extras_noballs"]
    )
    complete_overs = legal_bowled // 6
    partial_balls  = legal_bowled % 6
    total_overs    = complete_overs + (partial_balls / 10)  # e.g. 19.3

    inn["total_runs"]        = total_runs
    inn["total_wickets"]     = total_wickets
    inn["total_overs_faced"] = total_overs

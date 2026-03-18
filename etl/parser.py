"""
etl/parser.py

Parses a single Cricsheet IPL JSON file into a set of normalized Python dicts,
one per database table. Does no I/O — callers handle file reading and DB writes.

Returns a ParsedMatch dataclass with:
    match, innings_list, deliveries_list, players_dict,
    match_players_list, officials_list, powerplays_list
"""

from __future__ import annotations
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from etl.team_resolver import resolve_team


# ── Data containers ──────────────────────────────────────────────────────────

@dataclass
class ParsedMatch:
    match: dict
    innings_list: list[dict]
    deliveries_list: list[dict]
    players_dict: dict[str, dict]       # player_key → player row
    match_players_list: list[dict]
    officials_list: list[dict]
    powerplays_list: list[dict]


# ── Public entry point ───────────────────────────────────────────────────────

def parse_file(path: str | Path) -> ParsedMatch:
    """Parse a Cricsheet JSON file and return all normalized records."""
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)

    match_id = Path(path).stem  # e.g. "1082591"
    return _parse(match_id, raw)


def parse_dict(match_id: str, raw: dict) -> ParsedMatch:
    """Parse an already-loaded dict (used by the scraper pipeline)."""
    return _parse(match_id, raw)


# ── Internal parser ──────────────────────────────────────────────────────────

def _parse(match_id: str, raw: dict) -> ParsedMatch:
    meta = raw.get("meta", {})
    info = raw["info"]

    match_row = _parse_match(match_id, meta, info)
    players_dict = _parse_players(info)
    match_players = _parse_match_players(match_id, info)
    officials = _parse_officials(match_id, info)

    innings_list: list[dict] = []
    deliveries_list: list[dict] = []
    powerplays_list: list[dict] = []

    super_over_count = 0

    for inn_index, inn_raw in enumerate(raw.get("innings", [])):
        is_super_over = inn_raw.get("super_over", False)
        if is_super_over:
            super_over_count += 1
            innings_number = 2 + super_over_count  # 3, 4, …
        else:
            innings_number = inn_index + 1  # 1 or 2

        inn_row, deliveries, pps = _parse_innings(
            match_id, innings_number, inn_raw, is_super_over
        )
        innings_list.append(inn_row)
        deliveries_list.extend(deliveries)
        powerplays_list.extend(pps)

    return ParsedMatch(
        match=match_row,
        innings_list=innings_list,
        deliveries_list=deliveries_list,
        players_dict=players_dict,
        match_players_list=match_players,
        officials_list=officials,
        powerplays_list=powerplays_list,
    )


# ── Match ────────────────────────────────────────────────────────────────────

def _parse_match(match_id: str, meta: dict, info: dict) -> dict:
    outcome = info.get("outcome", {})
    by = outcome.get("by", {})
    toss = info.get("toss", {})
    event = info.get("event", {})
    dates = info.get("dates", [])

    return {
        "match_id":          match_id,
        "data_version":      meta.get("data_version"),
        "created_date":      meta.get("created"),
        "revision":          meta.get("revision"),
        "season":            str(info.get("season", "")),
        "match_number":      event.get("match_number"),
        "event_name":        event.get("name"),
        "match_type":        info.get("match_type"),
        "gender":            info.get("gender"),
        "team_type":         info.get("team_type"),
        "balls_per_over":    info.get("balls_per_over", 6),
        "overs":             info.get("overs", 20),
        "venue":             info.get("venue"),
        "city":              info.get("city"),
        "match_date":        dates[0] if dates else None,
        "team1":             resolve_team(info["teams"][0]) if len(info.get("teams", [])) > 0 else None,
        "team2":             resolve_team(info["teams"][1]) if len(info.get("teams", [])) > 1 else None,
        "toss_winner":       resolve_team(toss.get("winner", "")),
        "toss_decision":     toss.get("decision"),
        "outcome_winner":    resolve_team(outcome.get("winner", "")) if outcome.get("winner") else None,
        "outcome_by_runs":   by.get("runs"),
        "outcome_by_wickets": by.get("wickets"),
        "outcome_method":    outcome.get("method"),
        "outcome_result":    outcome.get("result"),
        "outcome_eliminator": outcome.get("eliminator"),
        "player_of_match":   info.get("player_of_match", []),
    }


# ── Players ──────────────────────────────────────────────────────────────────

def _parse_players(info: dict) -> dict[str, dict]:
    """Return {player_key: player_row} for everyone in the registry."""
    registry = info.get("registry", {}).get("people", {})
    players: dict[str, dict] = {}
    for name, key in registry.items():
        players[key] = {
            "player_key":  key,
            "player_name": name,
        }
    return players


def _parse_match_players(match_id: str, info: dict) -> list[dict]:
    """One row per player per team in the playing XI."""
    registry = info.get("registry", {}).get("people", {})
    rows: list[dict] = []
    for team, players in info.get("players", {}).items():
        for name in players:
            rows.append({
                "match_id":    match_id,
                "team":        resolve_team(team),
                "player_key":  registry.get(name, name),  # fallback to name if not in registry
                "player_name": name,
            })
    return rows


# ── Officials ────────────────────────────────────────────────────────────────

def _parse_officials(match_id: str, info: dict) -> list[dict]:
    officials_raw = info.get("officials", {})
    role_map = {
        "umpires":          "umpire",
        "tv_umpires":       "tv_umpire",
        "reserve_umpires":  "reserve_umpire",
        "match_referees":   "match_referee",
    }
    rows: list[dict] = []
    for raw_key, role in role_map.items():
        for name in officials_raw.get(raw_key, []):
            rows.append({
                "match_id": match_id,
                "role":     role,
                "name":     name,
            })
    return rows


# ── Innings ──────────────────────────────────────────────────────────────────

def _parse_innings(
    match_id: str,
    innings_number: int,
    inn_raw: dict,
    is_super_over: bool,
) -> tuple[dict, list[dict], list[dict]]:
    """Returns (innings_row, deliveries, powerplays)."""

    target = inn_raw.get("target", {})
    absent = inn_raw.get("absent_hurt", [])

    inn_row = {
        "match_id":       match_id,
        "innings_number": innings_number,
        "team":           resolve_team(inn_raw["team"]),
        "is_super_over":  is_super_over,
        "target_runs":    target.get("runs"),
        "target_overs":   target.get("overs"),
        "absent_hurt":    absent if absent else None,
        # totals filled in by computed.py
        "total_runs":     None,
        "total_wickets":  None,
        "total_overs_faced": None,
    }

    deliveries = _parse_deliveries(match_id, innings_number, is_super_over, inn_raw)
    powerplays = _parse_powerplays(match_id, innings_number, inn_raw)

    return inn_row, deliveries, powerplays


# ── Deliveries ───────────────────────────────────────────────────────────────

def _parse_deliveries(
    match_id: str,
    innings_number: int,
    is_super_over: bool,
    inn_raw: dict,
) -> list[dict]:
    rows: list[dict] = []
    legal_ball_counter = 0

    for over_raw in inn_raw.get("overs", []):
        over_number = over_raw["over"]  # 0-indexed

        for ball_index, delivery in enumerate(over_raw.get("deliveries", [])):
            ball_number = ball_index + 1  # 1-indexed position in the deliveries array

            extras = delivery.get("extras", {})
            runs = delivery.get("runs", {})
            wickets_raw = delivery.get("wickets", [])
            replacements = delivery.get("replacements", {})
            review = delivery.get("review", {})

            # Is this a legal delivery (not wide or no-ball)?
            is_wide   = bool(extras.get("wides"))
            is_noball = bool(extras.get("noballs"))
            is_legal  = not is_wide and not is_noball
            if is_legal:
                legal_ball_counter += 1

            # Wicket details (use first wicket for flat columns)
            is_wicket = len(wickets_raw) > 0
            first_wicket = wickets_raw[0] if is_wicket else {}
            fielders = first_wicket.get("fielders", [])

            # Impact player replacement on this ball (match-level replacements)
            match_replacements = replacements.get("match", [])
            rep = match_replacements[0] if match_replacements else {}

            # Stable delivery ID
            delivery_id = f"{match_id}_{innings_number}_{over_number}_{ball_number}"

            row = {
                "delivery_id":       delivery_id,
                "match_id":          match_id,
                "innings_number":    innings_number,
                "is_super_over":     is_super_over,
                "over_number":       over_number,
                "ball_number":       ball_number,
                "legal_ball_number": legal_ball_counter if is_legal else None,

                # Players
                "batter":            delivery["batter"],
                "bowler":            delivery["bowler"],
                "non_striker":       delivery["non_striker"],

                # Runs
                "runs_batter":       runs.get("batter", 0),
                "runs_extras":       runs.get("extras", 0),
                "runs_total":        runs.get("total", 0),

                # Extras
                "extras_wides":      extras.get("wides", 0),
                "extras_noballs":    extras.get("noballs", 0),
                "extras_byes":       extras.get("byes", 0),
                "extras_legbyes":    extras.get("legbyes", 0),
                "extras_penalty":    extras.get("penalty", 0),

                # Wicket (primary)
                "is_wicket":         is_wicket,
                "wicket_kind":       first_wicket.get("kind"),
                "wicket_player_out": first_wicket.get("player_out"),
                "wicket_fielders":   json.dumps(fielders) if fielders else None,
                "wickets_raw":       json.dumps(wickets_raw) if wickets_raw else None,

                # DRS Review
                "review_by":         review.get("by"),
                "review_umpire":     review.get("umpire"),
                "review_batter":     review.get("batter"),
                "review_decision":   review.get("decision"),
                "review_type":       review.get("type"),

                # Impact player replacement
                "replacement_in":     rep.get("in"),
                "replacement_out":    rep.get("out"),
                "replacement_team":   rep.get("team"),
                "replacement_reason": rep.get("reason"),

                # Computed columns — filled by computed.py
                "phase":                    _phase(over_number),
                "innings_score_at_ball":    None,
                "wickets_fallen_at_ball":   None,
                "legal_balls_bowled":       None,
                "balls_remaining":          None,
                "required_run_rate":        None,
            }
            rows.append(row)

    return rows


def _phase(over_number: int) -> str:
    if over_number <= 5:
        return "powerplay"
    if over_number <= 14:
        return "middle"
    return "death"


# ── Powerplays ───────────────────────────────────────────────────────────────

def _parse_powerplays(match_id: str, innings_number: int, inn_raw: dict) -> list[dict]:
    rows: list[dict] = []
    for pp in inn_raw.get("powerplays", []):
        rows.append({
            "match_id":       match_id,
            "innings_number": innings_number,
            "pp_type":        pp.get("type"),
            "from_over":      int(pp["from"]) - 1,  # Cricsheet is 1-indexed; store 0-indexed
            "to_over":        int(pp["to"]) - 1,
        })
    return rows

"""
scraper/cricinfo.py

Scrapes ESPNcricinfo for IPL matches played on a given date and converts
them to Cricsheet-compatible ParsedMatch objects.

This is a "gray area" scraper — ESPNcricinfo's ToS doesn't explicitly allow
automated access. We mitigate risk by:
  - Only scraping once per day (not continuously)
  - Adding polite delays between requests
  - Using a real browser User-Agent

How it works:
  1. Hit the ESPNcricinfo match schedule page for the IPL series
  2. Find any matches played on the target date
  3. For each match, fetch the full scorecard JSON from their internal API
  4. Map the response fields to the Cricsheet schema
  5. Return list of (match_id, ParsedMatch) tuples

NOTE: ESPNcricinfo's internal API endpoints and HTML structure can change.
If scraping breaks, check the network tab on a scorecard page to find
the current API endpoint.
"""

from __future__ import annotations
import json
import logging
import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

from etl.parser import ParsedMatch, parse_dict

logger = logging.getLogger(__name__)

# Polite delay between requests (seconds)
REQUEST_DELAY = 2.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ESPNcricinfo series ID for IPL — update each season if needed
# 2025 IPL series ID: 1449924  (verify on cricinfo before April)
IPL_SERIES_ID = "1449924"

# Match schedule API
SCHEDULE_URL = (
    "https://www.espncricinfo.com/series/{series_id}/match-schedule-fixtures-and-results"
)


# ── Public API ───────────────────────────────────────────────────────────────

def scrape_matches_on_date(date_str: str) -> list[tuple[str, ParsedMatch]]:
    """
    Scrape all IPL matches played on date_str (YYYY-MM-DD).
    Returns list of (match_id, ParsedMatch) tuples.
    match_id is the ESPNcricinfo match ID (used as our match_id key).
    """
    logger.info(f"Scraping ESPNcricinfo for IPL matches on {date_str}")

    match_ids = _get_match_ids_for_date(date_str)
    if not match_ids:
        return []

    results: list[tuple[str, ParsedMatch]] = []
    for cricinfo_match_id in match_ids:
        time.sleep(REQUEST_DELAY)
        try:
            raw = _fetch_match_json(cricinfo_match_id)
            if raw is None:
                continue
            parsed = parse_dict(cricinfo_match_id, raw)
            results.append((cricinfo_match_id, parsed))
            logger.info(f"  Scraped match {cricinfo_match_id}")
        except Exception as exc:
            logger.warning(f"  Failed to scrape match {cricinfo_match_id}: {exc}")

    return results


# ── Internal helpers ─────────────────────────────────────────────────────────

def _get_match_ids_for_date(date_str: str) -> list[str]:
    """
    Fetch the IPL schedule page and extract match IDs for the given date.
    Returns a list of ESPNcricinfo match ID strings.
    """
    url = SCHEDULE_URL.format(series_id=IPL_SERIES_ID)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error(f"Failed to fetch schedule: {exc}")
        return []

    # ESPNcricinfo embeds match data in a __NEXT_DATA__ script tag
    soup = BeautifulSoup(resp.text, "lxml")
    next_data_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not next_data_tag:
        logger.error("Could not find __NEXT_DATA__ in schedule page")
        return []

    try:
        page_data = json.loads(next_data_tag.string)
    except json.JSONDecodeError:
        logger.error("Failed to parse __NEXT_DATA__ JSON")
        return []

    # Navigate the Next.js page props to find match list
    # Path may vary — inspect network tab if this breaks
    try:
        matches = (
            page_data["props"]["appData"]["props"]["data"]["content"]["matches"]
        )
    except (KeyError, TypeError):
        logger.warning("Could not locate match list in page data — structure may have changed")
        return []

    ids: list[str] = []
    for match in matches:
        match_date = match.get("startDate", "")[:10]  # "YYYY-MM-DD"
        status = match.get("matchStatus", "")
        if match_date == date_str and status.lower() == "result":
            match_id = str(match.get("objectId") or match.get("id", ""))
            if match_id:
                ids.append(match_id)

    logger.info(f"Found {len(ids)} completed match(es) on {date_str}")
    return ids


def _fetch_match_json(cricinfo_match_id: str) -> Optional[dict]:
    """
    Fetch the full ball-by-ball data for a match from ESPNcricinfo's
    internal JSON API and convert to Cricsheet schema.

    ESPNcricinfo provides commentary/ball-by-ball data at:
    https://hs-consumer-api.espncricinfo.com/v1/pages/match/innings
    with query params matchId and inningNumber.

    We also fetch match info from:
    https://hs-consumer-api.espncricinfo.com/v1/pages/match/home?matchId=...
    """
    # Fetch match metadata
    info_url = (
        f"https://hs-consumer-api.espncricinfo.com/v1/pages/match/home"
        f"?matchId={cricinfo_match_id}&lang=en"
    )
    try:
        info_resp = requests.get(info_url, headers=HEADERS, timeout=15)
        info_resp.raise_for_status()
        info_data = info_resp.json()
    except Exception as exc:
        logger.error(f"Failed to fetch match info for {cricinfo_match_id}: {exc}")
        return None

    # Convert ESPNcricinfo format → Cricsheet-compatible dict
    # This mapping is partial — extend as you discover the full response structure
    try:
        raw = _convert_to_cricsheet(cricinfo_match_id, info_data)
    except Exception as exc:
        logger.error(f"Conversion failed for {cricinfo_match_id}: {exc}")
        return None

    return raw


def _convert_to_cricsheet(match_id: str, espn_data: dict) -> dict:
    """
    Convert ESPNcricinfo API response to a Cricsheet-compatible dict.

    This is a best-effort mapping. The structure follows Cricsheet's JSON spec
    so that parse_dict() can process it without modification.

    TODO: Expand this mapping as the ESPNcricinfo API response is explored.
    Fields that can't be mapped will be left as None/empty — the parser
    handles optional fields gracefully.
    """
    match_info = espn_data.get("match", {})
    teams = espn_data.get("teams", [])

    team_names = [t.get("longName", t.get("name", "")) for t in teams[:2]]

    toss = match_info.get("toss", {})
    outcome = match_info.get("result", {})

    cricsheet = {
        "meta": {
            "data_version": "1.0.0",
            "created":      match_info.get("startDate", "")[:10],
            "revision":     1,
        },
        "info": {
            "balls_per_over": 6,
            "city":           match_info.get("ground", {}).get("town", {}).get("name"),
            "dates":          [match_info.get("startDate", "")[:10]],
            "event": {
                "name":         "Indian Premier League",
                "match_number": match_info.get("matchNumber"),
            },
            "gender":     "male",
            "match_type": "T20",
            "officials": {
                "umpires":         [u.get("name") for u in espn_data.get("officials", {}).get("umpires", [])],
                "tv_umpires":      [u.get("name") for u in espn_data.get("officials", {}).get("tvUmpires", [])],
                "reserve_umpires": [u.get("name") for u in espn_data.get("officials", {}).get("reserveUmpires", [])],
                "match_referees":  [u.get("name") for u in espn_data.get("officials", {}).get("matchReferees", [])],
            },
            "outcome": _map_outcome(outcome, team_names),
            "overs":    20,
            "player_of_match": [
                p.get("name") for p in espn_data.get("playersOfTheMatch", [])
            ],
            "players":  _map_players(teams),
            "registry": {"people": _build_registry(teams)},
            "season":   int(match_info.get("season", {}).get("year", 0) or 0),
            "team_type": "club",
            "teams":    team_names,
            "toss": {
                "winner":   toss.get("winner", {}).get("longName"),
                "decision": toss.get("decision", "").lower(),
            },
            "venue": match_info.get("ground", {}).get("longName"),
        },
        # innings are fetched separately and merged in — placeholder for now
        "innings": [],
    }

    return cricsheet


def _map_outcome(result: dict, team_names: list[str]) -> dict:
    outcome: dict = {}
    winner = result.get("winnerTeamId")
    if winner:
        # match winner team name
        outcome["winner"] = result.get("winningTeam", {}).get("longName")
        margin = result.get("winMargin", 0)
        margin_type = result.get("winByInnings", False)
        if result.get("winByRuns"):
            outcome["by"] = {"runs": margin}
        elif result.get("winByWickets"):
            outcome["by"] = {"wickets": margin}
    elif result.get("resultType") == "tie":
        outcome["result"] = "tie"
    elif result.get("resultType") == "no result":
        outcome["result"] = "no result"
    return outcome


def _map_players(teams: list[dict]) -> dict[str, list[str]]:
    result: dict = {}
    for team in teams[:2]:
        name = team.get("longName", team.get("name", ""))
        players = [p.get("player", {}).get("longName", "") for p in team.get("players", [])]
        result[name] = [p for p in players if p]
    return result


def _build_registry(teams: list[dict]) -> dict[str, str]:
    """Build name → cricinfo_id registry (using numeric cricinfo IDs as keys)."""
    registry: dict = {}
    for team in teams[:2]:
        for p in team.get("players", []):
            player = p.get("player", {})
            name = player.get("longName", "")
            pid = str(player.get("objectId", player.get("id", "")))
            if name and pid:
                registry[name] = pid
    return registry

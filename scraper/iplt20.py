"""
scraper/iplt20.py

Scrapes IPL auction and squad data from iplt20.com (official IPL website)
to populate the player_season table.

Fields captured per player per season:
  team, acquisition_type (auctioned/retained/rtm/traded),
  auction_price_lakhs, is_overseas

Approach:
  1. Fetch the squads page for each IPL season
  2. Extract player list, team, price, overseas status
  3. Match to our players table via cricinfo_id (fetched from ESPNcricinfo links on the page)
  4. Upsert into player_season

NOTE: iplt20.com structure changes season to season. This scraper targets
the 2025 season structure. Older seasons may need manual CSV imports.
"""

from __future__ import annotations
import logging
import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

REQUEST_DELAY = 2.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# IPL team squad pages — update slug if URL structure changes
TEAM_SQUADS_URL = "https://www.iplt20.com/teams/{team_slug}/squad"

# Mapping from iplt20 team slugs to our canonical names
TEAM_SLUGS = {
    "chennai-super-kings":       "Chennai Super Kings",
    "mumbai-indians":            "Mumbai Indians",
    "royal-challengers-bengaluru": "Royal Challengers Bengaluru",
    "kolkata-knight-riders":     "Kolkata Knight Riders",
    "sunrisers-hyderabad":       "Sunrisers Hyderabad",
    "rajasthan-royals":          "Rajasthan Royals",
    "delhi-capitals":            "Delhi Capitals",
    "punjab-kings":              "Punjab Kings",
    "lucknow-super-giants":      "Lucknow Super Giants",
    "gujarat-titans":            "Gujarat Titans",
}


def scrape_current_season(conn, season: str) -> int:
    """
    Scrape squad data for all teams for the given season.
    Returns total rows upserted into player_season.
    """
    # Build cricinfo_id → player_key lookup from our DB
    with conn.cursor() as cur:
        cur.execute("SELECT cricinfo_id, player_key FROM players WHERE cricinfo_id IS NOT NULL")
        cricinfo_map: dict[int, str] = {row[0]: row[1] for row in cur.fetchall()}

    total = 0
    for slug, canonical_team in TEAM_SLUGS.items():
        time.sleep(REQUEST_DELAY)
        rows = _scrape_team_squad(slug, canonical_team, season, cricinfo_map)
        if rows:
            _upsert_player_season(conn, rows)
            total += len(rows)
            logger.info(f"  {canonical_team}: {len(rows)} players")

    logger.info(f"player_season upserted: {total} rows for season {season}")
    return total


def _scrape_team_squad(
    slug: str,
    team: str,
    season: str,
    cricinfo_map: dict[int, str],
) -> list[dict]:
    url = TEAM_SQUADS_URL.format(team_slug=slug)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning(f"Failed to fetch squad for {team}: {exc}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    rows = []

    # iplt20.com player cards — inspect the page structure if this breaks
    player_cards = soup.select(".player-card, .squad-player, [class*='player']")
    for card in player_cards:
        player_data = _parse_player_card(card, team, season, cricinfo_map)
        if player_data:
            rows.append(player_data)

    if not rows:
        logger.warning(f"No players parsed for {team} — page structure may have changed")

    return rows


def _parse_player_card(card, team: str, season: str, cricinfo_map: dict[int, str]) -> Optional[dict]:
    """
    Parse a single player card element from iplt20.com squad page.
    Returns a player_season dict or None if player can't be matched.
    """
    # Try to extract ESPNcricinfo player ID from any link on the card
    cricinfo_id = None
    for a in card.find_all("a", href=True):
        match = re.search(r"/player/[^/]+-(\d+)", a["href"])
        if match:
            cricinfo_id = int(match.group(1))
            break

    player_key = cricinfo_map.get(cricinfo_id) if cricinfo_id else None
    if not player_key:
        return None

    # Price — shown as "₹X Cr" or "X Lakhs" depending on page
    price_lakhs = None
    price_el = card.select_one("[class*='price'], [class*='amount'], [class*='value']")
    if price_el:
        price_lakhs = _parse_price(price_el.get_text())

    # Overseas flag
    is_overseas = bool(card.select_one("[class*='overseas'], [class*='foreign']"))

    # Acquisition type — iplt20 labels retained/RTM/auctioned players
    acquisition = "auctioned"
    card_text = card.get_text().lower()
    if "retained" in card_text:
        acquisition = "retained"
    elif "rtm" in card_text:
        acquisition = "rtm"
    elif "traded" in card_text or "trade" in card_text:
        acquisition = "traded"
    elif "uncapped" in card_text or "draft" in card_text:
        acquisition = "draft"

    return {
        "player_key":           player_key,
        "season":               season,
        "team":                 team,
        "acquisition_type":     acquisition,
        "auction_price_lakhs":  price_lakhs,
        "is_overseas":          is_overseas,
    }


def _parse_price(text: str) -> Optional[float]:
    """Parse price strings like '₹15 Cr', '1.5 Cr', '75 Lakhs' into lakhs."""
    text = text.replace("₹", "").replace(",", "").strip().lower()
    cr_match = re.search(r"([\d.]+)\s*cr", text)
    lakh_match = re.search(r"([\d.]+)\s*lakh", text)
    if cr_match:
        return float(cr_match.group(1)) * 100  # crores → lakhs
    if lakh_match:
        return float(lakh_match.group(1))
    return None


def _upsert_player_season(conn, rows: list[dict]):
    with conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO player_season
                    (player_key, season, team, acquisition_type, auction_price_lakhs, is_overseas)
                VALUES %s
                ON CONFLICT (player_key, season) DO UPDATE SET
                    team                = EXCLUDED.team,
                    acquisition_type    = EXCLUDED.acquisition_type,
                    auction_price_lakhs = EXCLUDED.auction_price_lakhs,
                    is_overseas         = EXCLUDED.is_overseas
                """,
                [(r["player_key"], r["season"], r["team"], r["acquisition_type"],
                  r["auction_price_lakhs"], r["is_overseas"]) for r in rows],
            )

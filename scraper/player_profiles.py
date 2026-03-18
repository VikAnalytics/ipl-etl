"""
scraper/player_profiles.py

Fetches player profile data from Wikidata for all players that have
a cricinfo_id populated in our players table.

Wikidata is free, open, and has no rate limiting for reasonable queries.
It links to ESPNcricinfo via property P1244 (ESPNcricinfo player ID).

Fields fetched:
  nationality, batting_style, bowling_style, playing_role, date_of_birth

Approach: single SPARQL batch query for all 925 cricinfo IDs at once.
Falls back to chunked queries if the batch is too large.
"""

from __future__ import annotations
import logging
import time
from typing import Optional

import requests
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

HEADERS = {
    "User-Agent": "ipl-etl-enrichment/1.0 (cricket analytics project)",
    "Accept": "application/sparql-results+json",
}

CHUNK_SIZE = 200  # IDs per SPARQL query


def enrich_all(conn, force: bool = False) -> tuple[int, int]:
    """
    Fetch and store profiles for all players with a cricinfo_id.
    Returns (success_count, error_count).
    """
    with conn.cursor() as cur:
        if force:
            cur.execute("SELECT player_key, cricinfo_id FROM players WHERE cricinfo_id IS NOT NULL")
        else:
            cur.execute("""
                SELECT player_key, cricinfo_id FROM players
                WHERE cricinfo_id IS NOT NULL
                AND nationality IS NULL
                AND playing_role IS NULL
            """)
        players = cur.fetchall()

    if not players:
        logger.info("No players need enrichment")
        return 0, 0

    logger.info(f"Querying Wikidata for {len(players)} players")

    # Build cricinfo_id → player_key map
    cricinfo_to_key: dict[str, str] = {str(cid): pk for pk, cid in players}
    cricinfo_ids = list(cricinfo_to_key.keys())

    all_results: dict[str, dict] = {}

    # Query in chunks to stay within SPARQL URL length limits
    for i in range(0, len(cricinfo_ids), CHUNK_SIZE):
        chunk = cricinfo_ids[i:i + CHUNK_SIZE]
        time.sleep(1.0)  # polite delay
        results = _query_wikidata(chunk)
        all_results.update(results)
        logger.info(f"  Queried {min(i + CHUNK_SIZE, len(cricinfo_ids))}/{len(cricinfo_ids)} — {len(all_results)} matched so far")

    # Build update rows
    updates = []
    for cricinfo_id, profile in all_results.items():
        player_key = cricinfo_to_key.get(cricinfo_id)
        if player_key:
            updates.append((
                profile.get("nationality"),
                profile.get("batting_style"),
                profile.get("bowling_style"),
                profile.get("playing_role"),
                profile.get("date_of_birth"),
                player_key,
            ))

    if updates:
        _write_updates(conn, updates)

    success = len(updates)
    error = len(players) - success
    logger.info(f"Wikidata enrichment done — {success} enriched, {error} not found in Wikidata")
    return success, error


def _query_wikidata(cricinfo_ids: list[str]) -> dict[str, dict]:
    """
    Query Wikidata for a batch of cricinfo IDs.
    Returns {cricinfo_id: profile_dict}.
    """
    values_clause = " ".join(f'"{cid}"' for cid in cricinfo_ids)

    query = f"""
    SELECT ?cricinfo_id
           ?dob
           (SAMPLE(?nationalityLabel) AS ?nationality)
           (SAMPLE(?batting_styleLabel) AS ?batting_style)
           (SAMPLE(?bowling_styleLabel) AS ?bowling_style)
           (SAMPLE(?roleLabel) AS ?playing_role)
    WHERE {{
      VALUES ?cricinfo_id {{ {values_clause} }}
      ?player wdt:P2697 ?cricinfo_id.
      OPTIONAL {{ ?player wdt:P569 ?dob. }}
      OPTIONAL {{
        ?player wdt:P27 ?nationality_item.
        ?nationality_item rdfs:label ?nationalityLabel.
        FILTER(LANG(?nationalityLabel) = "en")
      }}
      OPTIONAL {{
        ?player wdt:P1750 ?batting_style_item.
        ?batting_style_item rdfs:label ?batting_styleLabel.
        FILTER(LANG(?batting_styleLabel) = "en")
      }}
      OPTIONAL {{
        ?player wdt:P1751 ?bowling_style_item.
        ?bowling_style_item rdfs:label ?bowling_styleLabel.
        FILTER(LANG(?bowling_styleLabel) = "en")
      }}
      OPTIONAL {{
        ?player wdt:P2828 ?role_item.
        ?role_item rdfs:label ?roleLabel.
        FILTER(LANG(?roleLabel) = "en")
      }}
    }}
    GROUP BY ?cricinfo_id ?dob
    """

    try:
        resp = requests.get(
            WIKIDATA_SPARQL,
            params={"query": query, "format": "json"},
            headers=HEADERS,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning(f"Wikidata query failed: {exc}")
        return {}

    results: dict[str, dict] = {}
    for row in data.get("results", {}).get("bindings", []):
        cricinfo_id = row.get("cricinfo_id", {}).get("value", "")
        if not cricinfo_id:
            continue

        dob_raw = row.get("dob", {}).get("value", "")
        dob = dob_raw[:10] if dob_raw else None  # "1988-11-05T00:00:00Z" → "1988-11-05"

        results[cricinfo_id] = {
            "nationality":   row.get("nationality", {}).get("value"),
            "batting_style": row.get("batting_style", {}).get("value"),
            "bowling_style": row.get("bowling_style", {}).get("value"),
            "playing_role":  row.get("playing_role", {}).get("value"),
            "date_of_birth": dob,
        }

    return results


def _write_updates(conn, updates: list):
    with conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                UPDATE players AS p SET
                    nationality   = v.nationality,
                    batting_style = v.batting_style,
                    bowling_style = v.bowling_style,
                    playing_role  = v.playing_role,
                    date_of_birth = v.dob::date,
                    updated_at    = NOW()
                FROM (VALUES %s) AS v(nationality, batting_style, bowling_style, playing_role, dob, player_key)
                WHERE p.player_key = v.player_key
                """,
                updates,
                template="(%s, %s, %s, %s, %s, %s)",
            )
    logger.info(f"Wrote {len(updates)} player profile updates to DB")

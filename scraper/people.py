"""
scraper/people.py

Downloads Cricsheet's people.csv and populates cricinfo_id + full_name
in the players table. This is the bridge between our Cricsheet hex IDs
and ESPNcricinfo numeric IDs — no name matching needed.

CSV columns: identifier, name, unique_name, cricinfo, ...
  identifier → player_key in our DB
  cricinfo   → ESPNcricinfo numeric player ID
"""

from __future__ import annotations
import csv
import io
import logging

import requests
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

PEOPLE_CSV_URL = "https://cricsheet.org/register/people.csv"


def download_and_load(conn) -> int:
    logger.info("Downloading Cricsheet people.csv...")
    try:
        resp = requests.get(PEOPLE_CSV_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error(f"Failed to download people.csv: {exc}")
        return 0

    rows = list(csv.DictReader(io.StringIO(resp.text)))
    logger.info(f"Downloaded {len(rows)} records from people.csv")

    lookup: dict[str, dict] = {}
    for row in rows:
        cs_id = row.get("identifier", "").strip()
        cricinfo_raw = row.get("key_cricinfo", "").strip()
        unique_name = row.get("unique_name", "").strip()
        name = row.get("name", "").strip()
        if cs_id:
            lookup[cs_id] = {
                "cricinfo_id": int(cricinfo_raw) if cricinfo_raw.isdigit() else None,
                "full_name":   unique_name or name or None,
            }

    with conn.cursor() as cur:
        cur.execute("SELECT player_key FROM players")
        our_keys = {row[0] for row in cur.fetchall()}

    updates = [
        (v["cricinfo_id"], v["full_name"], k)
        for k, v in lookup.items()
        if k in our_keys and v["cricinfo_id"] is not None
    ]

    if not updates:
        logger.warning("No cricinfo_id matches found — check people.csv column names")
        return 0

    with conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                UPDATE players AS p SET
                    cricinfo_id = v.cricinfo_id::int,
                    full_name   = v.full_name,
                    updated_at  = NOW()
                FROM (VALUES %s) AS v(cricinfo_id, full_name, player_key)
                WHERE p.player_key = v.player_key::varchar
                """,
                updates,
                template="(%s, %s, %s)",
            )

    logger.info(f"Updated cricinfo_id for {len(updates)} / {len(our_keys)} players")
    return len(updates)

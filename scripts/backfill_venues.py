"""
scripts/backfill_venues.py

Normalizes all venue name variants in existing matches rows to canonical names.

Run once after deploying venue_resolver.py.

Tables updated:
  matches — venue
"""

from __future__ import annotations
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv()

from etl.loader import get_connection
from etl.venue_resolver import resolve_venue

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def main():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("SELECT match_id, venue FROM matches WHERE venue IS NOT NULL")
        rows = cur.fetchall()

    updates = []
    changed = 0
    for match_id, venue in rows:
        canonical = resolve_venue(venue)
        updates.append((canonical, match_id))
        if canonical != venue:
            logger.info(f"  {venue!r}  →  {canonical!r}")
            changed += 1

    with conn:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                "UPDATE matches AS m SET venue = v.venue FROM (VALUES %s) AS v(venue, match_id) WHERE m.match_id = v.match_id",
                updates,
                template="(%s, %s)",
            )

    conn.close()
    logger.info(f"Done. {changed} venue(s) normalized out of {len(rows)} matches.")


if __name__ == "__main__":
    main()

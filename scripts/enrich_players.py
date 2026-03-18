"""
scripts/enrich_players.py

Orchestrates the full player enrichment pipeline:

  Step 1 — people.csv    : Cricsheet hex ID → ESPNcricinfo numeric ID
  Step 2 — player profiles: ESPNcricinfo API → nationality, role, style, DOB
  Step 3 — iplt20 squads : auction price, retention, overseas flag

Run steps independently or all at once:
  python scripts/enrich_players.py              # all steps
  python scripts/enrich_players.py --step 1     # only people.csv
  python scripts/enrich_players.py --step 2     # only profiles
  python scripts/enrich_players.py --step 3     # only auction data
  python scripts/enrich_players.py --step 2 --force  # re-fetch already enriched
"""

from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv()

from etl.loader import get_connection
from scraper.people import download_and_load
from scraper.player_profiles import enrich_all
from scraper.iplt20 import scrape_current_season

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CURRENT_SEASON = "2025"


def main():
    args = _parse_args()
    steps = {args.step} if args.step else {1, 2, 3}

    conn = get_connection()

    if 1 in steps:
        logger.info("=== Step 1: Cricsheet people.csv → cricinfo_id ===")
        n = download_and_load(conn)
        logger.info(f"Step 1 done: {n} players updated with cricinfo_id")

    if 2 in steps:
        logger.info("=== Step 2: ESPNcricinfo player profiles ===")
        ok, err = enrich_all(conn, force=args.force)
        logger.info(f"Step 2 done: {ok} enriched, {err} failed")

    if 3 in steps:
        logger.info(f"=== Step 3: iplt20.com squad/auction data (season {CURRENT_SEASON}) ===")
        n = scrape_current_season(conn, season=CURRENT_SEASON)
        logger.info(f"Step 3 done: {n} player_season rows upserted")

    conn.close()
    logger.info("Enrichment complete.")


def _parse_args():
    p = argparse.ArgumentParser(description="Enrich players table with profile and auction data")
    p.add_argument("--step",  type=int, choices=[1, 2, 3], default=None,
                   help="Run a single step only (1=people.csv, 2=profiles, 3=auction)")
    p.add_argument("--force", action="store_true",
                   help="Re-fetch profiles even if already enriched (step 2 only)")
    return p.parse_args()


if __name__ == "__main__":
    main()

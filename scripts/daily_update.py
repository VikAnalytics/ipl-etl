"""
scripts/daily_update.py

Entry point for the GitHub Actions daily cron job.
Runs after each IPL match day (triggered at 18:30 UTC = midnight IST).

Steps:
  1. Scrape ESPNcricinfo for IPL matches played today
  2. Convert each to Cricsheet-compatible JSON
  3. Parse, enrich, and upsert into Supabase

Usage:
    python scripts/daily_update.py [--date YYYY-MM-DD]

If --date is omitted, defaults to today (UTC).
"""

from __future__ import annotations
import argparse
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv()

from etl.computed import enrich
from etl.loader import get_connection, Loader
from scraper.cricinfo import scrape_matches_on_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    args = _parse_args()
    target_date = args.date or date.today().isoformat()
    logger.info(f"Running daily update for date: {target_date}")

    conn = get_connection()
    loader = Loader(conn)

    matches = scrape_matches_on_date(target_date)
    if not matches:
        logger.info("No IPL matches found for this date — nothing to load.")
        conn.close()
        return

    logger.info(f"Found {len(matches)} match(es) to process")
    success = error = 0

    for match_id, parsed in matches:
        try:
            enrich(parsed.innings_list, parsed.deliveries_list)
            rows = loader.load(parsed)
            _log_run(conn, match_id, status="success", rows_inserted=rows)
            logger.info(f"  Loaded match {match_id} ({rows} rows)")
            success += 1
        except Exception as exc:
            logger.error(f"  Failed match {match_id}: {exc}")
            _log_run(conn, match_id, status="error", error_message=str(exc))
            error += 1

    conn.close()
    logger.info(f"Done — {success} succeeded, {error} failed")
    if error:
        sys.exit(1)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None, help="Date to scrape (YYYY-MM-DD), defaults to today")
    return p.parse_args()


def _log_run(conn, match_id, status, rows_inserted=0, error_message=None):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_run_log (match_id, source_file, status, error_message, rows_inserted)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (match_id, "cricinfo_scraper", status, error_message, rows_inserted),
            )


if __name__ == "__main__":
    main()

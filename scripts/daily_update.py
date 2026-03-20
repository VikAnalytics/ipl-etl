"""
scripts/daily_update.py

Manual-trigger update script for in-season IPL matches.

How it works:
  1. Downloads the latest Cricsheet IPL JSON ZIP
  2. Finds match files not yet in etl_run_log (new matches)
  3. Runs them through the same ETL pipeline as historical_load.py

When to run:
  After each IPL match day, once you know Cricsheet has published
  the match (usually 12-24h after match). Trigger manually from
  GitHub Actions → "IPL Daily ETL" → "Run workflow".

Usage:
  python scripts/daily_update.py              # process all new matches
  python scripts/daily_update.py --dry-run    # show what would be loaded
"""

from __future__ import annotations
import argparse
import io
import json
import logging
import sys
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv()

from etl.parser import parse_dict
from etl.computed import enrich
from etl.loader import get_connection, Loader
from etl.utils import fetch_done_matches, log_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CRICSHEET_ZIP_URL = "https://cricsheet.org/downloads/ipl_json.zip"


def main():
    args = _parse_args()

    logger.info("Downloading Cricsheet IPL ZIP...")
    try:
        resp = requests.get(CRICSHEET_ZIP_URL, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error(f"Failed to download ZIP: {exc}")
        sys.exit(1)

    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    available = {
        Path(name).stem
        for name in zf.namelist()
        if name.endswith(".json")
    }
    logger.info(f"ZIP contains {len(available)} matches")

    conn = get_connection()
    already_loaded = fetch_done_matches(conn)
    new_matches = sorted(available - already_loaded)

    if not new_matches:
        logger.info("No new matches found — already up to date.")
        conn.close()
        return

    logger.info(f"Found {len(new_matches)} new match(es): {new_matches}")

    if args.dry_run:
        logger.info("Dry run — exiting without loading.")
        conn.close()
        return

    loader = Loader(conn)
    success = error = 0

    for match_id in new_matches:
        try:
            raw = _read_from_zip(zf, match_id)
            parsed = parse_dict(match_id, raw)
            enrich(parsed.innings_list, parsed.deliveries_list)
            rows = loader.load(parsed)
            log_run(conn, match_id, CRICSHEET_ZIP_URL, "success", rows_inserted=rows)
            logger.info(f"  Loaded {match_id} — {rows} rows")
            success += 1
        except Exception as exc:
            logger.error(f"  Failed {match_id}: {exc}")
            log_run(conn, match_id, CRICSHEET_ZIP_URL, "error", error_message=str(exc))
            error += 1

    conn.close()
    logger.info(f"Done — {success} loaded, {error} failed")
    if error:
        sys.exit(1)


def _read_from_zip(zf: zipfile.ZipFile, match_id: str) -> dict:
    with zf.open(f"{match_id}.json") as f:
        return json.load(f)


def _parse_args():
    p = argparse.ArgumentParser(description="Load new IPL matches from Cricsheet ZIP")
    p.add_argument("--dry-run", action="store_true", help="Show new matches without loading")
    return p.parse_args()


if __name__ == "__main__":
    main()

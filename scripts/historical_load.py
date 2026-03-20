"""
scripts/historical_load.py

One-time (re-runnable) batch loader for all Cricsheet IPL JSON files.

Usage:
    python scripts/historical_load.py [--json-dir ipl_json]

Options:
    --json-dir   Path to folder containing .json match files (default: ipl_json)
    --match-id   Load a single match ID only (for testing / reruns)
    --skip-done  Skip matches that already have a 'success' log entry

Progress is written to etl_run_log so reruns skip already-successful matches
unless --skip-done is omitted.
"""

from __future__ import annotations
import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.parser import parse_file, ParsedMatch
from etl.computed import enrich
from etl.loader import get_connection, Loader
from etl.utils import fetch_done_matches, log_run

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Entry point ──────────────────────────────────────────────────────────────

def main():
    args = _parse_args()
    json_dir = Path(args.json_dir)
    if not json_dir.exists():
        logger.error(f"JSON directory not found: {json_dir}")
        sys.exit(1)

    files = sorted(json_dir.glob("*.json"))
    if args.match_id:
        files = [f for f in files if f.stem == args.match_id]
        if not files:
            logger.error(f"Match {args.match_id} not found in {json_dir}")
            sys.exit(1)

    logger.info(f"Found {len(files)} match files in {json_dir}")

    conn = get_connection()
    already_done: set[str] = set()
    if args.skip_done:
        already_done = fetch_done_matches(conn)
        logger.info(f"Skipping {len(already_done)} already-loaded matches")

    pending = [f for f in files if f.stem not in already_done]
    logger.info(f"Loading {len(pending)} matches")

    success = error = 0
    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(pending), unit="match")
    except ImportError:
        pbar = None

    loader = Loader(conn)

    for fpath in pending:
        match_id = fpath.stem
        try:
            parsed = parse_file(fpath)
            enrich(parsed.innings_list, parsed.deliveries_list)
            rows = loader.load(parsed)
            log_run(conn, match_id, str(fpath), "success", rows_inserted=rows)
            success += 1
        except Exception as exc:
            logger.warning(f"  FAILED {match_id}: {exc}")
            log_run(conn, match_id, str(fpath), "error", error_message=str(exc))
            error += 1

        if pbar:
            pbar.set_postfix(ok=success, err=error)
            pbar.update(1)

    if pbar:
        pbar.close()

    conn.close()
    logger.info(f"Done — {success} succeeded, {error} failed")
    if error:
        sys.exit(1)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Batch-load Cricsheet IPL JSON files into Supabase")
    p.add_argument("--json-dir",  default="ipl_json", help="Path to JSON match files folder")
    p.add_argument("--match-id",  default=None,       help="Load a single match ID (for testing)")
    p.add_argument("--skip-done", action="store_true", help="Skip matches already logged as success")
    return p.parse_args()


if __name__ == "__main__":
    main()

"""
etl/utils.py

Shared utilities used by both historical_load.py and daily_update.py.
"""

from __future__ import annotations


def fetch_done_matches(conn) -> set[str]:
    """Return match_ids that have already been successfully loaded."""
    with conn.cursor() as cur:
        cur.execute("SELECT match_id FROM etl_run_log WHERE status = 'success'")
        return {row[0] for row in cur.fetchall()}


def log_run(
    conn,
    match_id: str,
    source_file: str,
    status: str,
    rows_inserted: int = 0,
    error_message: str = None,
) -> None:
    """Write an ETL run result to etl_run_log."""
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_run_log (match_id, source_file, status, error_message, rows_inserted)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (match_id, source_file, status, error_message, rows_inserted),
            )

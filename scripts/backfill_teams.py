"""
scripts/backfill_teams.py

Normalizes all team name variants in existing DB rows to canonical names,
and creates the teams table seed data.

Run once after deploying the teams table migration.

Tables updated:
  matches      — team1, team2, toss_winner, outcome_winner, outcome_eliminator
  innings      — team
  match_players — team
  deliveries   — replacement_team (rare, impact player substitutions)
"""

from __future__ import annotations
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv()

from etl.loader import get_connection
from etl.team_resolver import resolve_team, _TEAMS

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def main():
    conn = get_connection()

    # 1. Seed teams table
    _seed_teams(conn)

    # 2. Normalize matches
    _normalize_matches(conn)

    # 3. Normalize innings
    _normalize_innings(conn)

    # 4. Normalize match_players
    _normalize_match_players(conn)

    # 5. Normalize deliveries.replacement_team
    _normalize_deliveries(conn)

    conn.close()
    logger.info("Backfill complete.")


def _seed_teams(conn):
    with conn:
        with conn.cursor() as cur:
            for team in _TEAMS:
                cur.execute(
                    """
                    INSERT INTO teams (canonical_name, short_name, aliases)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (canonical_name) DO UPDATE SET
                        short_name = EXCLUDED.short_name,
                        aliases    = EXCLUDED.aliases
                    """,
                    (team["canonical"], team["short"], team["aliases"]),
                )
    logger.info(f"Seeded {len(_TEAMS)} teams")


def _normalize_matches(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT match_id, team1, team2, toss_winner, outcome_winner, outcome_eliminator FROM matches")
        rows = cur.fetchall()

    updates = []
    for match_id, t1, t2, toss, winner, elim in rows:
        new = (
            resolve_team(t1 or ""),
            resolve_team(t2 or ""),
            resolve_team(toss or "") if toss else None,
            resolve_team(winner or "") if winner else None,
            resolve_team(elim or "") if elim else None,
            match_id,
        )
        updates.append(new)

    with conn:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                """
                UPDATE matches AS m SET
                    team1             = v.team1,
                    team2             = v.team2,
                    toss_winner       = v.toss_winner,
                    outcome_winner    = v.outcome_winner,
                    outcome_eliminator = v.outcome_eliminator
                FROM (VALUES %s) AS v(team1, team2, toss_winner, outcome_winner, outcome_eliminator, match_id)
                WHERE m.match_id = v.match_id
                """,
                updates,
                template="(%s, %s, %s, %s, %s, %s)",
            )
    logger.info(f"Normalized team names in {len(updates)} matches")


def _normalize_innings(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT innings_id, team FROM innings")
        rows = cur.fetchall()

    updates = [(resolve_team(team or ""), iid) for iid, team in rows]
    with conn:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                "UPDATE innings AS i SET team = v.team FROM (VALUES %s) AS v(team, innings_id) WHERE i.innings_id = v.innings_id",
                updates,
                template="(%s, %s)",
            )
    logger.info(f"Normalized team names in {len(updates)} innings")


def _normalize_match_players(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT match_id, player_key, team FROM match_players")
        rows = cur.fetchall()

    updates = [(resolve_team(team or ""), match_id, player_key) for match_id, player_key, team in rows]
    with conn:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                """
                UPDATE match_players AS mp SET team = v.team
                FROM (VALUES %s) AS v(team, match_id, player_key)
                WHERE mp.match_id = v.match_id AND mp.player_key = v.player_key
                """,
                updates,
                template="(%s, %s, %s)",
            )
    logger.info(f"Normalized team names in {len(updates)} match_players rows")


def _normalize_deliveries(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT delivery_id, replacement_team FROM deliveries WHERE replacement_team IS NOT NULL")
        rows = cur.fetchall()

    if not rows:
        logger.info("No replacement_team values to normalize")
        return

    updates = [(resolve_team(team), did) for did, team in rows]
    with conn:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                "UPDATE deliveries AS d SET replacement_team = v.team FROM (VALUES %s) AS v(team, delivery_id) WHERE d.delivery_id = v.delivery_id",
                updates,
                template="(%s, %s)",
            )
    logger.info(f"Normalized replacement_team in {len(updates)} deliveries")


if __name__ == "__main__":
    main()

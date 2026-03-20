"""
etl/loader.py

Upserts a ParsedMatch (after computed enrichment) into Supabase/Postgres.

Uses psycopg2 with execute_values for efficient bulk inserts.
Every table uses INSERT ... ON CONFLICT DO UPDATE so the loader is idempotent
— safe to re-run for any match.

Usage:
    conn = get_connection()
    loader = Loader(conn)
    loader.load(parsed_match)
    conn.close()
"""

from __future__ import annotations
import json
import os
import logging
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values

from etl.parser import ParsedMatch

logger = logging.getLogger(__name__)


# ── Connection ───────────────────────────────────────────────────────────────

def get_connection() -> psycopg2.extensions.connection:
    """Create a psycopg2 connection from DATABASE_URL env var."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise EnvironmentError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(url)


# ── Loader ───────────────────────────────────────────────────────────────────

class Loader:
    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn

    def load(self, parsed: ParsedMatch) -> int:
        """
        Upsert all records from a ParsedMatch.
        Returns total rows inserted/updated across all tables.
        """
        total = 0
        with self.conn:
            with self.conn.cursor() as cur:
                total += self._upsert_match(cur, parsed.match)
                total += self._upsert_players(cur, parsed.players_dict)
                total += self._upsert_match_players(cur, parsed.match_players_list)
                total += self._upsert_officials(cur, parsed.officials_list)
                total += self._upsert_innings(cur, parsed.innings_list)
                # powerplays need innings_ids — fetch them first
                innings_id_map = self._fetch_innings_ids(cur, parsed.match["match_id"])
                total += self._upsert_powerplays(cur, parsed.powerplays_list)
                total += self._upsert_deliveries(cur, parsed.deliveries_list, innings_id_map)
        return total

    # ── Match ────────────────────────────────────────────────────────────────

    def _upsert_match(self, cur, row: dict) -> int:
        sql = """
            INSERT INTO matches (
                match_id, data_version, created_date, revision,
                season, match_number, event_name,
                match_type, gender, team_type, balls_per_over, overs,
                venue, city, match_date,
                team1, team2,
                toss_winner, toss_decision,
                outcome_winner, outcome_by_runs, outcome_by_wickets,
                outcome_method, outcome_result, outcome_eliminator,
                player_of_match
            ) VALUES %s
            ON CONFLICT (match_id) DO UPDATE SET
                data_version    = EXCLUDED.data_version,
                revision        = EXCLUDED.revision,
                outcome_winner  = EXCLUDED.outcome_winner,
                player_of_match = EXCLUDED.player_of_match,
                updated_at      = NOW()
        """
        values = [(
            row["match_id"], row["data_version"], row["created_date"], row["revision"],
            row["season"], row["match_number"], row["event_name"],
            row["match_type"], row["gender"], row["team_type"],
            row["balls_per_over"], row["overs"],
            row["venue"], row["city"], row["match_date"],
            row["team1"], row["team2"],
            row["toss_winner"], row["toss_decision"],
            row["outcome_winner"], row["outcome_by_runs"], row["outcome_by_wickets"],
            row["outcome_method"], row["outcome_result"], row["outcome_eliminator"],
            row["player_of_match"],
        )]
        execute_values(cur, sql, values)
        return 1

    # ── Players ──────────────────────────────────────────────────────────────

    def _upsert_players(self, cur, players: dict) -> int:
        if not players:
            return 0
        sql = """
            INSERT INTO players (player_key, player_name)
            VALUES %s
            ON CONFLICT (player_key) DO UPDATE SET
                player_name = EXCLUDED.player_name
        """
        values = [(p["player_key"], p["player_name"]) for p in players.values()]
        execute_values(cur, sql, values)
        return len(values)

    # ── Match players ────────────────────────────────────────────────────────

    def _upsert_match_players(self, cur, rows: list[dict]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO match_players (match_id, team, player_key, player_name)
            VALUES %s
            ON CONFLICT (match_id, player_key) DO UPDATE SET
                team        = EXCLUDED.team,
                player_name = EXCLUDED.player_name
        """
        values = [(r["match_id"], r["team"], r["player_key"], r["player_name"]) for r in rows]
        execute_values(cur, sql, values)
        return len(values)

    # ── Officials ────────────────────────────────────────────────────────────

    def _upsert_officials(self, cur, rows: list[dict]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO officials (match_id, role, name)
            VALUES %s
            ON CONFLICT (match_id, role, name) DO NOTHING
        """
        values = [(r["match_id"], r["role"], r["name"]) for r in rows]
        execute_values(cur, sql, values)
        return len(values)

    # ── Innings ──────────────────────────────────────────────────────────────

    def _upsert_innings(self, cur, rows: list[dict]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO innings (
                match_id, innings_number, team, is_super_over,
                target_runs, target_overs, absent_hurt,
                total_runs, total_wickets, total_overs_faced
            ) VALUES %s
            ON CONFLICT (match_id, innings_number) DO UPDATE SET
                total_runs        = EXCLUDED.total_runs,
                total_wickets     = EXCLUDED.total_wickets,
                total_overs_faced = EXCLUDED.total_overs_faced,
                target_runs       = EXCLUDED.target_runs,
                absent_hurt       = EXCLUDED.absent_hurt
        """
        values = [(
            r["match_id"], r["innings_number"], r["team"], r["is_super_over"],
            r["target_runs"], r["target_overs"], r["absent_hurt"],
            r["total_runs"], r["total_wickets"], r["total_overs_faced"],
        ) for r in rows]
        execute_values(cur, sql, values)
        return len(values)

    def _fetch_innings_ids(self, cur, match_id: str) -> dict[int, int]:
        """Returns {innings_number: innings_id} for a match."""
        cur.execute(
            "SELECT innings_number, innings_id FROM innings WHERE match_id = %s",
            (match_id,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}

    # ── Powerplays ───────────────────────────────────────────────────────────

    def _upsert_powerplays(self, cur, rows: list[dict]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO powerplays (match_id, innings_number, pp_type, from_over, to_over)
            VALUES %s
            ON CONFLICT (match_id, innings_number, pp_type) DO UPDATE SET
                from_over = EXCLUDED.from_over,
                to_over   = EXCLUDED.to_over
        """
        values = [(r["match_id"], r["innings_number"], r["pp_type"], r["from_over"], r["to_over"]) for r in rows]
        execute_values(cur, sql, values)
        return len(values)

    # ── Deliveries ───────────────────────────────────────────────────────────

    def _upsert_deliveries(self, cur, rows: list[dict], innings_id_map: dict[int, int]) -> int:
        if not rows:
            return 0

        sql = """
            INSERT INTO deliveries (
                delivery_id, match_id, innings_id, innings_number, is_super_over,
                over_number, ball_number, legal_ball_number,
                batter, bowler, non_striker,
                runs_batter, runs_extras, runs_total,
                extras_wides, extras_noballs, extras_byes, extras_legbyes, extras_penalty,
                is_wicket, wicket_kind, wicket_player_out, wicket_fielders, wickets_raw,
                review_by, review_umpire, review_batter, review_decision, review_type,
                replacement_in, replacement_out, replacement_team, replacement_reason,
                phase,
                innings_score_at_ball, wickets_fallen_at_ball,
                legal_balls_bowled, balls_remaining, required_run_rate
            ) VALUES %s
            ON CONFLICT (delivery_id) DO UPDATE SET
                innings_score_at_ball  = EXCLUDED.innings_score_at_ball,
                wickets_fallen_at_ball = EXCLUDED.wickets_fallen_at_ball,
                legal_balls_bowled     = EXCLUDED.legal_balls_bowled,
                balls_remaining        = EXCLUDED.balls_remaining,
                required_run_rate      = EXCLUDED.required_run_rate,
                is_wicket              = EXCLUDED.is_wicket,
                wicket_kind            = EXCLUDED.wicket_kind,
                wicket_player_out      = EXCLUDED.wicket_player_out,
                wicket_fielders        = EXCLUDED.wicket_fielders
        """

        values = []
        for r in rows:
            innings_id = innings_id_map.get(r["innings_number"])
            values.append((
                r["delivery_id"], r["match_id"], innings_id, r["innings_number"], r["is_super_over"],
                r["over_number"], r["ball_number"], r["legal_ball_number"],
                r["batter"], r["bowler"], r["non_striker"],
                r["runs_batter"], r["runs_extras"], r["runs_total"],
                r["extras_wides"], r["extras_noballs"], r["extras_byes"],
                r["extras_legbyes"], r["extras_penalty"],
                r["is_wicket"], r["wicket_kind"], r["wicket_player_out"],
                r["wicket_fielders"], r["wickets_raw"],
                r["review_by"], r["review_umpire"], r["review_batter"],
                r["review_decision"], r["review_type"],
                r["replacement_in"], r["replacement_out"],
                r["replacement_team"], r["replacement_reason"],
                r["phase"],
                r["innings_score_at_ball"], r["wickets_fallen_at_ball"],
                r["legal_balls_bowled"], r["balls_remaining"], r["required_run_rate"],
            ))

        execute_values(cur, sql, values, page_size=500)
        return len(values)

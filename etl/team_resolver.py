"""
etl/team_resolver.py

Resolves any team name variant to the canonical name stored in the DB.

Usage:
    from etl.team_resolver import resolve_team

    resolve_team("Delhi Daredevils")   # → "Delhi Capitals"
    resolve_team("Kings XI Punjab")    # → "Punjab Kings"
    resolve_team("RCB")                # → "Royal Challengers Bengaluru"
    resolve_team("Mumbai Indians")     # → "Mumbai Indians"
    resolve_team("Unknown FC")         # → "Unknown FC"  (passthrough with warning)

The mapping is the single source of truth — update here when new aliases appear.
The teams table in Supabase mirrors this and is kept in sync by backfill_teams.py.
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

# canonical_name → set of all known aliases / short names
_CANONICAL: dict[str, str] = {}

_TEAMS: list[dict] = [
    {
        "canonical": "Chennai Super Kings",
        "short": "CSK",
        "aliases": ["Chennai Super Kings", "CSK"],
    },
    {
        "canonical": "Mumbai Indians",
        "short": "MI",
        "aliases": ["Mumbai Indians", "MI"],
    },
    {
        "canonical": "Royal Challengers Bengaluru",
        "short": "RCB",
        "aliases": ["Royal Challengers Bangalore", "Royal Challengers Bengaluru", "RCB"],
    },
    {
        "canonical": "Kolkata Knight Riders",
        "short": "KKR",
        "aliases": ["Kolkata Knight Riders", "KKR"],
    },
    {
        "canonical": "Sunrisers Hyderabad",
        "short": "SRH",
        "aliases": ["Sunrisers Hyderabad", "SRH"],
    },
    {
        "canonical": "Rajasthan Royals",
        "short": "RR",
        "aliases": ["Rajasthan Royals", "RR"],
    },
    {
        "canonical": "Delhi Capitals",
        "short": "DC",
        "aliases": ["Delhi Daredevils", "Delhi Capitals", "DD", "DC"],
    },
    {
        "canonical": "Punjab Kings",
        "short": "PBKS",
        "aliases": ["Kings XI Punjab", "Punjab Kings", "KXIP", "PBKS"],
    },
    {
        "canonical": "Lucknow Super Giants",
        "short": "LSG",
        "aliases": ["Lucknow Super Giants", "LSG"],
    },
    {
        "canonical": "Gujarat Titans",
        "short": "GT",
        "aliases": ["Gujarat Titans", "GT"],
    },
    # ── Defunct franchises ── kept as-is, not merged into any current team
    {
        "canonical": "Gujarat Lions",
        "short": "GL",
        "aliases": ["Gujarat Lions", "GL"],
    },
    {
        "canonical": "Deccan Chargers",
        "short": "DC2",
        "aliases": ["Deccan Chargers"],
    },
    {
        "canonical": "Pune Warriors",
        "short": "PW",
        "aliases": ["Pune Warriors", "Pune Warriors India"],
    },
    {
        "canonical": "Kochi Tuskers Kerala",
        "short": "KTK",
        "aliases": ["Kochi Tuskers Kerala"],
    },
    {
        "canonical": "Rising Pune Supergiant",
        "short": "RPS",
        "aliases": ["Rising Pune Supergiant", "Rising Pune Supergiants"],
    },
]

# Build flat alias → canonical lookup at module load time
for _team in _TEAMS:
    for _alias in _team["aliases"]:
        _CANONICAL[_alias.lower()] = _team["canonical"]
    _CANONICAL[_team["short"].lower()] = _team["canonical"]
    _CANONICAL[_team["canonical"].lower()] = _team["canonical"]


def resolve_team(name: str) -> str:
    """
    Return the canonical team name for any input variant.
    Passes through unknown names unchanged (with a warning).
    """
    if not name:
        return name
    resolved = _CANONICAL.get(name.lower().strip())
    if resolved is None:
        logger.warning(f"Unknown team name: '{name}' — passing through unchanged")
        return name
    return resolved


def all_aliases(canonical: str) -> list[str]:
    """Return all known aliases for a canonical team name."""
    for team in _TEAMS:
        if team["canonical"] == canonical:
            return team["aliases"]
    return [canonical]

# IPL ETL Pipeline — Claude Context

## Project Overview
Custom ETL pipeline that transforms Cricsheet IPL JSON match files into a structured Postgres (Supabase) warehouse. Designed to support a downstream NL-to-SQL analytics dashboard (Streamlit) and REST API (FastAPI, planned).

## Stack
- **Language**: Python 3.11+
- **Database**: Supabase (hosted Postgres) — `db.qjvauxaoentvtefvbknp.supabase.co`
- **Orchestration**: GitHub Actions (manual trigger only)
- **Repo**: https://github.com/VikAnalytics/ipl-etl

## Project Structure
```
ipl-etl/
├── etl/
│   ├── parser.py           # Cricsheet JSON → normalized Python dicts
│   ├── computed.py         # Derived columns (phase, running score, RRR, etc.)
│   ├── loader.py           # Idempotent upserts into Supabase via psycopg2
│   ├── team_resolver.py    # Canonical team name resolution (aliases + old names)
│   └── utils.py            # Shared helpers: log_run, fetch_done_matches
├── scraper/
│   ├── people.py           # Cricsheet people.csv → cricinfo_id for all players
│   ├── player_profiles.py  # Wikidata SPARQL → full_name + nationality + DOB
│   └── iplt20.py           # iplt20.com squad/auction scraper (deferred, not validated)
├── schema/
│   └── schema.sql          # Full Postgres DDL — safe to re-run (IF NOT EXISTS)
├── scripts/
│   ├── historical_load.py  # One-time batch load of all Cricsheet historical files
│   ├── daily_update.py     # In-season update: downloads Cricsheet ZIP, loads new matches
│   ├── backfill_teams.py   # Normalize historical team names in DB to canonical
│   └── enrich_players.py   # Orchestrates player enrichment (steps 1-3)
├── docs/
│   └── technical.md        # Deep-dive technical reference (architecture, decisions, trade-offs)
├── .github/workflows/
│   └── daily_update.yml    # Manual-trigger GitHub Actions workflow
├── ipl_json/               # Raw Cricsheet JSON files (gitignored, ~1170 files)
├── .env.example
└── requirements.txt
```

## Database Schema
| Table | Rows | Description |
|---|---|---|
| `teams` | 14 | Canonical team names, short codes, aliases array |
| `matches` | 1,169 | One row per match — all metadata |
| `innings` | 2,365 | One row per innings (including super overs) |
| `deliveries` | 278,205 | One row per ball — core fact table |
| `players` | 925 | Player registry + nationality + DOB |
| `match_players` | 26,137 | Playing XI per match per team |
| `officials` | 5,817 | Umpires and referees per match |
| `powerplays` | — | Powerplay segments per innings |
| `player_season` | 0 | Auction price, overseas flag (deferred) |
| `etl_run_log` | — | ETL run history for idempotency |

## Key Design Decisions
- **match_id**: Cricsheet file stem (e.g. `1082591`) — PK across all tables
- **delivery_id**: `{match_id}_{innings}_{over}_{ball}` — stable composite key
- **Upserts everywhere**: `INSERT ... ON CONFLICT DO UPDATE` — fully idempotent, safe to re-run
- **over_number**: 1-indexed (1–20); Cricsheet source is 0-indexed, parser adds +1
- **phase**: `powerplay` = overs 1–6, `middle` = 7–15, `death` = 16–20
- **season**: VARCHAR(10) — Cricsheet uses `"2020/21"` for the UAE season
- **Team names**: always canonical; `team_resolver.py` normalizes at parse time. Old names (e.g. "Delhi Daredevils") resolve via `teams.aliases` GIN index at query time.
- **wicket_fielders**: JSONB array (supports multiple fielders per dismissal)
- **Super overs**: `innings_number` 3+ with `is_super_over = true`

## Team Name Aliases (key mappings)
| Old / Alternate | Canonical |
|---|---|
| Delhi Daredevils, DD | Delhi Capitals |
| Kings XI Punjab, KXIP | Punjab Kings |
| Royal Challengers Bangalore, RCB | Royal Challengers Bengaluru |
| Rising Pune Supergiants | Rising Pune Supergiant |
| Deccan Chargers | Kept as-is (different franchise from SRH) |

## Player Enrichment
878/925 players enriched with nationality + DOB via Wikidata. Batting/bowling style not available from any free source without scraping.

```bash
python scripts/enrich_players.py --step 1   # Cricsheet people.csv → cricinfo_id
python scripts/enrich_players.py --step 2   # Wikidata → nationality + DOB
python scripts/enrich_players.py --step 3   # iplt20.com → auction data (not validated)
```
Step 1 must run before Step 2.

## Setup (fresh environment)
```bash
pip install -r requirements.txt
cp .env.example .env          # fill in DATABASE_URL
psql $DATABASE_URL -f schema/schema.sql
python scripts/historical_load.py --skip-done
python scripts/backfill_teams.py
python scripts/enrich_players.py --step 1
python scripts/enrich_players.py --step 2
```

## In-Season Updates (April IPL)
Source: Cricsheet IPL ZIP (https://cricsheet.org/downloads/ipl_json.zip)
Cricsheet publishes match files 12–24h after each match.

**Runbook per match day:**
1. Check cricsheet.org/matches/ipl — confirm new match is published
2. GitHub → Actions → "IPL Match Update" → Run workflow
3. Optionally tick dry_run first to preview, then run for real

ESPNcricinfo and Cricbuzz both block automated requests (Akamai WAF) — not viable for scraping.

## Environment Variables
`DATABASE_URL` — Supabase Postgres URI (port 5432, session mode).
Set locally in `.env`, set in GitHub repo Settings → Secrets → Actions for CI.

## What's Next
- FastAPI REST layer on top of Supabase
- Streamlit NL-to-SQL dashboard (user has existing flat-file version to migrate)
- `player_season` enrichment via iplt20.com (deferred post-April)

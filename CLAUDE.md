# IPL ETL Pipeline

## Project Overview
Custom ETL pipeline that transforms Cricsheet IPL JSON match files into a structured Postgres (Supabase) warehouse. Designed to support downstream API and NL-to-SQL analytics dashboard.

## Stack
- **Language**: Python 3.11+
- **Database**: Supabase (hosted Postgres) — project at `db.qjvauxaoentvtefvbknp.supabase.co`
- **Orchestration**: GitHub Actions (daily cron at 18:30 UTC = midnight IST, April only)
- **Repo**: https://github.com/VikAnalytics/ipl-etl
- **Future**: FastAPI for REST API, Streamlit for NL-to-SQL dashboard

## Project Structure
```
ipl-etl/
├── etl/
│   ├── parser.py         # Parse Cricsheet JSON → normalized Python dicts
│   ├── computed.py       # Add derived columns (phase, running score, etc.)
│   ├── loader.py         # Upsert records into Supabase via psycopg2
│   └── team_resolver.py  # Canonical team name resolution (handles aliases + old names)
├── scraper/
│   ├── cricinfo.py       # ESPNcricinfo scraper for new April match files
│   ├── people.py         # Cricsheet people.csv → cricinfo_id mapping
│   ├── player_profiles.py # ESPNcricinfo player profiles (nationality, role, style)
│   └── iplt20.py         # iplt20.com scraper for auction/squad data
├── schema/
│   └── schema.sql        # Full Postgres DDL — run once to create all tables
├── scripts/
│   ├── historical_load.py    # One-time load of all Cricsheet historical files
│   ├── daily_update.py       # Entry point for GitHub Actions daily run
│   ├── backfill_teams.py     # Normalize historical team names in DB to canonical
│   └── enrich_players.py     # Orchestrate player enrichment (people.csv + profiles + auction)
├── .github/workflows/
│   └── daily_update.yml      # Cron workflow: scrape + parse + load
├── ipl_json/                 # Raw Cricsheet JSON files (gitignored)
├── .env.example
└── requirements.txt
```

## Database Schema (tables)
- `teams` — canonical team names with short names and aliases array (for query resolution)
- `matches` — one row per match, all match-level metadata
- `innings` — one row per innings (including super overs)
- `deliveries` — one row per ball; includes computed columns (phase, running score, etc.)
- `players` — player registry keyed on Cricsheet person ID; enriched with cricinfo_id, nationality, role
- `match_players` — squad per match (who played for which team)
- `officials` — match officials per match (umpires, referees)
- `powerplays` — powerplay segments per innings
- `player_season` — auction price, retention status, overseas flag per player per season
- `etl_run_log` — tracks every ETL run for idempotency and debugging

## Key Design Decisions
- **match_id**: Cricsheet file name without extension (e.g. `1082591`) — PK across all tables
- **delivery_id**: `{match_id}_{innings_number}_{over}_{ball}` — stable, human-readable composite
- **Upserts everywhere**: All loads use `INSERT ... ON CONFLICT DO UPDATE` — safe to re-run
- **over_number**: 0-indexed as in Cricsheet (over 0 = first over)
- **phase**: `powerplay` = overs 0–5, `middle` = 6–14, `death` = 15–19
- **wicket_fielders**: Stored as JSONB array (can have multiple fielders on a dismissal)
- **Super overs**: innings_number 3+ with `is_super_over = true`
- **Impact player replacements**: stored on the delivery where the replacement was recorded
- **season**: stored as VARCHAR(10) — Cricsheet uses `"2020/21"` for the UAE season
- **Team names**: always stored as canonical names (e.g. "Delhi Capitals", not "Delhi Daredevils"). team_resolver.py handles all variants. Old names resolve correctly via the `teams.aliases` array.

## Team Name Aliases (key mappings)
| Old / Alternate | Canonical |
|---|---|
| Delhi Daredevils, DD | Delhi Capitals |
| Kings XI Punjab, KXIP | Punjab Kings |
| Royal Challengers Bangalore, RCB | Royal Challengers Bengaluru |
| Rising Pune Supergiants | Rising Pune Supergiant |
| Deccan Chargers | Deccan Chargers (kept separate — different franchise from SRH) |

## Player Enrichment Pipeline
Run after historical load to fill in profile data:
```bash
python scripts/enrich_players.py          # all steps
python scripts/enrich_players.py --step 1 # people.csv → cricinfo_id
python scripts/enrich_players.py --step 2 # ESPNcricinfo profiles
python scripts/enrich_players.py --step 3 # iplt20 auction data
```
Step 1 must run before Step 2. Steps 2 and 3 are independent of each other.

## Environment Variables
See `.env.example`. Must set `DATABASE_URL` (Supabase direct connection string, port 5432).

## Running Locally
```bash
pip install -r requirements.txt
cp .env.example .env  # fill in DATABASE_URL
python scripts/historical_load.py --skip-done   # load all historical matches
python scripts/backfill_teams.py                # normalize team names in DB
python scripts/enrich_players.py                # enrich player profiles
```

## In-Season Updates (April IPL)
- **Source**: Cricsheet IPL ZIP (`https://cricsheet.org/downloads/ipl_json.zip`)
- **Workflow**: Manual trigger only — `.github/workflows/daily_update.yml`
- After each match day, once Cricsheet publishes (usually 12-24h after match):
  1. Go to GitHub → Actions → "IPL Match Update" → "Run workflow"
  2. Leave dry_run = false, click Run
  3. Script downloads ZIP, finds new match IDs not in etl_run_log, loads them
- **Dry run option**: set dry_run = true to preview new matches without loading
- ESPNcricinfo and Cricbuzz both block automated requests (Akamai/WAF) — not viable

## Notes
- Raw JSON files are gitignored (~1170 files, too large for git)
- `ipl_json/` is the expected local path for source files
- `scraper/iplt20.py` is for auction/player_season enrichment — not yet validated, deferred post-April
- `DATABASE_URL` secret must be set in GitHub repo Settings → Secrets → Actions

# IPL ETL Pipeline

## Project Overview
Custom ETL pipeline that transforms Cricsheet IPL JSON match files into a structured Postgres (Supabase) warehouse. Designed to support downstream API and NL-to-SQL analytics dashboard.

## Stack
- **Language**: Python 3.11+
- **Database**: Supabase (hosted Postgres)
- **Orchestration**: GitHub Actions (daily cron, runs after IPL match days in April)
- **Future**: FastAPI for REST API, Streamlit for NL-to-SQL dashboard

## Project Structure
```
ipl-etl/
├── etl/
│   ├── parser.py       # Parse Cricsheet JSON → normalized Python dicts
│   ├── computed.py     # Add derived columns (phase, running score, etc.)
│   └── loader.py       # Upsert records into Supabase via psycopg2
├── scraper/
│   ├── cricinfo.py     # ESPNcricinfo scraper for new April match files
│   └── iplt20.py       # iplt20.com scraper for auction/squad data
├── schema/
│   └── schema.sql      # Full Postgres DDL — run once to create all tables
├── scripts/
│   ├── historical_load.py   # One-time load of all Cricsheet historical files
│   └── daily_update.py      # Entry point for GitHub Actions daily run
├── .github/workflows/
│   └── daily_update.yml     # Cron workflow: scrape + parse + load
├── ipl_json/               # Raw Cricsheet JSON files (gitignored)
├── .env.example
└── requirements.txt
```

## Database Schema (tables)
- `matches` — one row per match, all match-level metadata
- `innings` — one row per innings (including super overs)
- `deliveries` — one row per ball; includes computed columns (phase, running score, etc.)
- `players` — player registry keyed on Cricsheet person ID
- `match_players` — squad per match (who played for which team)
- `officials` — match officials per match (umpires, referees)
- `powerplays` — powerplay segments per innings
- `player_season` — auction price, retention status, overseas flag per player per season

## Key Design Decisions
- **match_id**: Cricsheet file name without extension (e.g. `1082591`) — used as PK across all tables
- **delivery_id**: `{match_id}_{innings_number}_{over}_{ball}` — stable, human-readable composite
- **Upserts everywhere**: All loads use `INSERT ... ON CONFLICT DO UPDATE` — safe to re-run
- **over_number**: 0-indexed as in Cricsheet (over 0 = first over)
- **phase**: `powerplay` = overs 0–5, `middle` = 6–14, `death` = 15–19
- **wicket_fielders**: Stored as JSONB array (can have multiple fielders on a dismissal)
- **Super overs**: innings_number 3+ with `is_super_over = true`
- **Impact player replacements**: stored on the delivery where the replacement was recorded

## Environment Variables
See `.env.example`. Must set `DATABASE_URL` (Supabase direct connection string).

## Running Locally
```bash
pip install -r requirements.txt
cp .env.example .env  # fill in DATABASE_URL
python scripts/historical_load.py  # load all historical matches
```

## GitHub Actions
Workflow runs daily at 18:30 UTC (midnight IST) during April. It:
1. Scrapes ESPNcricinfo for any new IPL match files from that day
2. Converts to Cricsheet-compatible format
3. Runs the ETL pipeline to upsert into Supabase

## Notes
- Raw JSON files are gitignored (too large for repo, ~1170 files)
- `ipl_json/` is the expected local path for source files
- Scraper is gray-area (ESPNcricinfo ToS) — do not abuse request rates; includes polite delays

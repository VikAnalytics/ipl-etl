# IPL ETL Pipeline

A custom ETL pipeline that loads Cricsheet IPL match data into a structured Postgres warehouse (Supabase), enriched with player profiles and canonical team names. Built to power a NL-to-SQL analytics dashboard and REST API.

## What's in the database

| Table | Rows | Description |
|---|---|---|
| `matches` | 1,169 | All IPL matches (2008–2025) |
| `innings` | 2,365 | Per-innings totals + target |
| `deliveries` | 278,205 | Ball-by-ball with phase, running score, RRR |
| `players` | 925 | Registry with nationality + DOB |
| `match_players` | 26,137 | Playing XI per match |
| `teams` | 14 | Canonical names + all historical aliases |
| `officials` | 5,817 | Umpires and referees |

## Stack

- **Python** — ETL, enrichment scripts
- **Supabase** — hosted Postgres
- **GitHub Actions** — manual-trigger workflow for in-season updates
- **Wikidata** — player nationality + DOB
- **Cricsheet** — source of all match data

## Setup

**1. Prerequisites**
```bash
pip install -r requirements.txt
cp .env.example .env   # add your Supabase DATABASE_URL
```

**2. Create schema**

Paste `schema/schema.sql` into the Supabase SQL editor, or run:
```bash
psql $DATABASE_URL -f schema/schema.sql
```

**3. Load historical data**
```bash
python scripts/historical_load.py --skip-done
python scripts/backfill_teams.py
```

**4. Enrich players**
```bash
python scripts/enrich_players.py --step 1   # Cricsheet people.csv → cricinfo_id
python scripts/enrich_players.py --step 2   # Wikidata → nationality + DOB
```

## In-season updates (April IPL)

After each match day, once Cricsheet publishes the match file (usually 12–24h after the match):

1. Go to **GitHub → Actions → "IPL Match Update" → Run workflow**
2. Optionally enable **dry_run** first to preview new matches
3. Click **Run** — done in ~2 minutes

The script downloads the Cricsheet IPL ZIP, finds match IDs not yet in the database, and loads them through the same pipeline as the historical data.

## Project structure

```
etl/            Core pipeline (parser, computed columns, loader, team resolver)
scraper/        Player enrichment (Cricsheet people.csv, Wikidata profiles)
schema/         Postgres DDL
scripts/        Runnable entry points
.github/        GitHub Actions workflow
```

## Key design decisions

- **Canonical team names** — historical names like "Delhi Daredevils" and "Kings XI Punjab" are normalized to current names on ingest. Old names still work in queries via the `teams.aliases` column.
- **Idempotent loads** — every insert uses `ON CONFLICT DO UPDATE`. Safe to re-run any script.
- **Computed delivery columns** — `phase`, `innings_score_at_ball`, `wickets_fallen_at_ball`, `balls_remaining`, `required_run_rate` are pre-computed and stored, making NL-to-SQL queries simpler.
- **Season as VARCHAR** — Cricsheet uses `"2020/21"` for the UAE season; storing as text avoids type errors.

## Environment variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | Supabase Postgres URI (port 5432, session mode) |

Set locally in `.env` (gitignored). Set as a secret in GitHub repo settings for Actions.

# IPL ETL Pipeline

A custom ETL pipeline that loads Cricsheet IPL match data into a structured Postgres warehouse (Supabase), enriched with player profiles and canonical team/venue names. Built to power a NL-to-SQL analytics dashboard and REST API.

## What's in the database

| Table | Rows | Description |
|---|---|---|
| `matches` | 1,191 | All IPL matches (2008–2026) |
| `innings` | 2,408 | Per-innings totals + target |
| `deliveries` | 283,229 | Ball-by-ball with phase, running score, RRR |
| `players` | 945 | Registry with nationality + DOB |
| `match_players` | 26,662 | Playing XI per match |
| `teams` | 15 | Canonical names + all historical aliases |
| `officials` | 5,927 | Umpires and referees |

## Stack

- **Python** — ETL, enrichment scripts
- **Supabase** — hosted Postgres
- **GitHub Actions** — automated in-season updates (triggered via external cron)
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
python scripts/backfill_venues.py
```

**4. Enrich players**
```bash
python scripts/enrich_players.py --step 1   # Cricsheet people.csv → cricinfo_id
python scripts/enrich_players.py --step 2   # Wikidata → nationality + DOB
```

## In-season updates (April–May IPL)

The GitHub Actions workflow runs automatically via an external cron service. It fires the workflow twice daily (06:00 and 14:00 UTC), downloads the Cricsheet IPL ZIP, finds any match IDs not yet in the database, and loads them through the same pipeline as the historical data.

Cricsheet typically publishes match files 12–24h after each match ends.

**To trigger manually:**
1. Go to **GitHub → Actions → "IPL Match Update" → Run workflow**
2. Optionally enable **dry_run** first to preview new matches
3. Click **Run** — done in ~2 minutes

**GitHub Actions secret required:**
`DATABASE_URL` must be set to the **Supabase session pooler URL** (not the direct connection URL) to avoid IPv6 connectivity issues from GitHub's runners. Get it from: Supabase Dashboard → Settings → Database → Session pooler.

## Project structure

```
etl/            Core pipeline (parser, computed columns, loader, resolvers)
scraper/        Player enrichment (Cricsheet people.csv, Wikidata profiles)
schema/         Postgres DDL
scripts/        Runnable entry points
.github/        GitHub Actions workflow
```

## Key design decisions

- **Canonical team names** — historical names like "Delhi Daredevils" and "Kings XI Punjab" are normalized to current names on ingest. Old names still work in queries via the `teams.aliases` column.
- **Canonical venue names** — 60+ Cricsheet venue variants (city suffixes, renamed stadiums) normalize to 36 canonical names. Renames handled: Feroz Shah Kotla → Arun Jaitley Stadium, Sardar Patel/Motera → Narendra Modi Stadium, Subrata Roy Sahara → Maharashtra CA Stadium, Sheikh Zayed → Zayed Cricket Stadium.
- **Canonical city names** — `Bangalore` normalized to `Bengaluru`; `Mohali` normalized to `New Chandigarh` for the Maharaja Yadavindra Singh stadium (Cricsheet labeling inconsistency).
- **Idempotent loads** — every insert uses `ON CONFLICT DO UPDATE`. Safe to re-run any script.
- **Computed delivery columns** — `phase`, `innings_score_at_ball`, `wickets_fallen_at_ball`, `balls_remaining`, `required_run_rate` are pre-computed and stored, making NL-to-SQL queries simpler.
- **Season as VARCHAR** — Cricsheet uses `"2020/21"` for the UAE season; storing as text avoids type errors.

## Environment variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | Supabase Postgres URI (session pooler, port 5432) |

Set locally in `.env` (gitignored). For GitHub Actions, use the **session pooler URL** from Supabase dashboard to avoid IPv6 issues.

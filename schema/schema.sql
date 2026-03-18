-- IPL ETL Database Schema
-- Run once against your Supabase project to create all tables.
-- Safe to re-run: uses CREATE TABLE IF NOT EXISTS.

-- ============================================================
-- TEAMS
-- Canonical team names with aliases for query resolution.
-- team1/team2 in matches and team in innings always use canonical_name.
-- Queries using old names (e.g. "Delhi Daredevils") resolve via aliases.
-- ============================================================
CREATE TABLE IF NOT EXISTS teams (
    team_id         SERIAL       PRIMARY KEY,
    canonical_name  VARCHAR(100) NOT NULL UNIQUE,  -- stored everywhere in DB
    short_name      VARCHAR(10),                   -- RCB, CSK, MI etc.
    aliases         TEXT[]                         -- all historical / alternate names
);

-- Seed data: current franchises + all known historical names as aliases
INSERT INTO teams (canonical_name, short_name, aliases) VALUES
    ('Chennai Super Kings',         'CSK',  ARRAY['Chennai Super Kings']),
    ('Mumbai Indians',              'MI',   ARRAY['Mumbai Indians']),
    ('Royal Challengers Bengaluru', 'RCB',  ARRAY['Royal Challengers Bangalore', 'Royal Challengers Bengaluru']),
    ('Kolkata Knight Riders',       'KKR',  ARRAY['Kolkata Knight Riders']),
    ('Sunrisers Hyderabad',         'SRH',  ARRAY['Sunrisers Hyderabad']),
    ('Rajasthan Royals',            'RR',   ARRAY['Rajasthan Royals']),
    ('Delhi Capitals',              'DC',   ARRAY['Delhi Daredevils', 'Delhi Capitals']),
    ('Punjab Kings',                'PBKS', ARRAY['Kings XI Punjab', 'Punjab Kings']),
    ('Lucknow Super Giants',        'LSG',  ARRAY['Lucknow Super Giants']),
    ('Gujarat Titans',              'GT',   ARRAY['Gujarat Titans']),
    -- Defunct franchises kept as-is (different ownership, not rebrands)
    ('Deccan Chargers',             'DC2',  ARRAY['Deccan Chargers']),
    ('Pune Warriors',               'PW',   ARRAY['Pune Warriors']),
    ('Kochi Tuskers Kerala',        'KTK',  ARRAY['Kochi Tuskers Kerala']),
    ('Rising Pune Supergiant',      'RPS',  ARRAY['Rising Pune Supergiant', 'Rising Pune Supergiants'])
ON CONFLICT (canonical_name) DO UPDATE SET
    short_name = EXCLUDED.short_name,
    aliases    = EXCLUDED.aliases;

CREATE INDEX IF NOT EXISTS idx_teams_canonical ON teams (canonical_name);
CREATE INDEX IF NOT EXISTS idx_teams_aliases   ON teams USING GIN (aliases);

-- ============================================================
-- MATCHES
-- One row per match (keyed on Cricsheet file ID)
-- ============================================================
CREATE TABLE IF NOT EXISTS matches (
    match_id            VARCHAR(20)  PRIMARY KEY,  -- Cricsheet file name without .json

    -- Meta
    data_version        VARCHAR(10),
    created_date        DATE,
    revision            INT,

    -- Event
    season              VARCHAR(10)  NOT NULL,  -- e.g. 2017 or "2020/21"
    match_number        INT,
    event_name          VARCHAR(100),

    -- Match info
    match_type          VARCHAR(20),               -- T20
    gender              VARCHAR(10),
    team_type           VARCHAR(20),               -- club
    balls_per_over      INT          DEFAULT 6,
    overs               INT          DEFAULT 20,

    -- Location / Date
    venue               VARCHAR(150),
    city                VARCHAR(100),
    match_date          DATE         NOT NULL,     -- first date (IPL always 1-day)

    -- Teams
    team1               VARCHAR(100) NOT NULL,
    team2               VARCHAR(100) NOT NULL,

    -- Toss
    toss_winner         VARCHAR(100),
    toss_decision       VARCHAR(10),               -- bat / field

    -- Outcome
    outcome_winner      VARCHAR(100),              -- NULL if no result / tie
    outcome_by_runs     INT,                       -- winning margin in runs
    outcome_by_wickets  INT,                       -- winning margin in wickets
    outcome_method      VARCHAR(50),               -- D/L etc.
    outcome_result      VARCHAR(50),               -- 'tie' / 'no result'
    outcome_eliminator  VARCHAR(100),              -- team that won eliminator

    -- Player of match (array — rare to have >1)
    player_of_match     TEXT[],

    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_matches_season   ON matches (season);
CREATE INDEX IF NOT EXISTS idx_matches_date     ON matches (match_date);
CREATE INDEX IF NOT EXISTS idx_matches_team1    ON matches (team1);
CREATE INDEX IF NOT EXISTS idx_matches_team2    ON matches (team2);


-- ============================================================
-- INNINGS
-- One row per innings (2 per match, +1 per super over)
-- ============================================================
CREATE TABLE IF NOT EXISTS innings (
    innings_id          SERIAL       PRIMARY KEY,
    match_id            VARCHAR(20)  NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    innings_number      INT          NOT NULL,     -- 1, 2; 3+ for super overs
    team                VARCHAR(100) NOT NULL,
    is_super_over       BOOLEAN      DEFAULT FALSE,

    -- 2nd innings target (set by loader after parsing)
    target_runs         INT,
    target_overs        INT,

    -- Players who were absent hurt this innings
    absent_hurt         TEXT[],

    -- Aggregate totals (computed and stored for query convenience)
    total_runs          INT,
    total_wickets       INT,
    total_overs_faced   NUMERIC(5,1), -- e.g. 18.3

    UNIQUE (match_id, innings_number)
);

CREATE INDEX IF NOT EXISTS idx_innings_match    ON innings (match_id);


-- ============================================================
-- POWERPLAYS
-- Powerplay segments per innings
-- ============================================================
CREATE TABLE IF NOT EXISTS powerplays (
    powerplay_id        SERIAL       PRIMARY KEY,
    match_id            VARCHAR(20)  NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    innings_number      INT          NOT NULL,
    pp_type             VARCHAR(20),               -- mandatory, batting, fielding
    from_over           INT          NOT NULL,     -- 0-indexed (inclusive)
    to_over             INT          NOT NULL,     -- 0-indexed (inclusive)

    UNIQUE (match_id, innings_number, pp_type)
);


-- ============================================================
-- DELIVERIES
-- One row per ball (the core fact table)
-- ============================================================
CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id         VARCHAR(40)  PRIMARY KEY,  -- {match_id}_{inn}_{over}_{ball}
    match_id            VARCHAR(20)  NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    innings_id          INT          NOT NULL REFERENCES innings(innings_id) ON DELETE CASCADE,
    innings_number      INT          NOT NULL,
    is_super_over       BOOLEAN      DEFAULT FALSE,

    -- Ball position
    over_number         INT          NOT NULL,     -- 0-indexed
    ball_number         INT          NOT NULL,     -- position within over deliveries array (1-indexed)
    legal_ball_number   INT,                       -- counts only legal deliveries (no wide/noball)

    -- Players
    batter              VARCHAR(100) NOT NULL,
    bowler              VARCHAR(100) NOT NULL,
    non_striker         VARCHAR(100) NOT NULL,

    -- Runs
    runs_batter         INT          NOT NULL DEFAULT 0,
    runs_extras         INT          NOT NULL DEFAULT 0,
    runs_total          INT          NOT NULL DEFAULT 0,

    -- Extras breakdown
    extras_wides        INT          DEFAULT 0,
    extras_noballs      INT          DEFAULT 0,
    extras_byes         INT          DEFAULT 0,
    extras_legbyes      INT          DEFAULT 0,
    extras_penalty      INT          DEFAULT 0,

    -- Wicket (primary — flattened for easy querying)
    is_wicket           BOOLEAN      DEFAULT FALSE,
    wicket_kind         VARCHAR(50),               -- bowled, caught, run out, etc.
    wicket_player_out   VARCHAR(100),
    wicket_fielders     JSONB,                     -- [{name, substitute}]

    -- Full wickets array (for rare multi-wicket deliveries)
    wickets_raw         JSONB,

    -- DRS Review
    review_by           VARCHAR(100),
    review_umpire       VARCHAR(100),
    review_batter       VARCHAR(100),
    review_decision     VARCHAR(20),               -- upheld / struck down
    review_type         VARCHAR(20),               -- wicket / no ball

    -- Impact player replacement (if recorded on this ball)
    replacement_in      VARCHAR(100),
    replacement_out     VARCHAR(100),
    replacement_team    VARCHAR(100),
    replacement_reason  VARCHAR(50),               -- impact_player / injury etc.

    -- --------------------------------------------------------
    -- Computed / derived columns
    -- --------------------------------------------------------
    phase               VARCHAR(15)  NOT NULL,     -- powerplay / middle / death
    innings_score_at_ball   INT,                   -- cumulative runs AFTER this ball
    wickets_fallen_at_ball  INT,                   -- cumulative wickets AFTER this ball
    legal_balls_bowled      INT,                   -- legal balls bowled in innings up to and including this ball
    balls_remaining         INT,                   -- legal balls left in innings after this ball
    required_run_rate       NUMERIC(6,2)           -- only meaningful for 2nd innings / super overs
);

CREATE INDEX IF NOT EXISTS idx_deliveries_match      ON deliveries (match_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_innings    ON deliveries (innings_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_batter     ON deliveries (batter);
CREATE INDEX IF NOT EXISTS idx_deliveries_bowler     ON deliveries (bowler);
CREATE INDEX IF NOT EXISTS idx_deliveries_wicket     ON deliveries (is_wicket) WHERE is_wicket = TRUE;
CREATE INDEX IF NOT EXISTS idx_deliveries_season     ON deliveries (match_id, innings_number, over_number);


-- ============================================================
-- PLAYERS
-- Player registry (Cricsheet person ID as PK)
-- ============================================================
CREATE TABLE IF NOT EXISTS players (
    player_key          VARCHAR(20)  PRIMARY KEY,  -- Cricsheet 8-char hex ID
    player_name         VARCHAR(100) NOT NULL,

    -- Scraped from ESPNcricinfo (nullable until enriched)
    full_name           VARCHAR(150),
    nationality         VARCHAR(60),
    batting_style       VARCHAR(50),               -- Right hand / Left hand
    bowling_style       VARCHAR(80),               -- Right arm fast / Slow left arm etc.
    playing_role        VARCHAR(50),               -- Batter / Bowler / All-rounder / WK-Batter
    date_of_birth       DATE,
    cricinfo_id         INT,                       -- ESPNcricinfo numeric player ID

    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_players_name         ON players (player_name);
CREATE INDEX IF NOT EXISTS idx_players_nationality  ON players (nationality);


-- ============================================================
-- MATCH_PLAYERS
-- Squad per match (11 playing XI per team = 22 rows per match)
-- ============================================================
CREATE TABLE IF NOT EXISTS match_players (
    match_id            VARCHAR(20)  NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    team                VARCHAR(100) NOT NULL,
    player_key          VARCHAR(20)  NOT NULL REFERENCES players(player_key),
    player_name         VARCHAR(100) NOT NULL,

    PRIMARY KEY (match_id, player_key)
);

CREATE INDEX IF NOT EXISTS idx_match_players_player ON match_players (player_key);
CREATE INDEX IF NOT EXISTS idx_match_players_match  ON match_players (match_id);
CREATE INDEX IF NOT EXISTS idx_match_players_team   ON match_players (match_id, team);


-- ============================================================
-- OFFICIALS
-- Umpires, referees per match
-- ============================================================
CREATE TABLE IF NOT EXISTS officials (
    official_id         SERIAL       PRIMARY KEY,
    match_id            VARCHAR(20)  NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    role                VARCHAR(30)  NOT NULL,     -- umpire / tv_umpire / reserve_umpire / match_referee
    name                VARCHAR(100) NOT NULL,

    UNIQUE (match_id, role, name)
);

CREATE INDEX IF NOT EXISTS idx_officials_match      ON officials (match_id);
CREATE INDEX IF NOT EXISTS idx_officials_name       ON officials (name);


-- ============================================================
-- PLAYER_SEASON
-- Per-player per-season IPL info (auction, team, overseas)
-- Populated by scraper — NULLs until enriched
-- ============================================================
CREATE TABLE IF NOT EXISTS player_season (
    player_key          VARCHAR(20)  NOT NULL REFERENCES players(player_key),
    season              INT          NOT NULL,
    team                VARCHAR(100),
    acquisition_type    VARCHAR(20),               -- auctioned / retained / rtm / traded
    auction_price_lakhs NUMERIC(10,2),             -- INR lakhs
    is_overseas         BOOLEAN,
    squad_role          VARCHAR(50),               -- playing XI / support staff

    PRIMARY KEY (player_key, season)
);

CREATE INDEX IF NOT EXISTS idx_player_season_season ON player_season (season);
CREATE INDEX IF NOT EXISTS idx_player_season_team   ON player_season (team, season);


-- ============================================================
-- ETL RUN LOG
-- Tracks which files have been processed (idempotency)
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_run_log (
    log_id              SERIAL       PRIMARY KEY,
    match_id            VARCHAR(20)  NOT NULL,
    source_file         VARCHAR(200),
    status              VARCHAR(20)  NOT NULL,     -- success / error
    error_message       TEXT,
    rows_inserted       INT,
    run_at              TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_log_match        ON etl_run_log (match_id);
CREATE INDEX IF NOT EXISTS idx_etl_log_status       ON etl_run_log (status);

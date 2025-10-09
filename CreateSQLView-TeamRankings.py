from sqlalchemy import create_engine, text

PG_USER = "SeanZahller"
PG_PASS = "YvMiTe9!2"
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "nfl_warehouse"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

sql = """
BEGIN;

CREATE SCHEMA IF NOT EXISTS mart;

-- Drop in dependency order so we can change columns safely
DROP VIEW IF EXISTS mart.v_team_weekly_ranks;
DROP VIEW IF EXISTS mart.v_team_weekly_base;

-- 1) Base weekly team totals (offense yards from player stats, points from schedules)
CREATE VIEW mart.v_team_weekly_base AS
WITH
off_yards AS (
  SELECT
    season::int,
    week::int,
    recent_team::text AS team,
    SUM(COALESCE(passing_yards,0))::numeric  AS off_pass_yards,
    SUM(COALESCE(rushing_yards,0))::numeric  AS off_rush_yards
  FROM hist_weekly
  WHERE season_type = 'REG'
  GROUP BY season, week, recent_team
),
def_yards_allowed AS (
  -- yards *allowed* by team T == sum of opponent's offensive yards when opponent_team = T
  SELECT
    season::int,
    week::int,
    opponent_team::text AS team,                      
    SUM(COALESCE(passing_yards,0))::numeric AS def_pass_yards_allowed,
    SUM(COALESCE(rushing_yards,0))::numeric AS def_rush_yards_allowed
  FROM hist_weekly
  WHERE season_type = 'REG'
  GROUP BY season, week, opponent_team
),
points AS (
  SELECT
    season::int,
    week::int,
    home_team::text AS team,
    home_score::numeric AS points_for,
    away_score::numeric AS points_allowed
  FROM hist_schedules
  WHERE game_type = 'REG'
  UNION ALL
  SELECT
    season::int,
    week::int,
    away_team::text AS team,
    away_score::numeric AS points_for,
    home_score::numeric AS points_allowed
  FROM hist_schedules
  WHERE game_type = 'REG'
)
SELECT
  p.season,
  p.week,
  p.team,

  -- Offense totals
  COALESCE(o.off_pass_yards,0)           AS off_pass_yards,
  COALESCE(o.off_rush_yards,0)           AS off_rush_yards,
  COALESCE(p.points_for,0)               AS off_points,

  -- Defense allowed totals
  COALESCE(d.def_pass_yards_allowed,0)   AS def_pass_yards_allowed,
  COALESCE(d.def_rush_yards_allowed,0)   AS def_rush_yards_allowed,
  COALESCE(p.points_allowed,0)           AS def_points_allowed
FROM points p
LEFT JOIN off_yards o
  ON o.season = p.season AND o.week = p.week AND o.team = p.team
LEFT JOIN def_yards_allowed d
  ON d.season = p.season AND d.week  = p.week  AND d.team = p.team
;

-- 2) Weekly ranks 1..32 (ties share rank, no gaps)
CREATE VIEW mart.v_team_weekly_ranks AS
SELECT
  season,
  week,
  team,

  off_pass_yards,
  off_rush_yards,
  off_points,
  def_pass_yards_allowed,
  def_rush_yards_allowed,
  def_points_allowed,

  DENSE_RANK() OVER (PARTITION BY season, week ORDER BY off_pass_yards        DESC) AS off_pass_rank,
  DENSE_RANK() OVER (PARTITION BY season, week ORDER BY off_rush_yards        DESC) AS off_rush_rank,
  DENSE_RANK() OVER (PARTITION BY season, week ORDER BY off_points             DESC) AS off_points_rank,

  DENSE_RANK() OVER (PARTITION BY season, week ORDER BY def_pass_yards_allowed ASC) AS def_pass_rank,   -- fewer allowed is better
  DENSE_RANK() OVER (PARTITION BY season, week ORDER BY def_rush_yards_allowed ASC) AS def_rush_rank,
  DENSE_RANK() OVER (PARTITION BY season, week ORDER BY def_points_allowed     ASC) AS def_points_rank
FROM mart.v_team_weekly_base;

COMMIT;
"""

with engine.begin() as con:
    con.execute(text(sql))

print("✅ Recreated: mart.v_team_weekly_base and mart.v_team_weekly_ranks")
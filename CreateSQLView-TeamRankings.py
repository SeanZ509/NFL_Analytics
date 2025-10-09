from sqlalchemy import create_engine, text

PG_USER = "SeanZahller"
PG_PASS = "YvMiTe9!2"
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "nfl_warehouse"

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}", pool_pre_ping=True)

SQL = """
CREATE SCHEMA IF NOT EXISTS mart;

-- 1) Team weekly stats (REG season by default)
CREATE OR REPLACE VIEW mart.v_team_weekly_stats AS
WITH scored_games AS (
  SELECT season, week, game_type, gameday, home_team, away_team, home_score, away_score
  FROM hist_schedules
  WHERE home_score IS NOT NULL AND away_score IS NOT NULL
    AND game_type = 'REG'             -- <-- change/remove if you want playoffs too
),
team_points AS (
  SELECT season, week, gameday,
         home_team AS team,
         home_score::int AS points_for,
         away_score::int AS points_against
  FROM scored_games
  UNION ALL
  SELECT season, week, gameday,
         away_team AS team,
         away_score::int AS points_for,
         home_score::int AS points_against
  FROM scored_games
),
offense AS (
  -- Offensive yards produced by this team in that week
  SELECT season, week, team,
         SUM(passing_yards)::numeric AS pass_yards_for,
         SUM(rushing_yards)::numeric AS rush_yards_for
  FROM mart.v_player_games
  WHERE game_type = 'REG'
  GROUP BY season, week, team
),
defense AS (
  -- Yards allowed by this team in that week (opponent’s yards vs them)
  SELECT season, week, opp_team AS team,
         SUM(passing_yards)::numeric AS pass_yards_allowed,
         SUM(rushing_yards)::numeric AS rush_yards_allowed
  FROM mart.v_player_games
  WHERE game_type = 'REG'
  GROUP BY season, week, opp_team
)
SELECT
  tp.season, tp.week, tp.team, tp.gameday,
  tp.points_for,
  tp.points_against,
  COALESCE(o.pass_yards_for,0)      AS pass_yards_for,
  COALESCE(o.rush_yards_for,0)      AS rush_yards_for,
  COALESCE(d.pass_yards_allowed,0)  AS pass_yards_allowed,
  COALESCE(d.rush_yards_allowed,0)  AS rush_yards_allowed
FROM team_points tp
LEFT JOIN offense o
  ON o.season = tp.season AND o.week = tp.week AND o.team = tp.team
LEFT JOIN defense d
  ON d.season = tp.season AND d.week = tp.week AND d.team = tp.team
;

-- 2) Weekly ranks (higher offense = better rank; lower allowed = better rank)
CREATE OR REPLACE VIEW mart.v_team_weekly_ranks AS
SELECT
  season, week, team, gameday,
  points_for,
  points_against,
  pass_yards_for,
  rush_yards_for,
  pass_yards_allowed,
  rush_yards_allowed,

  RANK() OVER (PARTITION BY season, week ORDER BY pass_yards_for     DESC NULLS LAST) AS off_pass_rank,
  RANK() OVER (PARTITION BY season, week ORDER BY rush_yards_for     DESC NULLS LAST) AS off_rush_rank,
  RANK() OVER (PARTITION BY season, week ORDER BY points_for         DESC NULLS LAST) AS off_scoring_rank,

  RANK() OVER (PARTITION BY season, week ORDER BY pass_yards_allowed ASC  NULLS LAST) AS def_pass_rank,
  RANK() OVER (PARTITION BY season, week ORDER BY rush_yards_allowed ASC  NULLS LAST) AS def_rush_rank,
  RANK() OVER (PARTITION BY season, week ORDER BY points_against     ASC  NULLS LAST) AS def_scoring_rank
FROM mart.v_team_weekly_stats;
"""

with engine.begin() as con:
    con.execute(text(SQL))

peek_sql = """
SELECT season, week, team,
       off_pass_rank, off_rush_rank, off_scoring_rank,
       def_pass_rank, def_rush_rank, def_scoring_rank
FROM mart.v_team_weekly_ranks
WHERE season = 2025 AND week IN (1,2,3,4)
ORDER BY week, off_pass_rank
LIMIT 50;
"""
with engine.begin() as con:
    rows = con.execute(text(peek_sql)).fetchmany(20)
for r in rows:
    print(r)
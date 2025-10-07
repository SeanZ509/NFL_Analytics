from sqlalchemy import create_engine, text
import pandas as pd

USER="SeanZahller"; PASS="YvMiTe9!2"; HOST="localhost"; PORT=5432; DB="nfl_warehouse"
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASS}@{HOST}:{PORT}/{DB}", pool_pre_ping=True)

def cols_present():
    with engine.connect() as con:
        df = pd.read_sql(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='hist_weekly'
            """),
            con
        )
    return set(df["column_name"].tolist())

def pick(C, options, cast=None, default="0"):
    """Pick the first existing column from options; coalesce + cast. If none exist, use default."""
    for name in options:
        if name in C:
            expr = f"hw.{name}"
            break
    else:
        expr = default
    if cast:
        d = f"{default}::{cast}"
        return f"COALESCE({expr}, {d})::{cast}" if expr.startswith("hw.") else d
    return f"COALESCE({expr}, {default})" if expr.startswith("hw.") else default


def build_v_player_games_sql():
    C = cols_present()
    completions     = pick(C, ["completions","cmp"],                    "int")
    attempts_pass   = pick(C, ["attempts","pass_attempts","att"],       "int")
    passing_yards   = pick(C, ["passing_yards","pass_yards"],           "numeric")
    passing_tds     = pick(C, ["passing_tds","pass_tds"],               "int")
    interceptions   = pick(C, ["interceptions","interceptions_thrown","ints"], "int")

    rushing_att     = pick(C, ["rushing_attempts","rushing_att","rush_attempts","carries"], "int")
    rushing_yards   = pick(C, ["rushing_yards","rush_yards"],           "numeric")
    rushing_tds     = pick(C, ["rushing_tds","rush_tds"],               "int")

    targets         = pick(C, ["targets"],                               "int")
    receptions      = pick(C, ["receptions","rec"],                      "int")
    receiving_yards = pick(C, ["receiving_yards","rec_yards"],           "numeric")
    receiving_tds   = pick(C, ["receiving_tds","rec_tds"],               "int")

    position        = pick(C, ["position"],                              None, "NULL")
    position_group  = pick(C, ["position_group"],                        None, position)  # fallback to position

    scrimmage_yards = f"(({rushing_yards}) + ({receiving_yards}))::numeric"
    skill_tds       = f"(({rushing_tds}) + ({receiving_tds}))::int"
    qb_total_yards  = f"(({passing_yards}) + ({rushing_yards}))::numeric"

    return f"""
    CREATE SCHEMA IF NOT EXISTS mart;

    CREATE OR REPLACE VIEW mart.v_player_games AS
    SELECT
      hw.player_id::text AS player_id,
      COALESCE(NULLIF(hw.player_name,''), hw.player_display_name)::text AS player_name,
      {position}::text       AS position,
      {position_group}::text AS position_group,
      hw.recent_team::text   AS team,
      hw.opponent_team::text AS opp_team,
      hw.season::int         AS season,
      hw.week::int           AS week,

      g.game_id,
      g.game_type,
      g.is_home,
      g.is_playoff,
      g.is_primetime,

      -- Passing
      {completions}     AS completions,
      {attempts_pass}   AS attempts,
      {passing_yards}   AS passing_yards,
      {passing_tds}     AS passing_tds,
      {interceptions}   AS interceptions,

      -- Rushing
      {rushing_att}     AS rushing_attempts,
      {rushing_yards}   AS rushing_yards,
      {rushing_tds}     AS rushing_tds,

      -- Receiving
      {targets}         AS targets,
      {receptions}      AS receptions,
      {receiving_yards} AS receiving_yards,
      {receiving_tds}   AS receiving_tds,

      -- Derived
      {scrimmage_yards} AS scrimmage_yards,
      {skill_tds}       AS skill_tds,
      {qb_total_yards}  AS qb_total_yards
    FROM hist_weekly hw
    JOIN mart.v_team_games_enriched g
      ON g.season = hw.season
     AND g.week   = hw.week
     AND g.team   = hw.recent_team;
    """

MARTS_SQL = r"""
-- RB all-time/season/last/rolling
CREATE OR REPLACE VIEW mart.v_rb_alltime_stats AS
SELECT
  player_id, MAX(player_name) AS player_name,
  COUNT(*) AS games,
  SUM(scrimmage_yards)       AS total_yards,
  SUM(rushing_yards)         AS rushing_yards,
  SUM(receiving_yards)       AS receiving_yards,
  SUM(rushing_attempts)      AS rushing_attempts,
  SUM(targets)               AS targets,
  SUM(skill_tds)             AS total_tds,
  ROUND(AVG(scrimmage_yards), 3) AS avg_yards_per_game,
  SUM(skill_tds)        FILTER (WHERE is_primetime) AS tds_primetime,
  SUM(scrimmage_yards)  FILTER (WHERE is_primetime) AS yards_primetime,
  SUM(skill_tds)        FILTER (WHERE is_playoff)   AS tds_playoff,
  SUM(scrimmage_yards)  FILTER (WHERE is_playoff)   AS yards_playoff
FROM mart.v_player_games
WHERE position_group='RB'
GROUP BY player_id;

CREATE OR REPLACE VIEW mart.v_rb_season_stats AS
SELECT
  season, player_id, MAX(player_name) AS player_name,
  COUNT(*) AS games,
  SUM(scrimmage_yards)       AS total_yards,
  SUM(rushing_yards)         AS rushing_yards,
  SUM(receiving_yards)       AS receiving_yards,
  SUM(rushing_attempts)      AS rushing_attempts,
  SUM(targets)               AS targets,
  SUM(skill_tds)             AS total_tds,
  ROUND(AVG(scrimmage_yards), 3) AS avg_yards_per_game,
  SUM(skill_tds)        FILTER (WHERE is_primetime) AS tds_primetime,
  SUM(scrimmage_yards)  FILTER (WHERE is_primetime) AS yards_primetime,
  SUM(skill_tds)        FILTER (WHERE is_playoff)   AS tds_playoff,
  SUM(scrimmage_yards)  FILTER (WHERE is_playoff)   AS yards_playoff
FROM mart.v_player_games
WHERE position_group='RB'
GROUP BY season, player_id;

CREATE OR REPLACE VIEW mart.v_rb_last_season_stats AS
SELECT *
FROM mart.v_rb_season_stats
WHERE season = (SELECT MAX(season) FROM hist_schedules);

CREATE OR REPLACE VIEW mart.v_rb_rolling3 AS
SELECT
  season, player_id, player_name, team, week,
  SUM(scrimmage_yards) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS yards_last3,
  AVG(scrimmage_yards) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_yards_last3,
  SUM(skill_tds)       OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS tds_last3
FROM mart.v_player_games
WHERE position_group='RB';

-- WR
CREATE OR REPLACE VIEW mart.v_wr_alltime_stats AS
SELECT
  player_id, MAX(player_name) AS player_name,
  COUNT(*) AS games,
  SUM(receiving_yards) AS receiving_yards,
  SUM(receptions)      AS receptions,
  SUM(targets)         AS targets,
  SUM(receiving_tds)   AS receiving_tds,
  SUM(scrimmage_yards) AS total_yards,
  ROUND(AVG(receiving_yards), 3) AS avg_rec_yards_per_game,
  SUM(receiving_tds)   FILTER (WHERE is_primetime)   AS tds_primetime,
  SUM(receiving_yards) FILTER (WHERE is_primetime)   AS yards_primetime,
  SUM(receiving_tds)   FILTER (WHERE is_playoff)     AS tds_playoff,
  SUM(receiving_yards) FILTER (WHERE is_playoff)     AS yards_playoff
FROM mart.v_player_games
WHERE position_group='WR'
GROUP BY player_id;

CREATE OR REPLACE VIEW mart.v_wr_season_stats AS
SELECT
  season, player_id, MAX(player_name) AS player_name,
  COUNT(*) AS games,
  SUM(receiving_yards) AS receiving_yards,
  SUM(receptions)      AS receptions,
  SUM(targets)         AS targets,
  SUM(receiving_tds)   AS receiving_tds,
  SUM(scrimmage_yards) AS total_yards,
  ROUND(AVG(receiving_yards), 3) AS avg_rec_yards_per_game,
  SUM(receiving_tds)   FILTER (WHERE is_primetime)   AS tds_primetime,
  SUM(receiving_yards) FILTER (WHERE is_primetime)   AS yards_primetime,
  SUM(receiving_tds)   FILTER (WHERE is_playoff)     AS tds_playoff,
  SUM(receiving_yards) FILTER (WHERE is_playoff)     AS yards_playoff
FROM mart.v_player_games
WHERE position_group='WR'
GROUP BY season, player_id;

CREATE OR REPLACE VIEW mart.v_wr_last_season_stats AS
SELECT *
FROM mart.v_wr_season_stats
WHERE season = (SELECT MAX(season) FROM hist_schedules);

CREATE OR REPLACE VIEW mart.v_wr_rolling3 AS
SELECT
  season, player_id, player_name, team, week,
  SUM(receiving_yards) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rec_yards_last3,
  AVG(receiving_yards) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_rec_yards_last3,
  SUM(receiving_tds)   OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rec_tds_last3
FROM mart.v_player_games
WHERE position_group='WR';

-- TE
CREATE OR REPLACE VIEW mart.v_te_alltime_stats AS
SELECT
  player_id, MAX(player_name) AS player_name,
  COUNT(*) AS games,
  SUM(receiving_yards) AS receiving_yards,
  SUM(receptions)      AS receptions,
  SUM(targets)         AS targets,
  SUM(receiving_tds)   AS receiving_tds,
  SUM(scrimmage_yards) AS total_yards,
  ROUND(AVG(receiving_yards), 3) AS avg_rec_yards_per_game,
  SUM(receiving_tds)   FILTER (WHERE is_primetime)   AS tds_primetime,
  SUM(receiving_yards) FILTER (WHERE is_primetime)   AS yards_primetime,
  SUM(receiving_tds)   FILTER (WHERE is_playoff)     AS tds_playoff,
  SUM(receiving_yards) FILTER (WHERE is_playoff)     AS yards_playoff
FROM mart.v_player_games
WHERE position_group='TE'
GROUP BY player_id;

CREATE OR REPLACE VIEW mart.v_te_season_stats AS
SELECT
  season, player_id, MAX(player_name) AS player_name,
  COUNT(*) AS games,
  SUM(receiving_yards) AS receiving_yards,
  SUM(receptions)      AS receptions,
  SUM(targets)         AS targets,
  SUM(receiving_tds)   AS receiving_tds,
  SUM(scrimmage_yards) AS total_yards,
  ROUND(AVG(receiving_yards), 3) AS avg_rec_yards_per_game,
  SUM(receiving_tds)   FILTER (WHERE is_primetime)   AS tds_primetime,
  SUM(receiving_yards) FILTER (WHERE is_primetime)   AS yards_primetime,
  SUM(receiving_tds)   FILTER (WHERE is_playoff)     AS tds_playoff,
  SUM(receiving_yards) FILTER (WHERE is_playoff)     AS yards_playoff
FROM mart.v_player_games
WHERE position_group='TE'
GROUP BY season, player_id;

CREATE OR REPLACE VIEW mart.v_te_last_season_stats AS
SELECT *
FROM mart.v_te_season_stats
WHERE season = (SELECT MAX(season) FROM hist_schedules);

CREATE OR REPLACE VIEW mart.v_te_rolling3 AS
SELECT
  season, player_id, player_name, team, week,
  SUM(receiving_yards) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rec_yards_last3,
  AVG(receiving_yards) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_rec_yards_last3,
  SUM(receiving_tds)   OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rec_tds_last3
FROM mart.v_player_games
WHERE position_group='TE';

-- QB
CREATE OR REPLACE VIEW mart.v_qb_stats_alltime AS
SELECT
  player_id, MAX(player_name) AS player_name,
  COUNT(*) AS games,
  SUM(passing_yards)   AS passing_yards,
  SUM(completions)     AS completions,
  SUM(attempts)        AS attempts,
  SUM(passing_tds)     AS passing_tds,
  SUM(interceptions)   AS interceptions,
  SUM(rushing_yards)   AS rushing_yards,
  SUM(qb_total_yards)  AS total_yards_qb,
  ROUND(AVG(passing_yards), 3) AS avg_pass_yards_per_game,
  SUM(passing_tds)   FILTER (WHERE is_primetime)   AS passing_tds_primetime,
  SUM(passing_yards) FILTER (WHERE is_primetime)   AS passing_yards_primetime,
  SUM(passing_tds)   FILTER (WHERE is_playoff)     AS passing_tds_playoff,
  SUM(passing_yards) FILTER (WHERE is_playoff)     AS passing_yards_playoff
FROM mart.v_player_games
WHERE position_group='QB'
GROUP BY player_id;

CREATE OR REPLACE VIEW mart.v_qb_stats_season AS
SELECT
  season, player_id, MAX(player_name) AS player_name,
  COUNT(*) AS games,
  SUM(passing_yards)   AS passing_yards,
  SUM(completions)     AS completions,
  SUM(attempts)        AS attempts,
  SUM(passing_tds)     AS passing_tds,
  SUM(interceptions)   AS interceptions,
  SUM(rushing_yards)   AS rushing_yards,
  SUM(qb_total_yards)  AS total_yards_qb,
  ROUND(AVG(passing_yards), 3) AS avg_pass_yards_per_game,
  SUM(passing_tds)   FILTER (WHERE is_primetime)   AS passing_tds_primetime,
  SUM(passing_yards) FILTER (WHERE is_primetime)   AS passing_yards_primetime,
  SUM(passing_tds)   FILTER (WHERE is_playoff)     AS passing_tds_playoff,
  SUM(passing_yards) FILTER (WHERE is_playoff)     AS passing_yards_playoff
FROM mart.v_player_games
WHERE position_group='QB'
GROUP BY season, player_id;

CREATE OR REPLACE VIEW mart.v_qb_stats_last_season AS
SELECT *
FROM mart.v_qb_stats_season
WHERE season = (SELECT MAX(season) FROM hist_schedules);

CREATE OR REPLACE VIEW mart.v_qb_rolling3 AS
SELECT
  season, player_id, player_name, team, week,
  SUM(passing_yards) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS pass_yards_last3,
  AVG(passing_yards) OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_pass_yards_last3,
  SUM(passing_tds)   OVER (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS pass_tds_last3
FROM mart.v_player_games
WHERE position_group='QB';
"""


def main():
    vpg_sql = build_v_player_games_sql()
    with engine.begin() as con:
        con.execute(text(vpg_sql))
        con.execute(text(MARTS_SQL))
    print("✅ Created/updated: mart.v_player_games and all position marts (RB/WR/TE/QB).")

    with engine.connect() as con:
        count = con.execute(text("SELECT COUNT(*) FROM mart.v_player_games")).scalar()
        print(f"v_player_games rows: {count:,}")
        top_rb = pd.read_sql(
            text("""SELECT player_name, total_yards, rushing_yards, receiving_yards, total_tds
                    FROM mart.v_rb_alltime_stats
                    ORDER BY total_yards DESC NULLS LAST LIMIT 5"""),
            con
        )
        print("\nRB leaders (all-time, top 5 by total_yards):")
        print(top_rb.to_string(index=False))


if __name__ == "__main__":
    main()
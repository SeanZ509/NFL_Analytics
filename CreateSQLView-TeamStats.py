from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd

USER = "SeanZahller"
PASS = "YvMiTe9!2"
HOST = "localhost"
PORT = 5432          
DB   = "nfl_warehouse"

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASS}@{HOST}:{PORT}/{DB}", pool_pre_ping=True)

SQL_BUILD = r"""
-- 0) schema
CREATE SCHEMA IF NOT EXISTS mart;

-- 1) Per-team-per-game base with handy flags
CREATE OR REPLACE VIEW mart.v_team_games AS
WITH base AS (
  SELECT
    game_id,
    season::int        AS season,
    week::int          AS week,
    game_type,
    gameday::date      AS gamedate,
    NULLIF(gametime,'') AS gametime_txt,
    CASE
      WHEN gametime ~ '^\d{1,2}:\d{2}$' THEN gametime::time
      ELSE NULL
    END                AS gametime,
    weekday,
    home_team, away_team,
    home_score::numeric AS home_score,
    away_score::numeric AS away_score
  FROM hist_schedules
  WHERE home_score IS NOT NULL AND away_score IS NOT NULL
)
SELECT
  b.game_id, b.season, b.week, b.game_type, b.gamedate, b.gametime, b.weekday,
  b.home_team, b.away_team, b.home_score, b.away_score,
  t.team,
  CASE WHEN t.team = b.home_team THEN b.away_team ELSE b.home_team END AS opp,
  (t.team = b.home_team) AS is_home,
  CASE
    WHEN b.home_score = b.away_score THEN 0.5
    WHEN (t.team=b.home_team AND b.home_score > b.away_score)
      OR (t.team=b.away_team AND b.away_score > b.home_score) THEN 1.0
    ELSE 0.0
  END::numeric AS win_pts,
  (b.game_type IN ('WC','DIV','CON','SB')) AS is_playoff,
  CASE
    WHEN b.weekday IN ('Monday','Thursday') THEN TRUE
    WHEN b.weekday IN ('Sunday','Saturday') AND b.gametime >= time '19:00' THEN TRUE
    ELSE FALSE
  END AS is_primetime,
  (b.gametime IS NOT NULL AND b.gametime < time '15:00')                    AS is_morning,
  (b.gametime IS NOT NULL AND b.gametime >= time '15:00' AND b.gametime < time '19:00') AS is_afternoon,
  (b.gametime IS NOT NULL AND b.gametime >= time '19:00')                   AS is_evening,
  CASE WHEN t.team=b.home_team THEN b.home_score ELSE b.away_score END AS team_score,
  CASE WHEN t.team=b.home_team THEN b.away_score ELSE b.home_score END AS opp_score
FROM base b
CROSS JOIN LATERAL (VALUES (b.home_team),(b.away_team)) AS t(team);

-- 2) Season-end record per team
CREATE OR REPLACE VIEW mart.v_team_season_record AS
SELECT
  season, team,
  SUM(CASE WHEN win_pts=1   THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN win_pts=0   THEN 1 ELSE 0 END) AS losses,
  SUM(CASE WHEN win_pts=0.5 THEN 1 ELSE 0 END) AS ties,
  SUM(win_pts)/COUNT(*) AS win_pct
FROM mart.v_team_games
GROUP BY season, team;

-- 3) Enrich with opponent season win%
CREATE OR REPLACE VIEW mart.v_team_games_enriched AS
SELECT g.*, (sr_opp.win_pct >= 0.5) AS opp_is_500_plus
FROM mart.v_team_games g
JOIN mart.v_team_season_record sr_opp
  ON sr_opp.season = g.season
 AND sr_opp.team   = g.opp;

-- 4) All-time splits
CREATE OR REPLACE VIEW mart.v_team_alltime_splits AS
SELECT
  team,
  COUNT(*)                                   AS games,
  SUM(CASE WHEN win_pts=1   THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN win_pts=0   THEN 1 ELSE 0 END) AS losses,
  SUM(CASE WHEN win_pts=0.5 THEN 1 ELSE 0 END) AS ties,
  ROUND(SUM(win_pts)/COUNT(*)::numeric, 4)   AS win_pct,

  SUM(CASE WHEN is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_home,
  SUM(CASE WHEN NOT is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_away,

  SUM(CASE WHEN is_primetime AND win_pts=1 THEN 1 ELSE 0 END) AS wins_primetime,
  SUM(CASE WHEN is_morning   AND win_pts=1 THEN 1 ELSE 0 END) AS wins_morning,
  SUM(CASE WHEN is_afternoon AND win_pts=1 THEN 1 ELSE 0 END) AS wins_afternoon,

  SUM(CASE WHEN is_playoff AND win_pts=1 THEN 1 ELSE 0 END) AS wins_playoff,
  SUM(CASE WHEN NOT is_playoff AND win_pts=1 THEN 1 ELSE 0 END) AS wins_regular,

  SUM(CASE WHEN opp_is_500_plus THEN 1 ELSE 0 END)                       AS games_vs_500,
  SUM(CASE WHEN opp_is_500_plus THEN win_pts ELSE 0 END)                 AS wins_pts_vs_500,
  SUM(CASE WHEN opp_is_500_plus AND win_pts=1 THEN 1 ELSE 0 END)         AS wins_vs_500,
  ROUND(
    SUM(CASE WHEN opp_is_500_plus THEN win_pts ELSE 0 END)
    / NULLIF(SUM(CASE WHEN opp_is_500_plus THEN 1 ELSE 0 END),0)::numeric, 4
  ) AS win_pct_vs_500
FROM mart.v_team_games_enriched
GROUP BY team;

-- 5) Per-season splits
CREATE OR REPLACE VIEW mart.v_team_season_splits AS
SELECT
  season, team,
  COUNT(*)                                   AS games,
  SUM(CASE WHEN win_pts=1   THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN win_pts=0   THEN 1 ELSE 0 END) AS losses,
  SUM(CASE WHEN win_pts=0.5 THEN 1 ELSE 0 END) AS ties,
  ROUND(SUM(win_pts)/COUNT(*)::numeric, 4)   AS win_pct,

  SUM(CASE WHEN is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_home,
  SUM(CASE WHEN NOT is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_away,

  SUM(CASE WHEN is_primetime AND win_pts=1 THEN 1 ELSE 0 END) AS wins_primetime,
  SUM(CASE WHEN is_morning   AND win_pts=1 THEN 1 ELSE 0 END) AS wins_morning,
  SUM(CASE WHEN is_afternoon AND win_pts=1 THEN 1 ELSE 0 END) AS wins_afternoon,

  SUM(CASE WHEN is_playoff AND win_pts=1 THEN 1 ELSE 0 END) AS wins_playoff,
  SUM(CASE WHEN NOT is_playoff AND win_pts=1 THEN 1 ELSE 0 END) AS wins_regular,

  SUM(CASE WHEN opp_is_500_plus THEN 1 ELSE 0 END)                       AS games_vs_500,
  SUM(CASE WHEN opp_is_500_plus THEN win_pts ELSE 0 END)                 AS wins_pts_vs_500,
  SUM(CASE WHEN opp_is_500_plus AND win_pts=1 THEN 1 ELSE 0 END)         AS wins_vs_500,
  ROUND(
    SUM(CASE WHEN opp_is_500_plus THEN win_pts ELSE 0 END)
    / NULLIF(SUM(CASE WHEN opp_is_500_plus THEN 1 ELSE 0 END),0)::numeric, 4
  ) AS win_pct_vs_500
FROM mart.v_team_games_enriched
GROUP BY season, team;

-- 6) Last season convenience
CREATE OR REPLACE VIEW mart.v_team_last_season_splits AS
SELECT s.*
FROM mart.v_team_season_splits s
WHERE s.season = (SELECT MAX(season) FROM hist_schedules);
"""

def run_build():
    with engine.begin() as con:
        con.execute(text(SQL_BUILD))
    print("✅ Feature mart views created/updated.")

def load_division_mapping(csv_path: Path):
    """
    CSV columns expected: season,team,conference,division
    Example rows:
      2024,KC,AFC,West
      2024,LAC,AFC,West
      ...
    """
    if not csv_path.exists():
        print(f"[skip] division CSV not found: {csv_path}")
        return
    df = pd.read_csv(csv_path, dtype={"season": int, "team": str, "conference": str, "division": str})
    with engine.begin() as con:
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_team_division(
              season int,
              team text,
              conference text,
              division text,
              PRIMARY KEY (season, team)
            );
        """))
        tmp_table = "dim_team_division_tmp"
        df.to_sql(tmp_table, engine, if_exists="replace", index=False)
        con.execute(text(f"""
            INSERT INTO dim_team_division(season,team,conference,division)
            SELECT season,team,conference,division FROM {tmp_table}
            ON CONFLICT (season,team) DO UPDATE
              SET conference=EXCLUDED.conference, division=EXCLUDED.division;
            DROP TABLE {tmp_table};
        """))
    print(f"✅ Division mapping loaded from {csv_path}")

def sanity_peek():
    q1 = """
      SELECT team, games, wins, losses, ties, win_pct
      FROM mart.v_team_alltime_splits
      ORDER BY win_pct DESC, wins DESC
      LIMIT 10;
    """
    q2 = """
      SELECT season, team, wins_home, wins_away, wins, losses, ties, win_pct
      FROM mart.v_team_last_season_splits
      ORDER BY win_pct DESC, wins DESC
      LIMIT 10;
    """
    with engine.connect() as con:
        print("\nTop all-time win% (quick peek):")
        print(pd.read_sql(text(q1), con).to_string(index=False))
        print("\nLast season leaders (quick peek):")
        print(pd.read_sql(text(q2), con).to_string(index=False))

if __name__ == "__main__":
    run_build()
    sanity_peek()
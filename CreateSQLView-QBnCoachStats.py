from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import nfl_data_py as nfl

# --- DB CONFIG (edit if needed) ---
USER="SeanZahller"; PASS="YvMiTe9!2"; HOST="localhost"; PORT=5432; DB="nfl_warehouse"
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASS}@{HOST}:{PORT}/{DB}", pool_pre_ping=True)

# ============================
# QB VIEWS
# ============================
SQL_QB = r"""
CREATE SCHEMA IF NOT EXISTS mart;

-- 1) Primary QB per team-week (most attempts; tiebreak by passing yards)
CREATE OR REPLACE VIEW mart.v_qb_primary_game AS
WITH qb AS (
  SELECT
    season::int AS season,
    week::int   AS week,
    recent_team::text AS team,
    player_id::text AS qb_id,
    COALESCE(NULLIF(player_name,''), player_display_name)::text AS qb_name,
    COALESCE(attempts,0)::int AS attempts,
    COALESCE(passing_yards,0)::int AS passing_yards,
    ROW_NUMBER() OVER (
      PARTITION BY season, week, recent_team
      ORDER BY COALESCE(attempts,0) DESC, COALESCE(passing_yards,0) DESC, player_id
    ) AS rn
  FROM hist_weekly
  WHERE (position='QB' OR position_group='QB')
)
SELECT season, week, team, qb_id, qb_name, attempts, passing_yards
FROM qb
WHERE rn = 1 AND attempts >= 1;

-- 2) Join QB to per-team game features (uses your mart.v_team_games_enriched)
CREATE OR REPLACE VIEW mart.v_qb_games AS
SELECT
  g.game_id, g.season, g.week, g.game_type, g.gamedate, g.gametime, g.weekday,
  g.team, g.opp, g.is_home, g.is_playoff, g.is_primetime, g.is_morning, g.is_afternoon, g.is_evening,
  g.win_pts, g.opp_is_500_plus,
  q.qb_id, q.qb_name, q.attempts, q.passing_yards
FROM mart.v_team_games_enriched g
JOIN mart.v_qb_primary_game q
  ON q.season = g.season AND q.week = g.week AND q.team = g.team;

-- 3) QB all-time splits
CREATE OR REPLACE VIEW mart.v_qb_alltime_splits AS
SELECT
  qb_id, MAX(qb_name) AS qb_name,
  COUNT(*) AS games,
  SUM(CASE WHEN win_pts=1   THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN win_pts=0   THEN 1 ELSE 0 END) AS losses,
  SUM(CASE WHEN win_pts=0.5 THEN 1 ELSE 0 END) AS ties,
  ROUND(SUM(win_pts)/COUNT(*)::numeric, 4) AS win_pct,

  SUM(CASE WHEN is_home THEN 1 ELSE 0 END) AS games_home,
  SUM(CASE WHEN is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_home,
  SUM(CASE WHEN NOT is_home THEN 1 ELSE 0 END) AS games_away,
  SUM(CASE WHEN NOT is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_away,

  SUM(CASE WHEN is_primetime THEN 1 ELSE 0 END) AS games_primetime,
  SUM(CASE WHEN is_primetime AND win_pts=1 THEN 1 ELSE 0 END) AS wins_primetime,

  SUM(CASE WHEN is_morning THEN 1 ELSE 0 END) AS games_morning,
  SUM(CASE WHEN is_morning AND win_pts=1 THEN 1 ELSE 0 END) AS wins_morning,

  SUM(CASE WHEN is_afternoon THEN 1 ELSE 0 END) AS games_afternoon,
  SUM(CASE WHEN is_afternoon AND win_pts=1 THEN 1 ELSE 0 END) AS wins_afternoon,

  SUM(CASE WHEN is_evening THEN 1 ELSE 0 END) AS games_evening,
  SUM(CASE WHEN is_evening AND win_pts=1 THEN 1 ELSE 0 END) AS wins_evening,

  SUM(CASE WHEN is_playoff THEN 1 ELSE 0 END) AS games_playoff,
  SUM(CASE WHEN is_playoff AND win_pts=1 THEN 1 ELSE 0 END) AS wins_playoff,

  SUM(CASE WHEN opp_is_500_plus THEN 1 ELSE 0 END) AS games_vs_500,
  SUM(CASE WHEN opp_is_500_plus AND win_pts=1 THEN 1 ELSE 0 END) AS wins_vs_500
FROM mart.v_qb_games
GROUP BY qb_id;

-- 4) QB per-season splits
CREATE OR REPLACE VIEW mart.v_qb_season_splits AS
SELECT
  season, qb_id, MAX(qb_name) AS qb_name,
  COUNT(*) AS games,
  SUM(CASE WHEN win_pts=1   THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN win_pts=0   THEN 1 ELSE 0 END) AS losses,
  SUM(CASE WHEN win_pts=0.5 THEN 1 ELSE 0 END) AS ties,
  ROUND(SUM(win_pts)/COUNT(*)::numeric, 4) AS win_pct,

  SUM(CASE WHEN is_home THEN 1 ELSE 0 END) AS games_home,
  SUM(CASE WHEN is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_home,
  SUM(CASE WHEN NOT is_home THEN 1 ELSE 0 END) AS games_away,
  SUM(CASE WHEN NOT is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_away,

  SUM(CASE WHEN is_primetime THEN 1 ELSE 0 END) AS games_primetime,
  SUM(CASE WHEN is_primetime AND win_pts=1 THEN 1 ELSE 0 END) AS wins_primetime,

  SUM(CASE WHEN is_playoff THEN 1 ELSE 0 END) AS games_playoff,
  SUM(CASE WHEN is_playoff AND win_pts=1 THEN 1 ELSE 0 END) AS wins_playoff,

  SUM(CASE WHEN opp_is_500_plus THEN 1 ELSE 0 END) AS games_vs_500,
  SUM(CASE WHEN opp_is_500_plus AND win_pts=1 THEN 1 ELSE 0 END) AS wins_vs_500
FROM mart.v_qb_games
GROUP BY season, qb_id;

-- 5) Last-season QB splits
CREATE OR REPLACE VIEW mart.v_qb_last_season_splits AS
SELECT s.*
FROM mart.v_qb_season_splits s
WHERE s.season = (SELECT MAX(season) FROM hist_schedules);
"""

# ============================
# COACH MAPPING + VIEWS
# ============================
DDL_COACH_DIM = r"""
CREATE TABLE IF NOT EXISTS dim_team_head_coach(
  season int NOT NULL,
  week   int NOT NULL,
  team   text NOT NULL,
  head_coach text NOT NULL,
  PRIMARY KEY (season, week, team)
);
"""

SQL_COACH_VIEWS = r"""
CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW mart.v_coach_games AS
SELECT g.*, c.head_coach
FROM mart.v_team_games_enriched g
JOIN dim_team_head_coach c
  ON c.season=g.season AND c.week=g.week AND c.team=g.team;

CREATE OR REPLACE VIEW mart.v_coach_alltime_splits AS
SELECT head_coach,
       COUNT(*) AS games,
       SUM(CASE WHEN win_pts=1   THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN win_pts=0   THEN 1 ELSE 0 END) AS losses,
       SUM(CASE WHEN win_pts=0.5 THEN 1 ELSE 0 END) AS ties,
       ROUND(SUM(win_pts)/COUNT(*)::numeric, 4) AS win_pct,
       SUM(CASE WHEN is_home THEN 1 ELSE 0 END) AS games_home,
       SUM(CASE WHEN is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_home,
       SUM(CASE WHEN NOT is_home THEN 1 ELSE 0 END) AS games_away,
       SUM(CASE WHEN NOT is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_away,
       SUM(CASE WHEN is_primetime THEN 1 ELSE 0 END) AS games_primetime,
       SUM(CASE WHEN is_primetime AND win_pts=1 THEN 1 ELSE 0 END) AS wins_primetime,
       SUM(CASE WHEN is_playoff THEN 1 ELSE 0 END) AS games_playoff,
       SUM(CASE WHEN is_playoff AND win_pts=1 THEN 1 ELSE 0 END) AS wins_playoff,
       SUM(CASE WHEN opp_is_500_plus THEN 1 ELSE 0 END) AS games_vs_500,
       SUM(CASE WHEN opp_is_500_plus AND win_pts=1 THEN 1 ELSE 0 END) AS wins_vs_500
FROM mart.v_coach_games
GROUP BY head_coach;

CREATE OR REPLACE VIEW mart.v_coach_season_splits AS
SELECT season, head_coach,
       COUNT(*) AS games,
       SUM(CASE WHEN win_pts=1   THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN win_pts=0   THEN 1 ELSE 0 END) AS losses,
       SUM(CASE WHEN win_pts=0.5 THEN 1 ELSE 0 END) AS ties,
       ROUND(SUM(win_pts)/COUNT(*)::numeric, 4) AS win_pct,
       SUM(CASE WHEN is_home THEN 1 ELSE 0 END) AS games_home,
       SUM(CASE WHEN is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_home,
       SUM(CASE WHEN NOT is_home THEN 1 ELSE 0 END) AS games_away,
       SUM(CASE WHEN NOT is_home AND win_pts=1 THEN 1 ELSE 0 END) AS wins_away,
       SUM(CASE WHEN is_primetime THEN 1 ELSE 0 END) AS games_primetime,
       SUM(CASE WHEN is_primetime AND win_pts=1 THEN 1 ELSE 0 END) AS wins_primetime,
       SUM(CASE WHEN is_playoff THEN 1 ELSE 0 END) AS games_playoff,
       SUM(CASE WHEN is_playoff AND win_pts=1 THEN 1 ELSE 0 END) AS wins_playoff,
       SUM(CASE WHEN opp_is_500_plus THEN 1 ELSE 0 END) AS games_vs_500,
       SUM(CASE WHEN opp_is_500_plus AND win_pts=1 THEN 1 ELSE 0 END) AS wins_vs_500
FROM mart.v_coach_games
GROUP BY season, head_coach;

CREATE OR REPLACE VIEW mart.v_coach_last_season_splits AS
SELECT *
FROM mart.v_coach_season_splits
WHERE season = (SELECT MAX(season) FROM hist_schedules);
"""

def build_qb_views():
    with engine.begin() as con:
        con.execute(text(SQL_QB))
    print("✅ QB views created/updated.")

def refresh_coach_mapping_from_schedules():
    """
    Build (season, week, team, head_coach) by merging nfl_data_py schedules to our DB on game_id,
    then use *DB team codes* to avoid any abbreviation mismatches (e.g., LAR/LA, JAC/JAX).
    """
    # Seasons we have in DB
    with engine.connect() as con:
        min_season, max_season = con.execute(text("SELECT MIN(season), MAX(season) FROM hist_schedules")).one()
    min_season, max_season = int(min_season), int(max_season)
    seasons = list(range(min_season, max_season + 1))
    print(f"Building coach map for seasons {min_season}-{max_season}...")

    # Pull schedules (need game_id + coaches)
    sched = nfl.import_schedules(seasons)
    needed = {"game_id","season","week","game_type","home_coach","away_coach"}
    missing = needed - set(sched.columns)
    if {"home_coach","away_coach"} & missing:
        raise RuntimeError("Schedules missing coach columns. Upgrade nfl_data_py:  pip install -U nfl_data_py")

    sched = sched[list(needed)].copy()

    # Get our DB's game_id + team codes (truth for team abbreviations)
    with engine.connect() as con:
        db_sched = pd.read_sql(
            text("""
                SELECT game_id, season::int AS season, week::int AS week, game_type,
                       home_team, away_team
                FROM hist_schedules
                WHERE home_score IS NOT NULL AND away_score IS NOT NULL
            """), con
        )

    # Merge on game_id to align precisely
    m = db_sched.merge(
        sched,
        on=["game_id","season","week","game_type"],
        how="left",
        validate="one_to_one",
        suffixes=("_db","_src")
    )

    # Build mapping rows (use DB team codes with coach names from schedules)
    home_rows = m[["season","week","game_type","home_team","home_coach"]].rename(
        columns={"home_team":"team","home_coach":"head_coach"}
    )
    away_rows = m[["season","week","game_type","away_team","away_coach"]].rename(
        columns={"away_team":"team","away_coach":"head_coach"}
    )
    coach_map = pd.concat([home_rows, away_rows], ignore_index=True)

    # Keep regular + postseason, valid coach names only
    coach_map = coach_map[coach_map["game_type"].isin(["REG","WC","DIV","CON","SB"])]
    coach_map = coach_map.dropna(subset=["head_coach"]).copy()
    coach_map["head_coach"] = coach_map["head_coach"].astype(str).str.strip()

    # Final shape & types
    coach_map = coach_map.drop_duplicates(subset=["season","week","team"])
    coach_map = coach_map.astype({"season":int,"week":int,"team":"string","head_coach":"string"})

    print(f"Prepared {len(coach_map):,} coach rows to upsert.")

    # Ensure dim table exists and UPSERT
    with engine.begin() as con:
        con.execute(text(DDL_COACH_DIM))
        tmp = "dim_team_head_coach_tmp"
        coach_map.to_sql(tmp, con, if_exists="replace", index=False)
        con.execute(text(f"""
            INSERT INTO dim_team_head_coach(season,week,team,head_coach)
            SELECT season,week,team,head_coach FROM {tmp}
            ON CONFLICT (season,week,team) DO UPDATE
              SET head_coach = EXCLUDED.head_coach;
            DROP TABLE {tmp};
        """))
    print("✅ dim_team_head_coach upsert complete (team codes from DB).")

def build_coach_views():
    with engine.begin() as con:
        con.execute(text(SQL_COACH_VIEWS))
    print("✅ Coach views created/updated.")

def sanity_peek():
    with engine.connect() as con:
        print("\nTop QBs by wins (all-time):")
        q1 = """
            SELECT qb_name, games, wins, losses, ties, win_pct,
                   games_home, wins_home, games_away, wins_away,
                   games_primetime, wins_primetime, games_playoff, wins_playoff
            FROM mart.v_qb_alltime_splits
            ORDER BY wins DESC
            LIMIT 10;
        """
        print(pd.read_sql(text(q1), con).to_string(index=False))

        print("\nTop Coaches by wins (all-time):")
        q2 = """
            SELECT head_coach, games, wins, losses, ties, win_pct,
                   games_home, wins_home, games_away, wins_away,
                   games_primetime, wins_primetime, games_playoff, wins_playoff
            FROM mart.v_coach_alltime_splits
            ORDER BY wins DESC
            LIMIT 10;
        """
        try:
            print(pd.read_sql(text(q2), con).to_string(index=False))
        except Exception as e:
            print("Coach views not available yet (did coach mapping fail?).", e)

if __name__ == "__main__":
    # QB views depend on your existing team-game mart views (built earlier).
    build_qb_views()

    # Build/refresh coach mapping from schedules, then create coach views.
    refresh_coach_mapping_from_schedules()
    build_coach_views()

    # Quick read-only check
    sanity_peek()
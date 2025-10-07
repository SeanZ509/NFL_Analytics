from __future__ import annotations
import pandas as pd
import polars as pl
from sqlalchemy import create_engine, text
import nflreadpy as nread

SEASON = 2025
WEEKS  = None  

PG_USER = "SeanZahller"
PG_PASS = "YvMiTe9!2"
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "nfl_warehouse"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

def get_table_columns(table: str) -> list[str]:
    with engine.begin() as con:
        rows = con.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=:t
            ORDER BY ordinal_position
        """), {"t": table}).fetchall()
    return [r[0] for r in rows]

def write_df(table: str, df: pd.DataFrame):
    if df.empty:
        print(f"[skip] {table}: nothing to write")
        return
    for c in df.select_dtypes(include="object").columns:
        df[c] = df[c].astype(str)
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=50_000)
    print(f"[ok] {table}: +{len(df):,} rows")

def harmonize_weekly(df: pd.DataFrame, target_table: str) -> pd.DataFrame:
    """
    Map nflreadpy columns to your existing hist_weekly schema (from nfl_data_py).
    Drop any columns not present in the table.
    """
    rename_map = {
        "team": "recent_team",                    
        "passing_interceptions": "interceptions",  
        "sacks_suffered": "sacks",                 
        "sack_yards_lost": "sack_yards",          
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    for c in ("season", "week"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    table_cols = set(get_table_columns(target_table))
    keep = [c for c in df.columns if c in table_cols]
    dropped = [c for c in df.columns if c not in table_cols]
    if dropped:
        print(f"[info] dropping {len(dropped)} cols not in {target_table}: {dropped[:8]}{'...' if len(dropped)>8 else ''}")
    return df[keep]

def main():
    with engine.begin() as con:
        print("DB:", con.execute(text("select version()")).scalar())

    sch_pl: pl.DataFrame = nread.load_schedules(SEASON)
    if WEEKS is None:
        completed_weeks = (
            sch_pl
            .filter(
                (pl.col("game_type") == "REG") &
                pl.col("home_score").is_not_null() &
                pl.col("away_score").is_not_null()
            )
            .select(pl.col("week"))
            .unique()
            .sort("week")
            .get_column("week")
            .to_list()
        )
        weeks_to_pull = completed_weeks
    else:
        weeks_to_pull = list(WEEKS)

    if not weeks_to_pull:
        print(f"[info] No completed regular-season weeks detected for {SEASON}. Nothing to do.")
        return

    print(f"[info] Weeks to pull for {SEASON}: {weeks_to_pull}")
    sch_pl = (
        sch_pl
        .filter(
            (pl.col("game_type") == "REG") &
            (pl.col("week").is_in(weeks_to_pull)) &
            (pl.col("home_score").is_not_null()) &
            (pl.col("away_score").is_not_null())
        )
    )
    schedules = sch_pl.to_pandas()
    wk_pl: pl.DataFrame = nread.load_player_stats(SEASON, summary_level="week")
    wk_pl = wk_pl.filter(
        (pl.col("season_type") == "REG") &
        (pl.col("week").is_in(weeks_to_pull))
    )
    weekly = wk_pl.to_pandas()

    with engine.begin() as con:
        con.execute(text("""
            DELETE FROM hist_schedules
            WHERE season=:s AND week = ANY(:w)
        """), {"s": SEASON, "w": weeks_to_pull})
        con.execute(text("""
            DELETE FROM hist_weekly
            WHERE season=:s AND week = ANY(:w)
        """), {"s": SEASON, "w": weeks_to_pull})

    write_df("hist_schedules", schedules)
    weekly_h = harmonize_weekly(weekly, "hist_weekly")
    write_df("hist_weekly", weekly_h)

    with engine.begin() as con:
        cnt_s = con.execute(
            text("SELECT COUNT(*) FROM hist_schedules WHERE season=:s AND week=ANY(:w)"),
            {"s": SEASON, "w": weeks_to_pull}
        ).scalar()
        cnt_w = con.execute(
            text("SELECT COUNT(*) FROM hist_weekly WHERE season=:s AND week=ANY(:w)"),
            {"s": SEASON, "w": weeks_to_pull}
        ).scalar()
    print(f"[count] hist_schedules {SEASON} W{weeks_to_pull[0]}–{weeks_to_pull[-1]}: {cnt_s:,}")
    print(f"[count] hist_weekly   {SEASON} W{weeks_to_pull[0]}–{weeks_to_pull[-1]}: {cnt_w:,}")
    print("\n✅ In-season load complete.")

if __name__ == "__main__":
    main()
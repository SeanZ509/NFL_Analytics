from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

# --- EDIT these ---
DATA_DIR = Path(r"C:\Users\seanz\VSCode_WS\Sports\NFL_Analytics\data_historic")
DB_URL   = "postgresql+psycopg2://SeanZahller:YvMiTe9!2@localhost:32769/nfl_warehouse"
EXPECTED_PBP_SEASONS = list(range(2000, 2025))  # 2000..2024

engine = create_engine(DB_URL)

def table_rowcount(con, table):
    try:
        return con.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
    except Exception:
        return None

def ensure_table_from_parquet(table, file, if_exists="replace", chunksize=100_000):
    if not file.exists():
        print(f"[skip] {file.name} not found")
        return
    df = pd.read_parquet(file)
    if df.empty:
        print(f"[skip] {file.name} empty")
        return
    print(f"[load] {file.name} -> {table} ({len(df):,} rows)")
    df.to_sql(table, engine, if_exists=if_exists, index=False, method="multi", chunksize=chunksize)

def seasons_present(con):
    try:
        rows = con.execute(text("SELECT DISTINCT season FROM hist_pbp")).fetchall()
        return sorted([r[0] for r in rows])
    except Exception:
        return []

def main():
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"DATA_DIR not found: {DATA_DIR}")
    print("Using DATA_DIR:", DATA_DIR)

    with engine.begin() as con:
        print("DB version:", con.execute(text("select version()")).scalar(), "\n")

        # 1) Core “whole-file” tables: load only if missing/empty
        core = [
            ("hist_schedules", DATA_DIR / "schedules_2000_2024.parquet"),
            ("hist_weekly", DATA_DIR / "weekly_2000_2024.parquet"),
            ("hist_rosters_seasonal", DATA_DIR / "rosters_seasonal_2000_2024.parquet"),
            ("hist_rosters_weekly", DATA_DIR / "rosters_weekly_available_years.parquet"),
        ]
        for table, file in core:
            cnt = table_rowcount(con, table)
            if cnt is None or cnt == 0:
                print(f"[ensure] {table} is missing/empty -> loading")
                ensure_table_from_parquet(table, file, if_exists=("replace"))
            else:
                print(f"[ok]     {table} already has {cnt:,} rows")

    # 2) PBP per-season resume
    with engine.begin() as con:
        have = seasons_present(con)
    need = [s for s in EXPECTED_PBP_SEASONS if s not in have]
    print("\nPBP seasons present:", have or "(none)")
    print("PBP seasons needed :", need or "(none)")

    first_append = False
    # If hist_pbp doesn’t exist yet, create it on the first season we load
    with engine.begin() as con:
        cnt = table_rowcount(con, "hist_pbp")
        if cnt is None:
            first_append = True

    for s in need:
        f = DATA_DIR / f"pbp_{s}.parquet"
        if not f.exists():
            print(f"[skip] pbp_{s}.parquet not found")
            continue
        df = pd.read_parquet(f)
        if df.empty:
            print(f"[skip] pbp_{s}.parquet empty")
            continue
        mode = "replace" if first_append else "append"
        print(f"[{mode}] pbp_{s}.parquet -> hist_pbp ({len(df):,} rows)")
        df.to_sql("hist_pbp", engine, if_exists=mode, index=False, method="multi", chunksize=100_000)
        first_append = False

    # Final counts
    with engine.begin() as con:
        for t in ["hist_schedules","hist_weekly","hist_rosters_seasonal","hist_rosters_weekly","hist_pbp"]:
            try:
                cnt = con.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                print(f"{t:24s} {cnt:,}")
            except Exception:
                print(f"{t:24s} (missing)")

        rows = con.execute(text("SELECT DISTINCT season FROM hist_pbp ORDER BY season")).fetchall()
        seasons_loaded = [r[0] for r in rows]
        print("\nPBP seasons now in DB:", seasons_loaded)

    print("\n✅ Resume complete.")

if __name__ == "__main__":
    main()
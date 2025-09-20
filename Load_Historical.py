# Script for populating DB w/ Docker connection PostgreSQL with initial historical data
# No play by play data
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = Path(r"C:\Users\seanz\VSCode_WS\Sports\NFL_Analytics\data_historic")

PG_USER = "SeanZahller"
PG_PASS = "YvMiTe9!2"
PG_HOST = "localhost"
PG_PORT = 5432   
PG_DB   = "nfl_warehouse"

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

def load_parquet(table: str, file_path: Path, if_exists="replace", chunksize=100_000):
    if not file_path.exists():
        print(f"[skip] {file_path.name} not found")
        return
    print(f"[load] {file_path.name} -> {table}")
    df = pd.read_parquet(file_path)
    if df.empty:
        print(f"[skip] {file_path.name} is empty")
        return
    df.to_sql(table, engine, if_exists=if_exists, index=False, method="multi", chunksize=chunksize)
    print(f"[done] {table}: {len(df):,} rows")

def main():
    with engine.begin() as con:
        ver = con.execute(text("select version()")).scalar()
        print("Connected to:", ver)

    load_parquet("hist_schedules", DATA_DIR / "schedules_2000_2024.parquet", if_exists="replace")
    load_parquet("hist_weekly",    DATA_DIR / "weekly_2000_2024.parquet",    if_exists="replace")
    load_parquet("hist_rosters_seasonal", DATA_DIR / "rosters_seasonal_2000_2024.parquet", if_exists="replace")
    load_parquet("hist_rosters_weekly",   DATA_DIR / "rosters_weekly_available_years.parquet", if_exists="replace")

    with engine.begin() as con:
        for t in ["hist_schedules","hist_weekly","hist_rosters_seasonal","hist_rosters_weekly"]:
            try:
                cnt = con.execute(text(f"select count(*) from {t}")).scalar()
                print(f"[count] {t}: {cnt:,}")
            except Exception as e:
                print(f"[count] {t}: (missing) {e}")

    print("\n✅ Historical load complete (no PBP).")

if __name__ == "__main__":
    main()
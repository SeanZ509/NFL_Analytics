import os
from pathlib import Path
import pandas as pd
import nfl_data_py as nfl

pd.set_option("display.max_columns", None)

DATA_DIR = Path("data_historic")
DATA_DIR.mkdir(exist_ok=True)

SEASONS = list(range(2000, 2025))  

print("Loading schedules...")
schedules = nfl.import_schedules(SEASONS)
print("schedules shape:", schedules.shape)
schedules.to_parquet(DATA_DIR / "schedules_2000_2024.parquet", index=False)
print("Schedules saved.\n")

print("Loading weekly player stats...")
weekly = nfl.import_weekly_data(SEASONS)
print("weekly shape:", weekly.shape)
weekly.to_parquet(DATA_DIR / "weekly_2000_2024.parquet", index=False)
print("Weekly player stats saved.\n")

print("Loading play-by-play (this will take the longest)...")
for season in SEASONS:
    print(f"  -> {season}")
    pbp = nfl.import_pbp_data([season])
    print("     shape:", pbp.shape)
    pbp.to_parquet(DATA_DIR / f"pbp_{season}.parquet", index=False)

print("\nAll data saved to:", DATA_DIR.resolve())
print("Done ✅")
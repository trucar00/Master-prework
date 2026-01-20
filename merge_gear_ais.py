import pandas as pd

# IMPROVEMENT: Should merge also on a month condition, siste fangstdato e.l.

ais_df = pd.read_parquet("Data/AIS/01-10-15.parquet", engine="pyarrow")
gear_df = pd.read_csv("Data/fangstdata_2024_dropped.csv")

ais_df["date_time_utc"] = pd.to_datetime(ais_df["date_time_utc"])
gear_df["Siste fangstdato"] = pd.to_datetime(gear_df["Siste fangstdato"])

t_min = ais_df["date_time_utc"].min()
t_max = ais_df["date_time_utc"].max()

print("AIS window: ", t_min, "->", t_max)

gear_in_window = gear_df.loc[
    gear_df["Siste fangstdato"].between(t_min, t_max, inclusive="both")
].copy()

for name, d in gear_in_window.groupby("Fartøynavn"):
    gear_used = d["Redskap - gruppe"].unique()
    if len(gear_used) > 1:
        print(f"{name}: {gear_used}")

gear_map = (gear_in_window
            .dropna(subset=["Fartøynavn", "Redskap - gruppe"])
            .groupby("Fartøynavn")["Redskap - gruppe"]
            .agg(lambda s: sorted(set(s)))          # list of unique gear types
            .reset_index()
            .rename(columns={"Redskap - gruppe": "gear_types"}))

ais_merged = ais_df.merge(
    gear_map,
    left_on="ship_name",
    right_on="Fartøynavn",
    how="left"
).drop(columns=["Fartøynavn"])

ais_merged["n_gears"] = ais_merged["gear_types"].apply(lambda x: len(x) if isinstance(x, list) else 0)
ais_merged["multiple_gears"] = ais_merged["n_gears"] > 1

ais_merged = ais_merged.dropna(subset=["gear_types"])

#ais_merged.to_csv("Data/AIS_gear/01-10-15.csv", index=False)
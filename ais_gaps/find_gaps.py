import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet("../Data/AIS/whole_month/01clean2.parquet", columns=["mmsi", "date_time_utc", "lon", "lat"], engine="pyarrow")

df["date_time_utc"] = pd.to_datetime(df["date_time_utc"])

first_mmsis = df["mmsi"].drop_duplicates().head(5)

df_small = df[df["mmsi"].isin(first_mmsis)]

threshold = pd.Timedelta(hours=1)

for mmsi, d in df_small.groupby("mmsi"):
    d = d.sort_values(by="date_time_utc")
    d["gap"] = d["date_time_utc"].diff()
    d["large_gap"] = (d["gap"] > threshold)
    has_large_gap = (d["gap"] > threshold).any()
    gap_messages = d.loc[d["large_gap"] == True].copy()
    print(d.shape)
    print(gap_messages.shape)
    print(f"{mmsi}, gap: {has_large_gap}")
    fig = plt.figure(figsize=(10, 8))
    plt.plot(d["lon"], d["lat"], linewidth=1, label="Trajectory")
    plt.scatter(gap_messages["lon"], gap_messages["lat"], s=1.5, color="red", label="Message after gap")
    plt.legend()
    plt.show()
    
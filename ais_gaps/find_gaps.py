import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import matplotlib.colors as colors

df = pd.read_parquet("../Data/AIS/whole_month/01clean2.parquet", columns=["mmsi", "date_time_utc", "lon", "lat"], engine="pyarrow")

df["date_time_utc"] = pd.to_datetime(df["date_time_utc"])

#first_mmsis = df["mmsi"].drop_duplicates().head(20000)

#df_small = df[df["mmsi"].isin(first_mmsis)]

threshold = pd.Timedelta(hours=1)

lons = []
lats = []

#fig, ax = plt.subplots(figsize=(10, 8))

for mmsi, d in df.groupby("mmsi"):
    d = d.sort_values(by="date_time_utc")
    d["gap"] = d["date_time_utc"].diff()
    d["large_gap"] = (d["gap"] > threshold)
    nr_gaps = d["large_gap"].sum()
    gap_messages = d.loc[d["large_gap"] == True].copy()
    lons.extend(gap_messages["lon"].values)
    lats.extend(gap_messages["lat"].values)
    #print(d.shape)
    #print(gap_messages.shape)
    #print(f"{mmsi}, nr of gaps: {nr_gaps}")

    #plt.plot(d["lon"], d["lat"], linewidth=1, label="Trajectory")
    #ax.scatter(gap_messages["lon"], gap_messages["lat"], s=2, color="red")

#plt.legend()

plt.figure(figsize=(10,8))

plt.hist2d(lons, lats, bins=100, cmap="hot", norm=colors.LogNorm())
plt.colorbar(label="Number of AIS gaps")

plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Heatmap of AIS Signal Gaps")
plt.gca().set_aspect('equal', adjustable='box')
plt.show()
    
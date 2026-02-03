import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet("Data/AIS/whole_month/01.parquet", engine="pyarrow")
print(df.columns)
print(df.shape)
print(df["mmsi"].nunique())

df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
df["lat"] = pd.to_numeric(df["lat"], errors="coerce")

counts = (
    df.dropna(subset=["lon", "lat"])
      .groupby("mmsi")
      .size()
      .sort_values(ascending=False)
)

print("MMSI count:", df["mmsi"].nunique())
print("MMSI with >=2 points:", (counts >= 2).sum())
print("MMSI with 1 point:", (counts == 1).sum())
print(counts.head(20))

fig, ax = plt.subplots(figsize=(10,8))

cnt = 0
for name, d in df.groupby("mmsi"):
    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)

ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.show()


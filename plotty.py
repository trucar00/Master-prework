import matplotlib.pyplot as plt
import pandas as pd

#df = pd.read_csv("line_segments_no_label.csv")
df = pd.read_parquet("Data/AIS/whole_month2/02.parquet")
print(df.shape)
print(df.columns)

fig, ax = plt.subplots(figsize=(10,8))

for radio, d in df.groupby("mmsi"):
    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)


ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.show()

""" print(df["segment_id"].nunique())
print(df["trajectory_id"].nunique())

fig, ax = plt.subplots(figsize=(10,8))

for seg, d in df.groupby("segment_id"):
    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)


ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.show() """


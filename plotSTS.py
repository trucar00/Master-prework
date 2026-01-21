import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("Data/STS/ais.csv")

df["date_time_utc"] = pd.to_datetime(df["date_time_utc"])


fig, ax = plt.subplots(figsize=(10,8))

for name, d in df.groupby("mmsi"):
    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)

ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.show()
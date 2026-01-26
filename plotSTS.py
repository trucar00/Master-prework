import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import numpy as np

month = 5

sts = pd.read_csv("Data/STS/fangstdata_2024_sts.csv")
sts["Siste fangstdato"] = pd.to_datetime(sts["Siste fangstdato"])

sts_in_month = sts.loc[sts["Siste fangstdato"].dt.month == month].copy()
print(sts_in_month.head())

radio_giver = sts_in_month["Radiokallesignal (seddel)"].unique()
radio_receiver = sts_in_month["Mottakende fartøy rkal"].unique()

print("Giver:", radio_giver)
print("Receiver:", radio_receiver)

giver_receiver = np.concatenate((radio_giver, radio_receiver))
#print(giver_receiver)

call1 = "LK2407" # only: LFEP
call2 = "LK3928" # only: LJZO

df = pd.read_parquet(f"Data/AIS/all/{month:02d}.parquet", engine="pyarrow")
print("In ais:", df["callsign"].unique())

v1 = df.loc[df["callsign"] == call1].copy()
v2 = df.loc[df["callsign"] == call2].copy()
print(v1.shape, v2.shape)


v1["date_time_utc"] = pd.to_datetime(v1["date_time_utc"])
v2["date_time_utc"] = pd.to_datetime(v2["date_time_utc"])
v1 = v1.sort_values(by="date_time_utc")
v2 = v2.sort_values(by="date_time_utc")

fig = plt.figure(figsize=(10,8))
plt.plot(v1["lon"], v1["lat"], linewidth=1.0, alpha=0.7, label=call1)
plt.plot(v2["lon"], v2["lat"], linewidth=1.0, alpha=0.7, label=call2)
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.legend()
plt.show()

""" fig, ax = plt.subplots(figsize=(10,8))

for name, d in df.groupby("mmsi"):
    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)

ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.show() """
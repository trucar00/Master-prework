import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("Data/AIS_gear/merged.csv")


df_trawl = df[df["gear_types"].astype(str).str.contains("Krokredskap", na=False)] # Trål, Krokredskap, Bur og ruser, Garn, Snurrevad, Not

for name, d in df_trawl.groupby("ship_name"):
    gear_used = d["gear_types"].unique()
    print(f"{name}: {gear_used}")

#print(df_trawl.head())

print(df_trawl["ship_name"].unique())

fig, ax = plt.subplots(figsize=(10,8))

for name, d in df_trawl.groupby("ship_name"):
    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)

ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.show()

# v1 = df_trawl.loc[df_trawl["ship_name"] == "EMMA"].copy()
# v1["date_time_utc"] = pd.to_datetime(v1["date_time_utc"])
# v1 = v1.sort_values(by="date_time_utc")

# fig = plt.figure()
# plt.plot(v1["lon"], v1["lat"])
# plt.show()
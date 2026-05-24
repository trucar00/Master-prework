import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# needs more work, take the strict: only reported longlining. 

df = pd.read_parquet("ais_ers_labels_clean_01_04.parquet")
GEAR = "Trål"

translatation = {
    "Trål": "trawlers",
    "Krokredskap": "hooked gear (liners)",
    "Snurrevad": "danish seiners",
    "Not": "purse seiners",
    "Garn": "gillnetters",
}

ALLOWED = [GEAR, "no_fishing"]

valid_ids = df.groupby("trajectory_id")["report"] \
    .apply(lambda x: (GEAR in x.values) and x.isin(ALLOWED).all())

valid_ids = valid_ids[valid_ids].index

df = df[df["trajectory_id"].isin(valid_ids)].reset_index(drop=True)

print(df["report"].unique())
print(df.columns)

df["date_time_utc"] = pd.to_datetime(df["date_time_utc"])

df = df.dropna(subset=["speed", "lon", "lat"])
df = df[(df["speed"] > 0) & (df["speed"] < 20)]

plt.figure(figsize=(8,5))
sns.histplot(df["speed"], bins=50, kde=True)
plt.title(f"Overall speed distribution ({translatation[GEAR]})")
plt.xlabel("Speed")
plt.ylabel("Count")
plt.show()

""" fig, ax = plt.subplots(figsize=(10,8))

for name, d in df.groupby("mmsi"):
    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)

ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.show() """

""" fig, ax = plt.subplots(figsize=(10,8))

for mmsi, d in df.groupby("mmsi"):
    d = d.sort_values("date_time_utc")
    
    # Plot trajectory (line)
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.5, color="gray")

# Overlay high-speed points
fast = df[df["speed"] > 7.5]

ax.scatter(fast["lon"], fast["lat"], 
           color="blue", s=3, label="Speed > 7.5")

ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
ax.legend()
plt.show() """

""" slow = df[df["speed"] <= 7]
fast = df[df["speed"] > 7]

fig, ax = plt.subplots(figsize=(10,8))

ax.scatter(slow["lon"], slow["lat"], s=3, alpha=0.3, label="≤ 7.5")
#ax.scatter(fast["lon"], fast["lat"], s=3, alpha=0.8, label="> 7.5")

ax.legend()
plt.show() """

# for the mosrt part, liners steam and set the lines at approx the same speed. Hauling is substanitally lower. 
#

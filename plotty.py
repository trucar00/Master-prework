import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

#df = pd.read_csv("line_segments_no_label.csv")
# Load both datasets
df_new = pd.read_parquet("Data/AIS/whole_month_new/01_2024.parquet")
df_old = pd.read_csv("Data/AIS/whole_month_new/01.csv")

# Use ONE dataset to sample IDs (important!)
traj_ids = df_old["trajectory_id"].unique()

rng = np.random.default_rng(42)
sampled_traj_ids = rng.choice(
    traj_ids,
    size=int(0.1 * len(traj_ids)),
    replace=False
)

# Filter both datasets using SAME IDs
df_old_sample = df_old[df_old["trajectory_id"].isin(sampled_traj_ids)]
df_new_sample = df_new[df_new["trajectory_id"].isin(sampled_traj_ids)]

# Create side-by-side plots
fig, axes = plt.subplots(ncols=2, figsize=(14, 6), sharex=True, sharey=True)

datasets = [("Old", df_old_sample), ("New", df_new_sample)]

for ax, (title, df_plot) in zip(axes, datasets):
    for _, d in df_plot.groupby("mmsi"):
        d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
        d = d.sort_values(by="date_time_utc")
        ax.scatter(d["lon"], d["lat"], s=1, alpha=0.7)

    ax.set_title(title)
    ax.set_xlabel("Longitude")

axes[0].set_ylabel("Latitude")

plt.tight_layout()
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


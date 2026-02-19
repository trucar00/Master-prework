import pandas as pd
import numpy as np


path = "../Data/close_pairs2.csv"

df = pd.read_csv(path)
df["time_stamp"] = pd.to_datetime(df["time_stamp"])
df = df[(df["speed1"] > 0.25) & (df["speed2"] > 0.25)].copy()

df[["mmsi1", "mmsi2"]] = np.sort(df[["mmsi1", "mmsi2"]].values, axis=1)
df = df.sort_values(["mmsi1", "mmsi2", "time_stamp"]).reset_index(drop=True)
df["dt_prev"] = df.groupby(["mmsi1", "mmsi2"])["time_stamp"].diff()
df["is_consecutive"] = df["dt_prev"].eq(pd.Timedelta("10min"))

print(df.head(10))

# Create run IDs
df["run_id"] = df.groupby(["mmsi1", "mmsi2"])["is_consecutive"] \
                 .transform(lambda s: (~s).cumsum())

# Aggregate runs with start and end time
runs = (
    df.groupby(["mmsi1", "mmsi2", "run_id"])
      .agg(
          start_time=("time_stamp", "min"),
          end_time=("time_stamp", "max"),
          n_points=("time_stamp", "size"),
      )
      .reset_index()
)

# Keep only runs with >= 2 timestamps
runs = runs[runs["n_points"] >= 2]
runs = runs.sort_values(by="n_points", ascending=False)

print(runs.head())
print("Number of streaks:", len(runs))

runs.to_csv("consecutive.csv", index=False)

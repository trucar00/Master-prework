"""
Find vessel pairs that are within D_METERS for at least MIN_DURATION minutes.

Output columns:
mmsi1, mmsi2, start_time, out_time

Notes:
- Uses spatiotemporal pruning:
  1) read AIS in lat/lon tiles with pyarrow filters
  2) time-bin inside each tile
  3) BallTree (haversine) to find pairs within distance in each time bin
  4) stitch consecutive time bins into continuous "close" intervals

- "close in a bin" is evaluated using ONE point per vessel per bin (the last message in the bin).
  This is conservative (fewer false positives, may miss some encounters if AIS is sparse).
"""

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.neighbors import BallTree

# -----------------------------
# USER SETTINGS
# -----------------------------
AIS_PATH = "../Data/AIS/whole_month/01clean.parquet"

# Region tiling (degrees)
REGION_LAT_MIN = 55     # if you actually mean north of 62, set to 62
REGION_LAT_MAX = 90
REGION_LON_MIN = -10
REGION_LON_MAX = 45

N_LAT_TILES = 5
N_LON_TILES = 5

# Proximity + time requirements
D_METERS = 30
BIN = "2min"                # detection resolution (recommend 1–2min for 10min persistence)
MIN_DURATION_MINUTES = 10   # required close time

# Optional pruning (uncomment if you want)
# MAX_SPEED_KTS = 2.0
# FISHING_TYPE_CODE = 30

# -----------------------------
# CONSTANTS
# -----------------------------
EARTH_R = 6_371_000.0
RADIUS_RAD = D_METERS / EARTH_R
BIN_TD = pd.Timedelta(BIN)
MIN_BINS = int(np.ceil(MIN_DURATION_MINUTES / (BIN_TD.total_seconds() / 60.0)))

# -----------------------------
# HELPERS
# -----------------------------
def pairs_in_bin(g: pd.DataFrame) -> pd.DataFrame:
    """
    Return pairs (mmsi_1, mmsi_2) that are within D_METERS in this time bin.
    Uses one point per vessel per bin (last message).
    """
    if len(g) < 2:
        return pd.DataFrame(columns=["time_bin", "mmsi_1", "mmsi_2"])

    # One point per vessel per bin (keeps compute bounded)
    g = g.sort_values("date_time_utc").drop_duplicates("mmsi", keep="last")
    if len(g) < 2:
        return pd.DataFrame(columns=["time_bin", "mmsi_1", "mmsi_2"])

    coords = np.deg2rad(g[["lat", "lon"]].to_numpy())
    tree = BallTree(coords, metric="haversine")
    neigh = tree.query_radius(coords, r=RADIUS_RAD)

    mmsi = g["mmsi"].to_numpy()
    tbin = g["time_bin"].iloc[0]

    out = []
    for i, js in enumerate(neigh):
        for j in js:
            if j <= i:
                continue
            out.append((tbin, int(mmsi[i]), int(mmsi[j])))

    return pd.DataFrame(out, columns=["time_bin", "mmsi_1", "mmsi_2"])


def stitch_runs(pairs_all: pd.DataFrame) -> pd.DataFrame:
    """
    Convert per-bin proximity detections into continuous runs.
    Returns mmsi1, mmsi2, start_time, out_time (out_time is end of run).
    """
    if pairs_all.empty:
        return pd.DataFrame(columns=["mmsi1", "mmsi2", "start_time", "out_time"])

    # Normalize ordering so (A,B) == (B,A)
    pairs_all[["mmsi_1", "mmsi_2"]] = np.sort(
        pairs_all[["mmsi_1", "mmsi_2"]].values, axis=1
    )

    # Drop duplicate detections that can happen if you later add buffer/overlap logic
    pairs_all = pairs_all.drop_duplicates(subset=["time_bin", "mmsi_1", "mmsi_2"])

    pairs_all = pairs_all.sort_values(["mmsi_1", "mmsi_2", "time_bin"]).reset_index(drop=True)

    # Identify gaps larger than one bin => new run
    dt = pairs_all.groupby(["mmsi_1", "mmsi_2"])["time_bin"].diff()
    new_run = (dt.isna()) | (dt > BIN_TD)
    pairs_all["run_id"] = new_run.groupby([pairs_all["mmsi_1"], pairs_all["mmsi_2"]]).cumsum()

    runs = (
        pairs_all.groupby(["mmsi_1", "mmsi_2", "run_id"])
        .agg(start_time=("time_bin", "min"),
             last_bin=("time_bin", "max"),
             bins=("time_bin", "nunique"))
        .reset_index()
    )

    # Duration in bins -> require at least MIN_DURATION_MINUTES
    runs = runs[runs["bins"] >= MIN_BINS].copy()

    # out_time = end of the last bin (exclusive)
    runs["out_time"] = runs["last_bin"] + BIN_TD

    runs = runs.rename(columns={"mmsi_1": "mmsi1", "mmsi_2": "mmsi2"})
    return runs[["mmsi1", "mmsi2", "start_time", "out_time"]].sort_values(
        ["start_time", "mmsi1", "mmsi2"]
    ).reset_index(drop=True)


# -----------------------------
# MAIN
# -----------------------------
def main():
    lat_edges = np.linspace(REGION_LAT_MIN, REGION_LAT_MAX, N_LAT_TILES + 1)
    lon_edges = np.linspace(REGION_LON_MIN, REGION_LON_MAX, N_LON_TILES + 1)

    all_pairs_global = []

    for i in range(N_LON_TILES):
        lon_min, lon_max = float(lon_edges[i]), float(lon_edges[i + 1])
        for j in range(N_LAT_TILES):
            lat_min, lat_max = float(lat_edges[j]), float(lat_edges[j + 1])

            table = pq.read_table(
                AIS_PATH,
                columns=["mmsi", "lat", "lon", "date_time_utc", "speed", "ship_type"],
                filters=[
                    ("lat", ">=", lat_min), ("lat", "<", lat_max),
                    ("lon", ">=", lon_min), ("lon", "<", lon_max),
                    # Optional pruning:
                    # ("speed", "<", MAX_SPEED_KTS),
                    # ("ship_type", "==", FISHING_TYPE_CODE),
                ],
            )

            df_tile = table.to_pandas()
            if df_tile.empty:
                continue

            df_tile["date_time_utc"] = pd.to_datetime(df_tile["date_time_utc"], utc=True, errors="coerce")
            df_tile = df_tile.dropna(subset=["date_time_utc", "lat", "lon", "mmsi"])
            if df_tile.empty:
                continue

            df_tile["time_bin"] = df_tile["date_time_utc"].dt.floor(BIN)

            # Build pairs for each time bin in this tile
            tile_pairs = []
            for tbin, g in df_tile.groupby("time_bin"):
                pairs_df = pairs_in_bin(g)
                if not pairs_df.empty:
                    tile_pairs.append(pairs_df)

            if tile_pairs:
                all_pairs_global.append(pd.concat(tile_pairs, ignore_index=True))

    pairs_all = (
        pd.concat(all_pairs_global, ignore_index=True)
        if all_pairs_global
        else pd.DataFrame(columns=["time_bin", "mmsi_1", "mmsi_2"])
    )

    events = stitch_runs(pairs_all)

    # Output
    print(events)
    # Save if you want:
    events.to_csv("close_pairs_10min.csv", index=False)
    print("\nSaved:", "close_pairs_10min.csv")


if __name__ == "__main__":
    main()

# only gave like 4 cases of sts, needs to be examined further.
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
from sklearn.neighbors import BallTree
import math


AIS_PATH = "../Data/AIS/whole_month/01clean2.parquet"

REGION_LAT = 55 # We want all vessels north of 62 degrees north
REGION_LON_EAST = 45
REGION_LON_WEST = -10

D_METERS = 50
EARTH_R = 6_371_000
RADIUS_RAD = D_METERS / EARTH_R

BIN = "10min"
# Do not interpolate across gaps larger than this
MAX_INTERP_GAP = pd.Timedelta("15min")
MIN_POINTS_PER_VESSEL = 3


lat_range = np.linspace(REGION_LAT, 90, 5)
lon_range = np.linspace(REGION_LON_WEST, REGION_LON_EAST, 5)

def downsample(df, step="10min"):
    # Ensure datetime format
    df["date_time_utc"] = pd.to_datetime(df["date_time_utc"])
    df = df.sort_values(["mmsi", "date_time_utc"])
    df = df.set_index("date_time_utc")
    #print(df.head())

    def resample_and_interpolate(g, step="10min", max_gap=pd.Timedelta("15min")):
        g = g.sort_index()

        # IMPORTANT: make index unique (AIS often has duplicates)
        if not g.index.is_unique:
            num_cols = g.select_dtypes(include="number").columns
            other_cols = [c for c in g.columns if c not in num_cols]

            g_num = g[num_cols].groupby(level=0).mean()
            g_other = g[other_cols].groupby(level=0).first()
            g = pd.concat([g_num, g_other], axis=1).sort_index()

        t0, t1 = g.index.min(), g.index.max()

        # regular grid
        grid = pd.date_range(t0.floor(step), t1.ceil(step), freq=step)

        # union index (now safe because g.index is unique)
        g_res = g.reindex(g.index.union(grid)).sort_index()

        # mark real observations (before interpolation)
        is_obs = g_res["lon"].notna() & g_res["lat"].notna()

        # interpolate numeric columns by time
        num_cols = g_res.select_dtypes(include="number").columns
        g_res[num_cols] = g_res[num_cols].interpolate(method="time", limit_area="inside")

        # blank out interpolation inside big gaps between consecutive observations
        obs_times = g_res.index[is_obs]
        if len(obs_times) >= 2:
            gaps = obs_times.to_series().diff()
            big_starts = obs_times[1:][gaps.iloc[1:].values > max_gap]

            for t_start in big_starts:
                t_prev = obs_times[obs_times.get_loc(t_start) - 1]
                mask = (g_res.index > t_prev) & (g_res.index < t_start)
                g_res.loc[mask, num_cols] = np.nan

        # keep only grid timestamps inside original span
        g_res = g_res.loc[g_res.index.isin(grid)]
        g_res = g_res.loc[(g_res.index >= t0) & (g_res.index <= t1)]

        # fill non-numeric (safe even if callsign missing)
        for col in ["callsign", "day"]:
            if col in g_res.columns:
                g_res[col] = g_res[col].ffill().bfill().infer_objects(copy=False)

        return g_res


    resampled_parts = []

    for mmsi_val, g in df.groupby("mmsi", sort=False):
        g_res = resample_and_interpolate(g, step=step, max_gap=MAX_INTERP_GAP)
        g_res["mmsi"] = mmsi_val
        resampled_parts.append(g_res)

    resampled = pd.concat(resampled_parts)
    resampled.index.name = "date_time_utc"
    resampled = resampled.reset_index()   # creates resampled["date_time_utc"]

    return resampled

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000 # Radius of the earth in meters

    dLat = (lat2 - lat1) * math.pi / 180.0
    dLon = (lon2 - lon1) * math.pi / 180.0

    # convert to radians
    lat1 = (lat1) * math.pi / 180.0
    lat2 = (lat2) * math.pi / 180.0

    # apply formulae
    a = (pow(np.sin(dLat / 2), 2) + 
         pow(np.sin(dLon / 2), 2) * 
             np.cos(lat1) * np.cos(lat2))
    
    c = 2 * np.arcsin(np.sqrt(a))

    dist = R * c

    return dist

def pairs_within_radius(d_time, r_rad=RADIUS_RAD):
    x = d_time[["mmsi", "callsign", "lat", "lon"]].dropna()
    if len(x) < 2:
        return set()

    coords_rad = np.deg2rad(x[["lat", "lon"]].to_numpy())
    tree = BallTree(coords_rad, metric="haversine")

    # neighbors[i] = array of indices in x within radius of i (includes i itself)
    neighbors = tree.query_radius(coords_rad, r=r_rad)

    mmsis = x["mmsi"].to_numpy(dtype=np.int64)
    pairs = set()

    for i, neigh in enumerate(neighbors):
        for j in neigh:
            if j <= i:
                continue  # avoid self + duplicates
            m1 = int(mmsis[i])
            m2 = int(mmsis[j])
            if m1 != m2:
                pairs.add((m1, m2) if m1 < m2 else (m2, m1))

    return pairs


close_pairs = []
for i in range(len(lon_range)-1):
    for j in range(len(lat_range)-1):


        lon_min, lon_max = lon_range[i], lon_range[i+1]
        lat_min, lat_max = lat_range[j], lat_range[j+1]
        table = pq.read_table(
            AIS_PATH,
            columns=["mmsi", "lat", "lon", "date_time_utc", "speed", "cog", "ship_type", "callsign"],
            filters=[
                ("lat", ">=", lat_min),
                ("lat", "<",  lat_max),
                ("lon", ">=", lon_min),
                ("lon", "<",  lon_max),
            ],
        )

        df_tile = table.to_pandas()


        if df_tile.shape[0] == 0: # no messages within this tile
            continue # skip
        
        else:
            df_tile["date_time_utc"] = pd.to_datetime(df_tile["date_time_utc"])
            df_tile["day"] = df_tile["date_time_utc"].dt.floor("D")

            for day, df_day in df_tile.groupby("day"):
                df_resampled = downsample(df_day)
                for time_stamp, d_time in df_resampled.groupby("date_time_utc"):
                    #print(time_stamp, ": ", d_time.shape)
                    
                    # Check distance between each point (TEST)
                    #print(d_time.iloc[0]["mmsi"], d_time.iloc[1]["mmsi"])
                    pairs = pairs_within_radius(d_time)

                    if pairs:
                        #print(time_stamp, "pairs:", len(pairs))
                        # print a few
                        #print(pairs)
                        for k, (m1, m2) in enumerate(sorted(pairs)):
                            dm1 = d_time.loc[d_time["mmsi"] == m1].copy()
                            dm2 = d_time.loc[d_time["mmsi"] == m2].copy()
                            lon1, lat1 = dm1["lon"].iloc[0], dm1["lat"].iloc[0]
                            lon2, lat2 = dm2["lon"].iloc[0], dm2["lat"].iloc[0]
                            callsign1 = dm1["callsign"].iloc[0]
                            callsign2 = dm2["callsign"].iloc[0]
                            speed1 = dm1["speed"].mean()
                            speed2 = dm2["speed"].mean()
                            dist = haversine(lat1, lon1, lat2, lon2)
                            append_dict = {
                                "mmsi1": m1,
                                "mmsi2": m2,
                                "callsign1": callsign1,
                                "callsign2": callsign2,
                                "time_stamp": time_stamp,
                                "lon1": lon1,
                                "lat1": lat1,
                                "lon2": lon2,
                                "lat2": lat2,
                                "distance": dist,
                                "speed1": speed1,
                                "speed2": speed2
                            }
                            close_pairs.append(append_dict)
                            #print(f"Distance between {m1} and {m2} at time {time_stamp}: {dist:.2f}, speed: {speed1}, {speed2}")

                            #print(" ", m1, m2)


df_close_pairs = pd.DataFrame(close_pairs)
dups = df_resampled.duplicated(subset=["mmsi", "date_time_utc"]).sum()
print("duplicates (mmsi,time) in df_resampled:", dups)

df_close_pairs.to_csv("close_pairs.csv", index=False)  
# Notes
# Remove stationary doesnt work perfectly, think this works now
# Finds cases where dist < 50, need to find consecutive 
# Interpolates over removed stationary parts of trajectories -> fixed i think but take a look
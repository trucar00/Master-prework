import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
from sklearn.neighbors import BallTree
import math

AIS_PATH = "../Data/AIS/whole_month/01clean.parquet"

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

    def resample_and_interpolate(g):
        # Resample regularly
        t0, t1 = g.index.min(), g.index.max()
        g_res = g.resample(step, origin="start_day").first()

        # Interpolate only numeric columns (lon, lat, speed, etc.)
        num_cols = g_res.select_dtypes(include="number").columns
        g_res[num_cols] = g_res[num_cols].interpolate(method="linear", limit_area="inside")

        g_res = g_res.loc[(g_res.index >= t0) & (g_res.index <= t1)]
        # Fill remaining NaNs (like mmsi, ship_name) via forward/backward fill
        g_res[["callsign", "day"]] = g_res[["callsign", "day"]].ffill().bfill()
        #print(g_res.head())
        return g_res

    resampled_parts = []

    for mmsi_val, g in df.groupby("mmsi", sort=False):
        g_res = resample_and_interpolate(g)
        g_res["mmsi"] = mmsi_val
        resampled_parts.append(g_res)

    resampled = pd.concat(resampled_parts)
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
                        for k, (m1, m2) in enumerate(sorted(pairs)):
                            dm1 = d_time.loc[d_time["mmsi"] == m1].copy()
                            dm2 = d_time.loc[d_time["mmsi"] == m2].copy()
                            lon1, lat1 = dm1["lon"].iloc[0], dm1["lat"].iloc[0]
                            lon2, lat2 = dm2["lon"].iloc[0], dm2["lat"].iloc[0]
                            speed1 = dm1["speed"].iloc[0]
                            speed2 = dm2["speed"].iloc[0]
                            dist = haversine(lat1, lon1, lat2, lon2)
                            print(f"Distance between {m1} and {m2} at time {time_stamp}: {dist:.2f}, speed: {speed1}, {speed2}")

                            if k >= 5:
                                break
                            #print(" ", m1, m2)

                    """ if d_time.shape[0] > 1:
                        lon0, lat0 = d_time.iloc[0]["lon"], d_time.iloc[0]["lat"]
                        lon1, lat1 = d_time.iloc[1]["lon"], d_time.iloc[1]["lat"]
                        dist = haversine(lat1=lat0, lon1=lon0, lat2=lat1, lon2=lon1)
                        print(f"Distance between {d_time.iloc[0]["mmsi"]} and {d_time.iloc[1]["mmsi"]} at time {time_stamp}: {dist:.2f}")
                    else:
                        print("Not big enough") """



                """ fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8), sharex=True, sharey=True)

                # --- Original AIS points ---
                for mmsi, d in df_day.groupby("mmsi"):
                    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
                    d = d.sort_values("date_time_utc")
                    ax1.scatter(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)

                ax1.set_title(f"Original AIS, day: {day}")
                ax1.set_xlabel("Longitude")
                ax1.set_ylabel("Latitude")

                # --- Resampled AIS ---
                for mmsi, d in df_resampled.groupby("mmsi"):
                    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
                    d = d.sort_values("date_time_utc")
                    ax2.scatter(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)

                ax2.set_title(f"Resampled AIS, day: {day}")
                ax2.set_xlabel("Longitude")

                plt.tight_layout()
                plt.show() """

        
# Notes
# Remove stationary doesnt work perfectly
# Finds cases where dist < 50, need to find consecutive 
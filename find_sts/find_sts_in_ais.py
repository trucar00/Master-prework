import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
from sklearn.neighbors import BallTree

AIS_PATH = "../Data/AIS/whole_month/01clean.parquet"

REGION_LAT = 55 # We want all vessels north of 62 degrees north
REGION_LON_EAST = 45
REGION_LON_WEST = -10

D_METERS = 30
EARTH_R = 6_371_000
RADIUS_RAD = D_METERS / EARTH_R

BIN = "10min"

def pairs_in_bin(g: pd.DataFrame) -> pd.DataFrame:
    # Ensure enough points
    if len(g) < 2:
        return pd.DataFrame(columns=["time_bin", "mmsi_1", "mmsi_2"])

    # Optional: reduce to one point per vessel in the bin (keeps problem size sane)
    # choose the last message in the bin for each mmsi
    g = g.sort_values("date_time_utc").drop_duplicates("mmsi", keep="last")

    if len(g) < 2:
        return pd.DataFrame(columns=["time_bin", "mmsi_1", "mmsi_2"])

    coords = np.deg2rad(g[["lat", "lon"]].to_numpy())
    tree = BallTree(coords, metric="haversine")
    neigh = tree.query_radius(coords, r=RADIUS_RAD)

    mmsi = g["mmsi"].to_numpy()

    out = []
    # Build unique pairs (i < j)
    for i, js in enumerate(neigh):
        for j in js:
            if j <= i:
                continue
            out.append((g["time_bin"].iloc[0], int(mmsi[i]), int(mmsi[j])))

    return pd.DataFrame(out, columns=["time_bin", "mmsi_1", "mmsi_2"])


lat_range = np.linspace(REGION_LAT, 90, 5)
lon_range = np.linspace(REGION_LON_WEST, REGION_LON_EAST, 5)

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
            #print(df_tile["date_time_utc"].min(), df_tile["date_time_utc"].max())
            df_tile["time_bin"] = df_tile["date_time_utc"].dt.floor("10min")
            
            all_pairs = []
            for tbin, g in df_tile.groupby("time_bin"):
                pairs_df = pairs_in_bin(g)
                if not pairs_df.empty:
                    all_pairs.append(pairs_df)
                

            if all_pairs:
                pairs = pd.concat(all_pairs, ignore_index=True)
            else:
                pairs = pd.DataFrame(columns=["time_bin", "mmsi_1", "mmsi_2"])
            
            pairs[["mmsi_1", "mmsi_2"]] = np.sort(
                pairs[["mmsi_1", "mmsi_2"]].values,
                axis=1
            )
            print(pairs)


#print(lat_range)

        """ fig, ax = plt.subplots(figsize=(10, 8))
        for mmsi, d in df_tile.groupby("mmsi"):
            d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
            d = d.sort_values(by="date_time_utc")
            ax.scatter(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)
        
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        plt.show() """

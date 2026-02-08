import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from sklearn.neighbors import BallTree

# -----------------------------
# User parameters
# -----------------------------
AIS_PATH = "../Data/AIS/whole_month/01clean.parquet"

REGION_LAT = 55
REGION_LON_EAST = 45
REGION_LON_WEST = -10

# Distance threshold for "close"
D_METERS = 50
EARTH_R = 6_371_000
RADIUS_RAD = D_METERS / EARTH_R  # for haversine BallTree

# How long they must be continuously close
MIN_DURATION = pd.Timedelta("10min")

# Common time grid after resampling
RESAMPLE_FREQ = "1min"

# Do not interpolate across gaps larger than this
MAX_INTERP_GAP = pd.Timedelta("15min")

# Optional: ignore vessels with too few points in tile (before resampling)
MIN_POINTS_PER_VESSEL = 3

# Tile grid
N_TILES_LAT = 5
N_TILES_LON = 5

# -----------------------------
# Helpers
# -----------------------------
def _pair_key(a: int, b: int) -> tuple[int, int]:
    return (a, b) if a < b else (b, a)

def resample_vessels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resample each MMSI to RESAMPLE_FREQ using time interpolation for lat/lon.
    Interpolation only inside data range and limited by MAX_INTERP_GAP.
    """
    df = df.copy()
    df["date_time_utc"] = pd.to_datetime(df["date_time_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["mmsi", "lat", "lon", "date_time_utc"])
    df = df.sort_values(["mmsi", "date_time_utc"])

    out_parts = []
    # number of steps we allow interpolation across
    max_steps = int(MAX_INTERP_GAP / pd.Timedelta(RESAMPLE_FREQ))

    for mmsi, g in df.groupby("mmsi", sort=False):
        if len(g) < MIN_POINTS_PER_VESSEL:
            continue

        g = g.drop_duplicates(subset=["date_time_utc"])
        g = g.set_index("date_time_utc")[["lat", "lon"]].sort_index()

        # Build the per-vessel grid (you can also use a global tile grid; per-vessel is cheaper)
        start = g.index.min().floor(RESAMPLE_FREQ)
        end = g.index.max().ceil(RESAMPLE_FREQ)
        idx = pd.date_range(start, end, freq=RESAMPLE_FREQ, tz="UTC")

        r = g.reindex(idx)

        # --- FIX: clamp interpolation limit to series length (prevents sliding_window_view crash)
        # r has length = len(idx). If limit >= len(r), pandas/numpy can crash.
        safe_limit = None
        if max_steps is not None:
            safe_limit = max(0, min(max_steps, len(r) - 1))
            # If there's 0 or 1 sample, interpolation is meaningless anyway
            if safe_limit == 0:
                # still allow "inside" interpolation if there are no gaps (won't fill anything)
                pass

        r[["lat", "lon"]] = r[["lat", "lon"]].interpolate(
            method="time",
            limit=safe_limit,
            limit_area="inside",
        )


        r = r.dropna(subset=["lat", "lon"])
        if r.empty:
            continue

        r = r.reset_index().rename(columns={"index": "t"})
        r["mmsi"] = mmsi
        out_parts.append(r[["t", "mmsi", "lat", "lon"]])

    if not out_parts:
        return pd.DataFrame(columns=["t", "mmsi", "lat", "lon"])

    return pd.concat(out_parts, ignore_index=True)

def find_close_intervals(resampled: pd.DataFrame) -> pd.DataFrame:
    """
    For each timestamp, find pairs within distance.
    Then track continuous 'close' runs; output runs >= MIN_DURATION.
    """
    if resampled.empty:
        return pd.DataFrame(columns=["mmsi1", "mmsi2", "start_time", "out_time"])

    resampled = resampled.sort_values(["t", "mmsi"]).reset_index(drop=True)

    # Active runs: key -> (start_time, last_time)
    active: dict[tuple[int, int], tuple[pd.Timestamp, pd.Timestamp]] = {}
    results = []

    freq_td = pd.Timedelta(RESAMPLE_FREQ)
    # allow a little tolerance (e.g. missing one tick because of dropna)
    cont_tol = freq_td * 1.5

    for t, g in resampled.groupby("t", sort=True):
        if len(g) < 2:
            # no pairs possible; finalize anything that doesn't get continued
            continue

        mmsis = g["mmsi"].to_numpy()
        coords = np.deg2rad(g[["lat", "lon"]].to_numpy())  # radians for haversine

        tree = BallTree(coords, metric="haversine")
        neighbors = tree.query_radius(coords, r=RADIUS_RAD, return_distance=False)

        seen_pairs = set()
        for i, nbrs in enumerate(neighbors):
            # nbrs includes i itself
            for j in nbrs:
                if j <= i:
                    continue
                key = _pair_key(int(mmsis[i]), int(mmsis[j]))
                seen_pairs.add(key)

        # Update active runs with seen pairs
        # 1) continue / start pairs we see at time t
        for key in seen_pairs:
            if key in active:
                start_t, last_t = active[key]
                # if time is continuous, extend; else close and restart
                if (t - last_t) <= cont_tol:
                    active[key] = (start_t, t)
                else:
                    # finalize old run
                    if (last_t - start_t) >= MIN_DURATION:
                        results.append((key[0], key[1], start_t, last_t))
                    active[key] = (t, t)
            else:
                active[key] = (t, t)

        # 2) finalize pairs not seen at time t (they ended before t)
        # We don't instantly close them here, because they might just be missing due to interpolation dropouts.
        # But if they don't show up again, they'll be closed at the end anyway.
        # If you want stricter closure, uncomment below:
        #
        # missing = [k for k in active.keys() if k not in seen_pairs and (t - active[k][1]) > cont_tol]
        # for k in missing:
        #     start_t, last_t = active.pop(k)
        #     if (last_t - start_t) >= MIN_DURATION:
        #         results.append((k[0], k[1], start_t, last_t))

    # Finalize remaining active runs
    for key, (start_t, last_t) in active.items():
        if (last_t - start_t) >= MIN_DURATION:
            results.append((key[0], key[1], start_t, last_t))

    if not results:
        return pd.DataFrame(columns=["mmsi1", "mmsi2", "start_time", "out_time"])

    out = pd.DataFrame(results, columns=["mmsi1", "mmsi2", "start_time", "out_time"])
    out = out.sort_values(["start_time", "mmsi1", "mmsi2"]).drop_duplicates(ignore_index=True)
    return out

# -----------------------------
# Main: loop tiles
# -----------------------------
def main():
    lat_range = np.linspace(REGION_LAT, 90, N_TILES_LAT)
    lon_range = np.linspace(REGION_LON_WEST, REGION_LON_EAST, N_TILES_LON)

    all_hits = []

    for i in range(len(lon_range) - 1):
        for j in range(len(lat_range) - 1):
            lon_min, lon_max = lon_range[i], lon_range[i + 1]
            lat_min, lat_max = lat_range[j], lat_range[j + 1]

            table = pq.read_table(
                AIS_PATH,
                columns=["mmsi", "lat", "lon", "date_time_utc"],
                filters=[
                    ("lat", ">=", float(lat_min)),
                    ("lat", "<",  float(lat_max)),
                    ("lon", ">=", float(lon_min)),
                    ("lon", "<",  float(lon_max)),
                ],
            )
            df_tile = table.to_pandas()

            if df_tile.empty:
                continue

            # Resample/interpolate per vessel
            resampled = resample_vessels(df_tile)

            # Find close intervals in this tile
            hits = find_close_intervals(resampled)
            if not hits.empty:
                hits["tile_lon_min"] = lon_min
                hits["tile_lon_max"] = lon_max
                hits["tile_lat_min"] = lat_min
                hits["tile_lat_max"] = lat_max
                all_hits.append(hits)

            print(
                f"Tile lon[{lon_min:.2f},{lon_max:.2f}) lat[{lat_min:.2f},{lat_max:.2f}) "
                f"raw={len(df_tile):,} resampled={len(resampled):,} hits={len(hits):,}"
            )

    if not all_hits:
        print("No close intervals found.")
        return

    out = pd.concat(all_hits, ignore_index=True)

    # Keep only the requested columns (drop tile info if you want)
    out_simple = out[["mmsi1", "mmsi2", "start_time", "out_time"]].copy()

    # Optional: if the same event appears in overlapping tiles, you can dedupe more aggressively here.
    out_simple = out_simple.sort_values(["start_time", "mmsi1", "mmsi2"]).drop_duplicates(ignore_index=True)

    out_simple.to_csv("close_pairs_intervals.csv", index=False)
    print(f"\nSaved: close_pairs_intervals.csv  (rows={len(out_simple):,})")

if __name__ == "__main__":
    main()

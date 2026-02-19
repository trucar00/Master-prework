import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import os

# --- your functions (unchanged) ---
def haversine_m(lon1, lat1, lon2, lat2):
    R = 6371000.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlmb = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlmb / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c


def animate_sts_with_distance(d_rec, d_giv, title="", step="2min",
                              close_threshold_m=None, save_dir="saved_sts",
                              fps=30, dpi=150):
    d_rec = d_rec.sort_values("date_time_utc").dropna(subset=["lon","lat"]).copy()
    d_giv = d_giv.sort_values("date_time_utc").dropna(subset=["lon","lat"]).copy()
    if d_rec.empty or d_giv.empty:
        print("Empty track(s), nothing to animate.")
        return

    t0 = max(d_rec["date_time_utc"].min(), d_giv["date_time_utc"].min())
    t1 = min(d_rec["date_time_utc"].max(), d_giv["date_time_utc"].max())
    if t0 >= t1:
        print("No overlapping time window between the two tracks.")
        return

    times = pd.date_range(t0, t1, freq=step)

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    all_lon = pd.concat([d_rec["lon"], d_giv["lon"]])
    all_lat = pd.concat([d_rec["lat"], d_giv["lat"]])
    ax.set_xlim(all_lon.min() - 0.05, all_lon.max() + 0.05)
    ax.set_ylim(all_lat.min() - 0.05, all_lat.max() + 0.05)

    rec_line, = ax.plot([], [], linewidth=1.5, alpha=0.8, label="MMSI 1")
    giv_line, = ax.plot([], [], linewidth=0.9, alpha=0.8, linestyle="--", label="MMSI 2")
    rec_pt,   = ax.plot([], [], marker="o", markersize=6, label="MMSI 1 now")
    giv_pt,   = ax.plot([], [], marker="o", markersize=6, label="MMSI 2 now")

    time_text = ax.text(0.02, 0.98, "", transform=ax.transAxes, va="top")
    dist_text = ax.text(0.02, 0.93, "", transform=ax.transAxes, va="top")
    ax.legend()

    def update(i):
        t = times[i]
        rec_now = d_rec[d_rec["date_time_utc"] <= t]
        giv_now = d_giv[d_giv["date_time_utc"] <= t]

        rec_line.set_data(rec_now["lon"], rec_now["lat"])
        giv_line.set_data(giv_now["lon"], giv_now["lat"])

        dist_str = "dist: n/a"
        if not rec_now.empty and not giv_now.empty:
            rlon, rlat = rec_now["lon"].iloc[-1], rec_now["lat"].iloc[-1]
            glon, glat = giv_now["lon"].iloc[-1], giv_now["lat"].iloc[-1]
            rec_pt.set_data([rlon], [rlat])
            giv_pt.set_data([glon], [glat])

            d_m = haversine_m(rlon, rlat, glon, glat)
            dist_str = f"dist: {d_m:.0f} m" if d_m < 10_000 else f"dist: {d_m/1000:,.2f} km"
            if close_threshold_m is not None and d_m <= close_threshold_m:
                dist_str += "  (CLOSE)"

        time_text.set_text(t.strftime("%Y-%m-%d %H:%M"))
        dist_text.set_text(dist_str)
        return rec_line, giv_line, rec_pt, giv_pt, time_text, dist_text

    anim = FuncAnimation(fig, update, frames=len(times), interval=50, blit=True) # change interval for animation speed
    os.makedirs(save_dir, exist_ok=True)

    def on_key(event):
        if event.key == "s":
            safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in title)[:180]
            out = os.path.join(save_dir, f"{safe}.mp4")
            print("Saving:", out)
            anim.save(out, writer="ffmpeg", fps=fps, dpi=dpi)
            print("Saved.")
        elif event.key in ("n", "escape"):
            plt.close(fig)

    fig.canvas.mpl_connect("key_press_event", on_key)
    plt.show()

# --- simplified main ---
CONS_PATH = "consecutive.csv"
ERS_PATH = "../Data/elektronisk-rapportering-ers-2024-fangstmelding-dca.csv"
AIS_PATH = "../Data/AIS/whole_month/01clean2.parquet"

df_sts = pd.read_csv(CONS_PATH)

df_sts["pair"] = df_sts.apply(
    lambda r: tuple(sorted([r["callsign1"], r["callsign2"]])),
    axis=1
)

df_ers = pd.read_csv(ERS_PATH, sep=";", engine="python",
                     encoding="utf-8", decimal=",", usecols=["Meldingstidspunkt", "Radiokallesignal (ERS)", 
                                                             "Fartøynavn (ERS)", "Pumpet fra fartøy", "Starttidspunkt", 
                                                             "Stopptidspunkt", "Aktivitet", "Varighet"])

df_ers = df_ers.dropna(subset=["Radiokallesignal (ERS)", "Pumpet fra fartøy", "Starttidspunkt", "Stopptidspunkt"])

df_ers = df_ers.drop_duplicates(
    subset=["Radiokallesignal (ERS)", "Pumpet fra fartøy", "Starttidspunkt", "Stopptidspunkt"],
    keep="first"   # keeps the first occurrence
)

fmt = "%d.%m.%Y %H:%M:%S"
df_ers["Starttidspunkt"] = pd.to_datetime(df_ers["Starttidspunkt"], format=fmt)
df_ers["Stopptidspunkt"] = pd.to_datetime(df_ers["Stopptidspunkt"], format=fmt)

df_ers = df_ers.loc[df_ers["Starttidspunkt"].between("2024-01-01", "2024-01-31 23:59:59")]

# Between !!!

print(df_ers.shape)

df_ers["Radiokallesignal (ERS)"] = (
    df_ers["Radiokallesignal (ERS)"]
    .astype("string")
    .str.strip()
    .str.upper()
)

df_ers["Pumpet fra fartøy"] = (
    df_ers["Pumpet fra fartøy"]
    .astype("string")
    .str.strip()
    .str.upper()
)

df_ers["pair"] = df_ers.apply(
    lambda r: tuple(sorted([r["Radiokallesignal (ERS)"], r["Pumpet fra fartøy"]])),
    axis=1
)

sts_pairs = set(df_sts["pair"])

df_ers = df_ers[~df_ers["pair"].isin(sts_pairs)]

plot_callsigns = list(df_ers["Radiokallesignal (ERS)"].unique()) + list(df_ers["Pumpet fra fartøy"].unique())

df_ers = df_ers.rename(columns={"Radiokallesignal (ERS)": "callsign1", "Pumpet fra fartøy": "callsign2"})

# read only those two MMSIs
table = pq.read_table(
    AIS_PATH,
    columns=["mmsi", "callsign", "date_time_utc", "lon", "lat", "speed"],
    filters=[("callsign", "in", plot_callsigns)]
)
df = table.to_pandas()
df["date_time_utc"] = pd.to_datetime(df["date_time_utc"])

callsigns = []

for r in df_ers.itertuples(index=False):
    buf = pd.Timedelta(hours=1)
    start_time = r.Starttidspunkt - buf
    end_time = r.Stopptidspunkt + buf
    df_plot = df.loc[df["date_time_utc"].between(start_time, end_time)].copy()
    a = df_plot[df_plot["callsign"] == r.callsign1].copy()
    b = df_plot[df_plot["callsign"] == r.callsign2].copy()

    print(f"{r.callsign1} and {r.callsign2} between {start_time} and {end_time}.")

    animate_sts_with_distance(
        a, b,
        title=f"{r.callsign1} vs {r.callsign2} | {start_time}–{end_time}",
        step="2min",
        close_threshold_m=50  # optional, set None to disable
    )   

#print(list(set(callsigns)))

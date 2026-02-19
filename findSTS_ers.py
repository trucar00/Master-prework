import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt


df_ers = pd.read_csv("Data/elektronisk-rapportering-ers-2024-fangstmelding-dca.csv", sep=";", encoding="utf-8", decimal=",", engine="python",
                 usecols=["Meldingstidspunkt", "Radiokallesignal (ERS)", "Fartøynavn (ERS)", "Pumpet fra fartøy", 
                          "Starttidspunkt", "Stopptidspunkt", "Startposisjon bredde", "Startposisjon lengde", "Aktivitet", "Varighet"])

df_ers = df_ers.dropna(subset=["Radiokallesignal (ERS)", "Pumpet fra fartøy", "Meldingstidspunkt", "Starttidspunkt", "Stopptidspunkt", "Varighet"])

df_ers = df_ers.drop_duplicates(
    subset=["Radiokallesignal (ERS)", "Pumpet fra fartøy", "Starttidspunkt", "Stopptidspunkt"],
    keep="first"   # keeps the first occurrence
)

print(df_ers.shape)

fmt = "%d.%m.%Y %H:%M:%S"
df_ers["Meldingstidspunkt"] = pd.to_datetime(df_ers["Meldingstidspunkt"], format=fmt)
df_ers["Starttidspunkt"] = pd.to_datetime(df_ers["Starttidspunkt"], format=fmt)
df_ers["Stopptidspunkt"] = pd.to_datetime(df_ers["Stopptidspunkt"], format=fmt)

df_ers = df_ers.loc[df_ers["Meldingstidspunkt"].between("2024-01-01", "2024-01-31 23:59:59")]
buf = pd.Timedelta(minutes=30)
df_ers["start_buf"] = df_ers["Starttidspunkt"] - buf
df_ers["stop_buf"]  = df_ers["Stopptidspunkt"] + buf

df_ers["Radiokallesignal (ERS)"] = df_ers["Radiokallesignal (ERS)"].astype("string").str.strip().str.upper()
df_ers["Pumpet fra fartøy"] = df_ers["Pumpet fra fartøy"].astype("string").str.strip().str.upper()


receiver_vessels = list(df_ers["Radiokallesignal (ERS)"].unique())
giving_vessels = list(df_ers["Pumpet fra fartøy"].unique())
sts_callsigns = receiver_vessels + giving_vessels

#print(sts_callsigns)

table = pq.read_table(
    "Data/AIS/whole_month/01clean2.parquet",
    columns=["mmsi", "callsign", "date_time_utc", "lon", "lat"],
    filters=[("callsign", "in", sts_callsigns)]
)

df_ais = table.to_pandas()
print(df_ais.shape)



df_ais["date_time_utc"] = pd.to_datetime(df_ais["date_time_utc"])

""" for radio_rec in receiver_vessels:
    fig, ax = plt.subplots(figsize=(10,8))
    d_ers = df_ers.loc[df_ers["Radiokallesignal (ERS)"] == radio_rec]
    print(d_ers)

    d_ais_rec = df_ais.loc[df_ais["callsign"] == radio_rec].copy()
    d_ais_rec = d_ais_rec.sort_values(by="date_time_utc")
    ax.plot(d_ais_rec["lon"], d_ais_rec["lat"], linewidth=0.7, alpha=0.7, label=f"Receiving: {radio_rec}")
    for radio_giv, dd in d_ers.groupby("Pumpet fra fartøy"):
        start = dd["start_buf"].iloc[0]
        stop = dd["stop_buf"].iloc[0]

        d_ais_giv = df_ais.loc[df_ais["callsign"] == radio_giv].copy()
        d_ais_giv = d_ais_giv.sort_values(by="date_time_utc")
        d_ais_giv = d_ais_giv.loc[d_ais_giv["date_time_utc"].between(start, stop)]
        d_ais_rec = d_ais_rec.loc[d_ais_rec["date_time_utc"].between(start, stop)]
        #print("min:", d_ais_giv["date_time_utc"].min(), " max:", d_ais_giv["date_time_utc"].max())
        ax.plot(d_ais_rec["lon"], d_ais_rec["lat"], linewidth=1.5, alpha=0.7, label=f"Rec in window: {radio_rec}")

        ax.plot(d_ais_giv["lon"], d_ais_giv["lat"], linewidth=0.7, alpha=0.7, linestyle="--", label=radio_giv)
        ax.scatter(dd["Startposisjon lengde"], dd["Startposisjon bredde"], s=80, facecolors="none", edgecolors="r", linewidths=2)
        #print(radio_giv)

    ax.legend()
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    plt.show() """
    #d = df_ais.loc[df_ais["callsign"] == radio][]

for radio_rec in receiver_vessels:
    fig, ax = plt.subplots(figsize=(10, 8))

    d_ers = df_ers.loc[df_ers["Radiokallesignal (ERS)"] == radio_rec].copy()
    if d_ers.empty:
        continue

    # Receiver AIS (full track)
    d_ais_rec_full = (
        df_ais.loc[df_ais["callsign"] == radio_rec]
        .sort_values("date_time_utc")
        .copy()
    )

    ax.plot(
        d_ais_rec_full["lon"], d_ais_rec_full["lat"],
        linewidth=0.7, alpha=0.4,
        label=f"Receiving (full): {radio_rec}"
    )

    # Build ONE union mask for receiver "within any event window"
    rec_mask = pd.Series(False, index=d_ais_rec_full.index)
    for _, row in d_ers.iterrows():
        rec_mask |= d_ais_rec_full["date_time_utc"].between(row["start_buf"], row["stop_buf"])

    d_ais_rec_win = d_ais_rec_full.loc[rec_mask]

    ax.plot(
        d_ais_rec_win["lon"], d_ais_rec_win["lat"],
        linewidth=2.5, alpha=0.9,
        label=f"Receiving (in window): {radio_rec}"
    )

    # Plot each giver inside its own window(s)
    for radio_giv, dd in d_ers.groupby("Pumpet fra fartøy"):
        d_ais_giv_full = (
            df_ais.loc[df_ais["callsign"] == radio_giv]
            .sort_values("date_time_utc")
            .copy()
        )

        # union of windows for THIS giver (in case multiple rows exist)
        giv_mask = pd.Series(False, index=d_ais_giv_full.index)
        for _, row in dd.iterrows():
            giv_mask |= d_ais_giv_full["date_time_utc"].between(row["start_buf"], row["stop_buf"])

            # event marker(s)
            ax.scatter(
                row["Startposisjon lengde"], row["Startposisjon bredde"],
                s=80, facecolors="none", edgecolors="r", linewidths=2
            )

        d_ais_giv_win = d_ais_giv_full.loc[giv_mask]

        ax.plot(
            d_ais_giv_win["lon"], d_ais_giv_win["lat"],
            linewidth=0.9, alpha=0.8, linestyle="--",
            label=f"Giver (in window): {radio_giv}"
        )

    ax.legend()
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    plt.show()

# try to plot it within the timeframe something, add 10 minutes buffer on start and end
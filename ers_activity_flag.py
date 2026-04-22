import pandas as pd
import matplotlib.pyplot as plt

# Doesnt find a trawler that reports steaming in january.

df = pd.read_csv("Data/ers-fangstmelding-nonan.csv")

steaming_df = df.loc[df["Aktivitet"] == "Steaming"]

fmt = "%d.%m.%Y %H:%M:%S"
steaming_df["Starttidspunkt"] = pd.to_datetime(steaming_df["Starttidspunkt"], format=fmt)
steaming_df["Stopptidspunkt"] = pd.to_datetime(steaming_df["Stopptidspunkt"], format=fmt)

steaming_df["Radiokallesignal (ERS)"] = steaming_df["Radiokallesignal (ERS)"].astype("string").str.strip().str.upper()
steaming_callsigns = steaming_df["Radiokallesignal (ERS)"].unique()
print(steaming_df["Radiokallesignal (ERS)"].nunique())

df_ais = pd.read_csv("Data/gear_specific/trawl_clean_downsampled.csv")
df_ais["date_time_utc"] = pd.to_datetime(df_ais["date_time_utc"])

# Apply the filter (equivalent to Parquet filters)
df_ais = df_ais[df_ais["callsign"].isin(steaming_callsigns)]
print(df_ais.shape)

for callsign, d in df_ais.groupby("callsign"):

    # Sort by time (important for plotting tracks)
    d = d.sort_values("date_time_utc")

    # Get steaming intervals for this vessel
    st = steaming_df.loc[
        steaming_df["Radiokallesignal (ERS)"] == callsign
    ]

    # Create mask: default False (blue)
    steaming_mask = pd.Series(False, index=d.index)

    # Mark AIS points inside any steaming interval
    for start, stop in zip(st["Starttidspunkt"], st["Stopptidspunkt"]):
        steaming_mask |= (
            (d["date_time_utc"] >= start) &
            (d["date_time_utc"] <= stop)
        )

    # Plot
    plt.figure(figsize=(8, 6))

    # Blue = not steaming
    """ plt.plot(
        d.loc[~steaming_mask, "lon"],
        d.loc[~steaming_mask, "lat"],
        "b",
        label="Other",
        linewidth=2
    ) """

    # Red = steaming
    plt.plot(
        d.loc[steaming_mask, "lon"],
        d.loc[steaming_mask, "lat"],
        "r",
        linewidth=4,
        label="Steaming"
    )

    plt.title(f"{callsign}")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.legend()
    plt.show()

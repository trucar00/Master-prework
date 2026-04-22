import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq 

# READY TO SAVE GEAR SPECIFIC AIS DATA

def get_ers(path="Data/ers-fangstmelding-nonan.csv"):
    df_ers = pd.read_csv("Data/ers-fangstmelding-nonan.csv") #whole of 2024

    print(df_ers["Redskap - gruppe"].unique())

    df_ers = df_ers.dropna(subset=["Starttidspunkt", "Stopptidspunkt", "Radiokallesignal (ERS)", "Redskap - gruppe", "Varighet"])
    df_ers = df_ers.drop_duplicates(keep="first")

    fmt = "%d.%m.%Y %H:%M:%S"
    df_ers["Starttidspunkt"] = pd.to_datetime(df_ers["Starttidspunkt"], format=fmt)
    df_ers["Stopptidspunkt"] = pd.to_datetime(df_ers["Stopptidspunkt"], format=fmt)

    df_ers = df_ers.loc[df_ers["Stopptidspunkt"] >= df_ers["Starttidspunkt"]].copy()
    df_ers["Varighet"] = pd.to_numeric(df_ers["Varighet"], errors="coerce")

    #df_ers = df_ers.loc[df_ers["Starttidspunkt"].between("2024-01-01", "2024-01-31 23:59:59")] # CHANGE for month

    df_ers["Radiokallesignal (ERS)"] = df_ers["Radiokallesignal (ERS)"].astype("string").str.strip().str.upper()
    df_ers["Redskap - gruppe"] = df_ers["Redskap - gruppe"].astype("string").str.strip()
    df_ers = df_ers.reset_index(drop=True)
    df_ers["ers_id"] = df_ers.index
    return df_ers


def filter_ers_by_gear(df, gear):
    df = df.loc[df["Redskap - gruppe"] == gear].copy()
    return df

def filter_ers_by_activity(df, activities):
    df = df.loc[
        df["Aktivitet"].isin(activities)
    ].copy()
    return df

def duration_filter(df, min_duration, max_duration):
    df = df.loc[(df["Varighet"] >= min_duration) & (df["Varighet"] <= max_duration)].copy()
    return df

def get_callsigns(df):
    return df["Radiokallesignal (ERS)"].unique()

def read_ais_parquet(parquet_path, callsigns=None):
    columns = ["mmsi", "trajectory_id", "callsign", "date_time_utc", "lon", "lat", "speed", "cog"]

    filters = None
    if callsigns is not None and len(callsigns) > 0:
        filters = [("callsign", "in", list(callsigns))]

        table = pq.read_table(parquet_path, columns=columns, filters=filters)
        df_ais = table.to_pandas()
    else:
        df_ais = pd.DataFrame(columns=columns)
        print("No callsigns")

    df_ais["callsign"] = df_ais["callsign"].astype("string").str.strip().str.upper()
    df_ais["date_time_utc"] = pd.to_datetime(df_ais["date_time_utc"], errors="coerce")
    df_ais = df_ais.dropna(subset=["callsign", "date_time_utc"])

    return df_ais

# -----------------------------------
# Interval join: AIS messages inside ERS windows
# -----------------------------------
def match_ais_to_ers_windows(df_ais, df_ers):
    """
    Returns AIS rows that fall inside one or more ERS activity windows
    for the same callsign.

    Output contains AIS columns + selected ERS columns.
    """
    out = []

    # Group both by callsign so we only compare relevant rows
    ers_groups = {
        c: g.sort_values("Starttidspunkt").reset_index(drop=True)
        for c, g in df_ers.groupby("Radiokallesignal (ERS)", sort=False)
    }

    for callsign, ais_g in df_ais.groupby("callsign", sort=False):
        if callsign not in ers_groups:
            continue

        ers_g = ers_groups[callsign]
        ais_g = ais_g.sort_values("date_time_utc").reset_index(drop=True)

        # Cross join within one callsign only
        ais_g = ais_g.assign(_tmp=1)
        ers_g = ers_g.assign(_tmp=1)

        merged = ais_g.merge(ers_g, on="_tmp", suffixes=("", "_ers")).drop(columns="_tmp")

        matched = merged.loc[
            (merged["date_time_utc"] >= merged["Starttidspunkt"]) &
            (merged["date_time_utc"] <= merged["Stopptidspunkt"])
        ].copy()

        if not matched.empty:
            out.append(matched)

    if not out:
        return pd.DataFrame()

    result = pd.concat(out, ignore_index=True)

    return result

# -----------------------------------
# Full pipeline
# -----------------------------------
def create_gear_specific_ais_dataset(
    ers_path,
    ais_parquet_path,
    gear=None,
    activities=None,
    min_duration=None,
    max_duration=None,
    save_path=None
):
    # Load ERS
    df_ers = get_ers(ers_path)

    if gear is not None:
        df_ers = filter_ers_by_gear(df_ers, gear)

    if activities is not None:
        df_ers = filter_ers_by_activity(df_ers, activities)

    if min_duration is not None and max_duration is not None:
        df_ers = duration_filter(df_ers, min_duration, max_duration)

    print("ERS rows after filtering:", len(df_ers))
    print("Unique ERS callsigns:", df_ers["Radiokallesignal (ERS)"].nunique())

    callsigns = df_ers["Radiokallesignal (ERS)"].dropna().unique().tolist()

    # Load AIS only for relevant callsigns
    df_ais = read_ais_parquet(ais_parquet_path, callsigns=callsigns)

    print("AIS rows before time-window filtering:", len(df_ais))
    print("Unique AIS callsigns:", df_ais["callsign"].nunique())

    # Match AIS rows to ERS activity windows
    matched = match_ais_to_ers_windows(df_ais, df_ers)

    print("AIS rows matched to ERS windows:", len(matched))

    # Keep only useful columns
    if not matched.empty:
        keep_cols = [
            #"ers_id",
            "mmsi",
            "trajectory_id",
            "callsign",
            "date_time_utc",
            "lon",
            "lat",
            "speed",
            "cog",
            "Radiokallesignal (ERS)",
            "Redskap - gruppe",
            "Aktivitet",
            "Starttidspunkt",
            "Stopptidspunkt",
            "Varighet",
        ]
        keep_cols = [c for c in keep_cols if c in matched.columns]
        matched = matched[keep_cols].copy()

    if save_path is not None:
        matched.to_csv(save_path, index=False)
        print(f"Saved to {save_path}")

    return matched

GEAR = "Snurrevad"
for month in range(1, 9):

    print(f"Finding fishing segments for {GEAR} for month: {month:02d}")
    matched = create_gear_specific_ais_dataset(
        ers_path="Data/ers-fangstmelding-nonan.csv",
        ais_parquet_path=f"Data/AIS/whole_month2/{month:02d}.parquet",
        gear=GEAR,
        activities=["I fiske"],   # adjust to your actual values
        min_duration=15,
        max_duration=1500,
        save_path=None              #f"gear/not/{month:02d}.csv"
    )

    print(matched.head())
    print(matched.shape)

    if len(matched) < 5:
        print(f"Not enough messages for {GEAR} in month {month:02d}")
        continue
    else:

        fig, ax = plt.subplots(figsize=(10,8))

        cnt = 0
        for radio, d in matched.groupby("callsign"):
            d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
            d = d.sort_values(by="date_time_utc")
            ax.scatter(d["lon"], d["lat"], s=1, alpha=0.7)


        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        #plt.title(matched)
        plt.show()


# AIS format
# time, loc, callsign, gear, start_pos, end_pos

# can use start time (and end time) to approximate some time window in which the gear is being used, !!
# and then color the plots when they report that they begin fishing, seem innaccurate
# redskap, tidsrom, 
# all ais messages that 
# plot ais messages only within the timewindow the gear is being used. finne trål, finne lsm når de endrer redskap. 
# create a dataset where we redskap start and redskap end. 

# for every callsign: can find the time window in which the redskap is being used




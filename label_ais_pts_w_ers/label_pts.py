import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq

GEAR_TYPES = ["Trål", "Not", "Krokredskap", "Snurrevad", "Garn"]

DURATION_LIMITS = {
    "Trål": (30, 500),
    "Not": (15, 250),
    "Snurrevad": (15, 250),
    "Krokredskap": (500, 1500),
    "Garn": (150, 1250),
}

def get_ers(ers_path, gear_types=GEAR_TYPES, activities=["I fiske"]):
    df_ers = pd.read_csv(ers_path)

    print(df_ers["Redskap - gruppe"].unique())

    df_ers = df_ers.dropna(
        subset=[
            "Starttidspunkt",
            "Stopptidspunkt",
            "Radiokallesignal (ERS)",
            "Redskap - gruppe",
            "Varighet",
            "Aktivitet",
        ]
    )
    df_ers = df_ers.drop_duplicates(keep="first")

    fmt = "%d.%m.%Y %H:%M:%S"
    df_ers["Starttidspunkt"] = pd.to_datetime(df_ers["Starttidspunkt"], format=fmt)
    df_ers["Stopptidspunkt"] = pd.to_datetime(df_ers["Stopptidspunkt"], format=fmt)

    df_ers = df_ers.loc[df_ers["Stopptidspunkt"] >= df_ers["Starttidspunkt"]].copy()
    df_ers["Varighet"] = pd.to_numeric(df_ers["Varighet"], errors="coerce")

    df_ers["Radiokallesignal (ERS)"] = (
        df_ers["Radiokallesignal (ERS)"].astype("string").str.strip().str.upper()
    )
    df_ers["Redskap - gruppe"] = (
        df_ers["Redskap - gruppe"].astype("string").str.strip()
    )

    df_ers = df_ers.loc[df_ers["Redskap - gruppe"].isin(gear_types)].copy()
    df_ers = df_ers.loc[df_ers["Aktivitet"].isin(activities)].copy()

    # apply duration limits for each gear type
    df_ers["min_duration"] = df_ers["Redskap - gruppe"].map(lambda g: DURATION_LIMITS[g][0])
    df_ers["max_duration"] = df_ers["Redskap - gruppe"].map(lambda g: DURATION_LIMITS[g][1])

    df_ers = df_ers.loc[
        (df_ers["Varighet"] >= df_ers["min_duration"]) &
        (df_ers["Varighet"] <= df_ers["max_duration"])
    ].copy()

    df_ers = df_ers.drop(columns=["min_duration", "max_duration"])

    df_ers = df_ers.reset_index(drop=True)
    return df_ers

def get_registered_callsigns(df_ers):
    return df_ers["Radiokallesignal (ERS)"].unique()

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

def assign_ais_message_to_label(df_ais, df_ers):

    ers_groups = {
        callsign: d.sort_values("Starttidspunkt").reset_index(drop=True)
        for callsign, d in df_ers.groupby("Radiokallesignal (ERS)", sort=False)
    }

    labeled_parts = []

    for callsign, d_ais in df_ais.groupby("callsign", sort=False):
        d_ais = d_ais.sort_values("date_time_utc").copy()
        if callsign not in ers_groups:
            labeled_parts.append(d_ais)
            continue

        d_ers = ers_groups[callsign]
        for _, row in d_ers.iterrows():
            mask = (
                (d_ais["date_time_utc"] >= row["Starttidspunkt"]) &
                (d_ais["date_time_utc"] <= row["Stopptidspunkt"])
            )
            d_ais.loc[mask, "label"] = row["Redskap - gruppe"]

        labeled_parts.append(d_ais)

    df_labeled = pd.concat(labeled_parts, ignore_index=True)
    return df_labeled


def local_main():
    df_ers = get_ers(ers_path="Data/ers-fangstmelding-nonan.csv")
    registered_callsigns = get_registered_callsigns(df_ers)

    df_ais = read_ais_parquet(parquet_path="Data/AIS/whole_month2/01.parquet", callsigns=registered_callsigns)


    df_ais_with_labels = assign_ais_message_to_label(df_ais, df_ers)
    print(df_ais_with_labels.head())
    df_ais_with_labels.to_csv("ais_with_ers_labels.csv")


def main():
    df_ers = get_ers(ers_path="ers-fangstmelding-nonan.csv")
    registered_callsigns = get_registered_callsigns(df_ers)

    for month in range(1, 13):
        filepath = f"~Test/IDUN/Processed_AIS_2024/Cleaned_pq/{month:02d}.parquet"

        df_ais = read_ais_parquet(parquet_path=filepath, callsigns=registered_callsigns)

        df_ais_with_labels = assign_ais_message_to_label(df_ais, df_ers)
        df_ais_with_labels.to_csv(f"ais_with_ers_labels_{month:02d}.csv")


if __name__ == "__main__":
    main()
    #local_main()

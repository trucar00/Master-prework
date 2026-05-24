import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq 

# READY TO SAVE GEAR SPECIFIC AIS DATA

def get_callsigns(month, gear, year=2024):
    df_ers = pd.read_csv(f"Data/ers-fangstmelding-nonan-{year}.csv")

    nr_callsigns_ers = df_ers["Radiokallesignal (ERS)"].nunique()

    print(df_ers["Redskap - gruppe"].unique())

    df_ers = df_ers.dropna(subset=["Starttidspunkt", "Stopptidspunkt", "Radiokallesignal (ERS)", "Redskap - gruppe", "Varighet"])
    df_ers = df_ers.drop_duplicates(keep="first")

    fmt = "%d.%m.%Y %H:%M:%S"
    df_ers["Starttidspunkt"] = pd.to_datetime(df_ers["Starttidspunkt"], format=fmt)
    df_ers["Stopptidspunkt"] = pd.to_datetime(df_ers["Stopptidspunkt"], format=fmt)

     # Keep only this month
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.offsets.MonthBegin(1) 

    print(start, end)

    df_ers = df_ers.loc[
        (df_ers["Starttidspunkt"] >= start) &
        (df_ers["Starttidspunkt"] < end)
    ].copy()

    df_ers["Radiokallesignal (ERS)"] = df_ers["Radiokallesignal (ERS)"].astype("string").str.strip().str.upper()
    df_ers["Redskap - gruppe"] = df_ers["Redskap - gruppe"].astype("string").str.strip()

    df_ers["start_pos"] = list(zip(df_ers["Startposisjon lengde"].astype(float), df_ers["Startposisjon bredde"].astype(float)))
    df_ers["end_pos"] = list(zip(df_ers["Stopposisjon lengde"].astype(float), df_ers["Stopposisjon bredde"].astype(float)))

    gear_sets = (
        df_ers.groupby("Radiokallesignal (ERS)")["Redskap - gruppe"]
            .agg(lambda s: set(s.astype("string").str.strip()))
    )

    callsigns_contain = gear_sets[
        gear_sets.apply(lambda x: len(x) >= 1 and gear in x)
    ].index.tolist()

    print(f"Nr with {gear} registered", len(callsigns_contain)) # Callsigns that have registered Trål

    callsigns = gear_sets[
        gear_sets.apply(lambda x: len(x) == 1 and x[0] == gear)
    ].index.tolist()

    check = df_ers.loc[df_ers["Radiokallesignal (ERS)"].isin(callsigns)]
    print(check[["Radiokallesignal (ERS)", "Redskap - gruppe"]].head())
    print(check["Redskap - gruppe"].unique())

    return callsigns

GEAR = "Bur og ruser"


for month in range(12, 12+1):
    callsigns = get_callsigns(month=month, gear=GEAR)
    print(f"Nr of only {GEAR}: ", len(callsigns)) # Callsigns that have only registered Trål

    
    table = pq.read_table(
        f"Data/AIS/whole_month_new/{month:02d}.parquet",
        columns=["callsign", "date_time_utc", "speed"],
        filters=[("callsign", "in", callsigns)]
    )

    df_ais = table.to_pandas()
    df_ais.to_parquet(f"gear/traps/{month:02d}.parquet", index=False)



# AIS format
# time, loc, callsign, gear, start_pos, end_pos

# can use start time (and end time) to approximate some time window in which the gear is being used, !!
# and then color the plots when they report that they begin fishing, seem innaccurate
# redskap, tidsrom, 
# all ais messages that 
# plot ais messages only within the timewindow the gear is being used. finne trål, finne lsm når de endrer redskap. 
# create a dataset where we redskap start and redskap end. 

# for every callsign: can find the time window in which the redskap is being used




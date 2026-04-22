import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq 

# READY TO SAVE GEAR SPECIFIC AIS DATA

df_ers = pd.read_csv("Data/ers-fangstmelding-nonan.csv")

nr_callsigns_ers = df_ers["Radiokallesignal (ERS)"].nunique()

print(df_ers["Redskap - gruppe"].unique())

df_ers = df_ers.dropna(subset=["Starttidspunkt", "Stopptidspunkt", "Radiokallesignal (ERS)", "Redskap - gruppe", "Varighet"])
df_ers = df_ers.drop_duplicates(keep="first")

fmt = "%d.%m.%Y %H:%M:%S"
df_ers["Starttidspunkt"] = pd.to_datetime(df_ers["Starttidspunkt"], format=fmt)
df_ers["Stopptidspunkt"] = pd.to_datetime(df_ers["Stopptidspunkt"], format=fmt)

#df_ers = df_ers.loc[df_ers["Starttidspunkt"].between("2024-01-01", "2024-01-31 23:59:59")] # CHANGE for month

df_ers["Radiokallesignal (ERS)"] = df_ers["Radiokallesignal (ERS)"].astype("string").str.strip().str.upper()
df_ers["Redskap - gruppe"] = df_ers["Redskap - gruppe"].astype("string").str.strip()

df_ers["start_pos"] = list(zip(df_ers["Startposisjon lengde"].astype(float), df_ers["Startposisjon bredde"].astype(float)))
df_ers["end_pos"] = list(zip(df_ers["Stopposisjon lengde"].astype(float), df_ers["Stopposisjon bredde"].astype(float)))

gear = "Not"

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

print(f"Nr of only {gear}: ", len(callsigns)) # Callsigns that have only registered Trål

check = df_ers.loc[df_ers["Radiokallesignal (ERS)"].isin(callsigns)]
print(check[["Radiokallesignal (ERS)", "Redskap - gruppe"]].head())
print(check["Redskap - gruppe"].unique())


table = pq.read_table(
    "Data/AIS/whole_month/01clean.parquet",
    columns=["mmsi", "callsign", "date_time_utc", "lon", "lat", "speed", "cog"],
    filters=[("callsign", "in", callsigns)]
)

df_ais = table.to_pandas()
print(df_ais.tail())
print(df_ais.shape)
#df_ais.to_csv(f"Data/gear_specific/not_feb_2024.csv", index=False)


fig, ax = plt.subplots(figsize=(10,8))

cnt = 0
for radio, d in df_ais.groupby("callsign"):
    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)


ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.title(gear)
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




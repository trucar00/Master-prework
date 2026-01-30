import pandas as pd
import matplotlib.pyplot as plt

df_ais = pd.read_parquet("Data/AIS/01.parquet", engine="pyarrow")
df_ers = pd.read_csv("Data/ers-fangstmelding-nonan.csv")

nr_callsigns_ais = df_ais["callsign"].nunique()
nr_callsigns_ers = df_ers["Radiokallesignal (ERS)"].nunique()
print("AIS:", nr_callsigns_ais, "ERS:", nr_callsigns_ers)

fmt = "%d.%m.%Y %H:%M:%S"
df_ers["Starttidspunkt"] = pd.to_datetime(df_ers["Starttidspunkt"], format=fmt)
df_ers["Stopptidspunkt"] = pd.to_datetime(df_ers["Stopptidspunkt"], format=fmt)

df_ers = df_ers.loc[df_ers["Starttidspunkt"].between("2024-01-01", "2024-01-05 23:59:59")]

df_ers["Radiokallesignal (ERS)"] = df_ers["Radiokallesignal (ERS)"].astype("string").str.strip().str.upper()
df_ers["Redskap - gruppe"] = df_ers["Redskap - gruppe"].astype("string").str.strip()

df_ais["callsign"] = df_ais["callsign"].astype("string").str.strip().str.upper()

df_ers["start_pos"] = list(zip(df_ers["Startposisjon lengde"].astype(float), df_ers["Startposisjon bredde"].astype(float)))
df_ers["end_pos"] = list(zip(df_ers["Stopposisjon lengde"].astype(float), df_ers["Stopposisjon bredde"].astype(float)))

#print(df_ers[["Fartøynavn (ERS)", "Starttidspunkt", "Startposisjon bredde", "Startposisjon lengde", "start_pos", "Stopposisjon bredde", "Stopposisjon lengde", "end_pos"]].head())

ers_summary = (
    df_ers.groupby("Radiokallesignal (ERS)", as_index=False)
       .agg(
           start_time=("Starttidspunkt", list),
           end_time=("Stopptidspunkt", list),
           gear=("Redskap - gruppe", list),
           start_pos=("start_pos", list),
           end_pos=("end_pos", list),
       )
       .rename(columns={"Radiokallesignal (ERS)": "callsign"})
)

#print(ers_summary.head())

merged = df_ais.merge(ers_summary, on="callsign", how="inner")

#print(merged[["date_time_utc", "lon", "lat", "callsign", "gear", "start_pos", "end_pos"]].head())

""" gears_by_callsign = (
    df_ers.dropna(subset=["Radiokallesignal (ERS)", "Redskap - gruppe"])
         .groupby("Radiokallesignal (ERS)")["Redskap - gruppe"]
         .apply(lambda s: sorted(set(s)))
         .reset_index(name="gears")
)

print(gears_by_callsign.to_string(index=False)) """

gear = "Snurrevad"

df_gear = merged[merged["gear"].astype(str).str.contains(gear, na=False)]
#print(df_gear.head())

fig, ax = plt.subplots(figsize=(10,8))

cnt = 0
for radio, d in df_gear.groupby("callsign"):
    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)
    start_positions = ers_summary.loc[ers_summary["callsign"] == radio]["start_pos"].iloc[0]
    end_positions = ers_summary.loc[ers_summary["callsign"] == radio]["end_pos"].iloc[0]
    for pos in start_positions:
        ax.scatter(pos[0], pos[1], color="green", s=4)
    for pos in end_positions:
        ax.scatter(pos[0], pos[1], color="red", s=4)
    
    print(d["callsign"].iloc[0])

    cnt += 1
    if cnt > 10:
        break


ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.show()


# AIS format
# time, loc, callsign, gear, start_pos, end_pos

# can use start time (and end time) to approximate some time window in which the gear is being used, !!
# and then color the plots when they report that they begin fishing, seem innaccurate




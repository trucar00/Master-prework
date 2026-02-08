import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

cases_df = pd.read_csv("close_pairs_intervals.csv")
cases_df["start_time"] = pd.to_datetime(cases_df["start_time"])
cases_df["out_time"] = pd.to_datetime(cases_df["out_time"])

result = cases_df.groupby("mmsi1")["mmsi2"].apply(list).to_dict()

unique_dict = {k: list(set(v)) for k, v in result.items()}

mmsis = list(cases_df["mmsi1"].unique()) + list(cases_df["mmsi2"].unique())

print(mmsis)

table = pq.read_table(
    "../Data/AIS/whole_month/01clean.parquet",
    columns=["mmsi", "callsign", "date_time_utc", "lon", "lat"],
    filters=[("mmsi", "in", mmsis)]
)

df_ais = table.to_pandas()

for k,v in unique_dict.items():
    fig, ax = plt.subplots(figsize=(10,8))
    dk= df_ais.loc[df_ais["mmsi"] == k]
    dv= df_ais.loc[df_ais["mmsi"] == v[0]]
    print(k, v[0])

    t_d = cases_df.loc[(cases_df["mmsi1"] == k) & (cases_df["mmsi2"] == v[0])].copy()

    buf = pd.Timedelta(hours=1)
    start = pd.to_datetime(t_d["start_time"].iloc[0], utc=True) - buf
    end   = pd.to_datetime(t_d["out_time"].iloc[0], utc=True) + buf

    print(start, end)

    dk["date_time_utc"] = pd.to_datetime(dk["date_time_utc"], utc=True)
    dv["date_time_utc"] = pd.to_datetime(dv["date_time_utc"], utc=True)


    dk = dk.loc[dk["date_time_utc"].between(start, end)]
    dv = dv.loc[dv["date_time_utc"].between(start, end)]
    dk = dk.sort_values(by="date_time_utc")
    dv = dv.sort_values(by="date_time_utc")
    ax.plot(dk["lon"], dk["lat"], linewidth=0.7, alpha=0.7, label=k)
    ax.plot(dv["lon"], dv["lat"], linewidth=0.7, alpha=0.7, label=v)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    plt.legend()
    plt.show()


""" fig, ax = plt.subplots(figsize=(10,8))
for m, d in df_ais.groupby("mmsi"):
    
    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    #cd = cases_df.loc[cases_df["mmsi1"]]
    ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7)


ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.show() """


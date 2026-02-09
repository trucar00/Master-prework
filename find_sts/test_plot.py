import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import pandas as pd

AIS_PATH = "../Data/AIS/whole_month/01clean2.parquet"

test_mmsis = [220507000, 245265000]

table = pq.read_table(
    AIS_PATH,
    columns=["mmsi", "lat", "lon", "date_time_utc", "speed", "cog", "ship_type", "callsign"],
    filters=[("mmsi", "in", test_mmsis)],
)

df_ais = table.to_pandas()
df_ais["date_time_utc"] = pd.to_datetime(df_ais["date_time_utc"])

start_time = pd.Timestamp("2024-01-10 22:00:00")
end_time = pd.Timestamp("2024-01-11 05:00:00")

df_ais = df_ais.loc[df_ais["date_time_utc"].between(start_time, end_time)]
print(df_ais)

fig, ax = plt.subplots(figsize=(10,8))
for mmsi,d in df_ais.groupby("mmsi"):
    d.sort_values(by="date_time_utc")
    ax.scatter(d["lon"], d["lat"])

plt.show()
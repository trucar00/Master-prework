import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

df = pd.read_csv("Data/fangstdata_2024.csv", sep=";", encoding="utf-8", decimal=",")

df = df[["Fartøynavn", "Fartøy ID", "Radiokallesignal (seddel)",  "Fartøytype (kode)", "Fartøynasjonalitet (kode)", 
        "Siste fangstdato", "Redskap - gruppe", "Hovedområde (kode)", "Landingsdato", "Produktvekt", 
        "Mottakende fartøy reg.merke" , "Mottakende fartøy rkal", "Mottakende fartøytype (kode)"]]

df["Fartøy ID"] = df["Fartøy ID"].astype("Int64").astype("string")

sts_df = df[
    df["Mottakende fartøy rkal"].notna() &
    df["Radiokallesignal (seddel)"].notna()
]

sts_df["Radiokallesignal (seddel)"] = sts_df["Radiokallesignal (seddel)"].astype("string").str.strip().str.upper()
sts_df["Mottakende fartøy rkal"] = sts_df["Mottakende fartøy rkal"].astype("string").str.strip().str.upper()

sts_df["Siste fangstdato"] = pd.to_datetime(sts_df["Siste fangstdato"], format="%d.%m.%Y", errors="coerce")
sts_df["Landingsdato"] = pd.to_datetime(sts_df["Landingsdato"], format="%d.%m.%Y", errors="coerce")
sts_df = sts_df.loc[sts_df["Siste fangstdato"].between("2024-01-01", "2024-01-31 23:59:59")]

print(sts_df[["Radiokallesignal (seddel)", "Mottakende fartøy rkal", "Siste fangstdato", "Landingsdato"]].head())
print(sts_df.loc[sts_df["Radiokallesignal (seddel)"] == "LK3887"][["Radiokallesignal (seddel)", "Mottakende fartøy rkal", "Landingsdato"]])


receiver_vessel = list(sts_df["Mottakende fartøy rkal"].unique())
giving_vessel = list(sts_df["Radiokallesignal (seddel)"].unique())
sts_callsigns = receiver_vessel + giving_vessel

print(sts_callsigns)

check = ["LK3887", "LCMN"]

table = pq.read_table(
    "Data/AIS/whole_month/01.parquet",
    columns=["mmsi", "callsign", "date_time_utc", "lon", "lat"],
    filters=[("callsign", "in", sts_callsigns)]
)

df_ais = table.to_pandas()

print(df_ais.shape)


fig, ax = plt.subplots(figsize=(10,8))

for radio, d in df_ais.groupby("callsign"):
    

    d["date_time_utc"] = pd.to_datetime(d["date_time_utc"])
    d = d.sort_values(by="date_time_utc")
    if radio in receiver_vessel:
        ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7, linestyle="--", label="Receiving")
    elif radio in giving_vessel:
        ax.plot(d["lon"], d["lat"], linewidth=0.7, alpha=0.7, label="Giving")
        print(radio, " is giving")
    else:
        print(radio, " wtf")

ax.legend()
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.show()

# LK3887, LCMN: no obvious sts case
import pandas as pd

df = pd.read_csv("Data/fangstdata_2024.csv", sep=";", encoding="utf-8", decimal=",")

df = df[["Fartøynavn", "Fartøy ID", "Radiokallesignal (seddel)",  "Fartøytype (kode)", "Fartøynasjonalitet (kode)", 
        "Siste fangstdato", "Redskap - gruppe", "Hovedområde (kode)", "Landingsdato", "Produktvekt", 
        "Mottakende fartøy reg.merke" , "Mottakende fartøy rkal", "Mottakende fartøytype (kode)"]]

df["Fartøy ID"] = df["Fartøy ID"].astype("Int64").astype("string")

sts_df = df[
    df["Mottakende fartøy rkal"].notna() &
    df["Radiokallesignal (seddel)"].notna()
]

sts_df["Siste fangstdato"] = pd.to_datetime(sts_df["Siste fangstdato"], format="%d.%m.%Y", errors="coerce")

print(sts_df[["Radiokallesignal (seddel)", "Mottakende fartøy rkal"]].head())

print(sts_df.shape)

sts_df.to_csv("Data/STS/fangstdata_2024_sts.csv", index=False)

ais_df = pd.read_parquet("Data/AIS/01.parquet", engine="pyarrow")
ais_df["date_time_utc"] = pd.to_datetime(ais_df["date_time_utc"])

t_min = ais_df["date_time_utc"].min()
t_max = ais_df["date_time_utc"].max()

print("AIS window: ", t_min, "->", t_max)

sts_in_window = sts_df.loc[
    sts_df["Siste fangstdato"].between(t_min, t_max, inclusive="both")
].copy()

sender_callsigns = (
    sts_in_window["Radiokallesignal (seddel)"]
    .astype("string")
    .str.strip()
    .dropna()
)

receiver_callsigns = (
    sts_in_window["Mottakende fartøy rkal"]
    .astype("string")
    .str.strip()
    .dropna()
)

sender_set = set(sender_callsigns.unique())
receiver_set = set(receiver_callsigns.unique())
all_set = sender_set | receiver_set

ais_df["callsign"] = ais_df["callsign"].astype("string").str.strip()

ais_sts = ais_df.loc[ais_df["callsign"].isin(all_set)].copy()
ais_sts["Receiving_vessel"] = ais_sts["callsign"].isin(receiver_set)

#ais_sts.to_csv("Data/STS/ais.csv", index=False)

print("AIS rows before:", len(ais_df))
print("AIS rows after STS filter:", len(ais_sts))
print(ais_sts[["date_time_utc", "callsign", "Receiving_vessel"]].head())


""" print(df["Redskap - gruppe"].unique())

for gear, d in df.groupby("Redskap - gruppe"):
    gear = gear.replace(" ", "_")
    d.to_csv(f"Data/{gear}_2024.csv") """
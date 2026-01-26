import pandas as pd

#df_ais = pd.read_parquet("Data/AIS/01.parquet", engine="pyarrow")
df_ers = pd.read_csv("Data/ers-fangstmelding-nonan.csv")

df_ers["Starttidspunkt"] = pd.to_datetime(df_ers["Starttidspunkt"])
df_ers = df_ers.loc[
    df_ers["Starttidspunkt"].between(
        "2024-01-01", "2024-01-05 23:59:59"
    )
]

df_ers["Radiokallesignal (ERS)"] = df_ers["Radiokallesignal (ERS)"].astype("string").str.strip().str.upper()
df_ers["Redskap - gruppe"] = df_ers["Redskap - gruppe"].astype("string").str.strip()

gears_by_callsign = (
    df_ers.dropna(subset=["Radiokallesignal (ERS)", "Redskap - gruppe"])
         .groupby("Radiokallesignal (ERS)")["Redskap - gruppe"]
         .apply(lambda s: sorted(set(s)))
         .reset_index(name="gears")
)

print(gears_by_callsign.to_string(index=False))

""" print(df_ers["Radiokallesignal (ERS)"].unique())
print(df_ais["callsign"].unique()) """
# merge on radio


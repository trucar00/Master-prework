import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq 
import seaborn as sns

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
df_ers["Varighet"] = pd.to_numeric(df_ers["Varighet"], errors="coerce")
df_ers = df_ers.loc[df_ers["Varighet"] < 2000].copy()

gears = ["Trål", "Not", "Krokredskap", "Snurrevad", "Garn"]
activity_flags = ["I fiske", "Setting av redskap"]

plt.figure()

for gear in gears:

    gear_specific = df_ers.loc[df_ers["Redskap - gruppe"] == gear].copy()
    reported_gear_fishing = gear_specific.loc[
        gear_specific["Aktivitet"].isin(activity_flags)
    ].copy()

    sns.kdeplot(
        reported_gear_fishing["Varighet"].dropna(),
        label=gear,
        clip=(0, None)
    )

plt.xlabel("Minutes")
plt.title("Duration by gear type")
plt.legend()
plt.show()
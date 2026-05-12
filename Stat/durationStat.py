import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq 
import seaborn as sns
import numpy as np

files = [
    "Data/ers-fangstmelding-nonan-2023.csv",
    "Data/ers-fangstmelding-nonan-2024.csv",
    "Data/ers-fangstmelding-nonan-2025.csv"
]

dfs = [pd.read_csv(f) for f in files]
df_ers = pd.concat(dfs, ignore_index=True)

df_ers = df_ers[
    df_ers["Starttidspunkt"].str.contains(" ", na=False) &
    df_ers["Stopptidspunkt"].str.contains(" ", na=False)
]

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

gears = ["Trål", "Not", "Krokredskap", "Snurrevad", "Garn", "Bur og ruser"]
activity_flags = ["I fiske"]

gear_translation = {
    "Trål": "Trawl",
    "Not": "Purse seine",
    "Krokredskap": "Hook gear",
    "Snurrevad": "Danish seine",
    "Garn": "Gillnet",
    "Bur og ruser": "Traps",
}

plt.figure()

for gear in gears:

    gear_specific = df_ers.loc[df_ers["Redskap - gruppe"] == gear].copy()
    reported_gear_fishing = gear_specific.loc[
        gear_specific["Aktivitet"].isin(activity_flags)
    ].copy()

    sns.kdeplot(
        reported_gear_fishing["Varighet"].dropna(),
        label=gear_translation[gear],
        clip=(0, 1800),
        linewidth=2.5
        
    )

plt.xlabel("Minutes", fontsize=14)
plt.ylabel("Density", fontsize=14)
#plt.title("Duration by gear type")
plt.legend(fontsize=12)
plt.xticks(np.arange(0, df_ers["Varighet"].max(), 250))
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.tight_layout()
plt.subplots_adjust(left=0.08)  # reduce left margin
plt.xlim(0, 1800)
plt.show()
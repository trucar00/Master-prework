import pandas as pd

# Finds the callsigns that are close in proximity from AIS data (sts found in AIS) in the ERS
# some of them are yes, so we find true sts cases in the ais data!

callsigns = ['JWLM', 'LLMI', 'LJZH', '3YMI', 'LALD', 'LFNM', 'LDIW', 'LJSY', 'LEWL', 'LLQX', 
             'LIRW', 'LCOV', 'LIZI', 'LCUF', 'LLWF', 'LADH', '3YVG', 'LCGV', 'LJDJ', 'JXUE', 
             'LHEA', 'LKLV', 'LDEF', 'LCJG', 'LLWG', 'LMFZ', 'LMCW', 'LGSH', 'LLLP', 'LLAS']

df_sts = pd.read_csv("consecutive.csv")

df_sts["pair"] = df_sts.apply(
    lambda r: tuple(sorted([r["callsign1"], r["callsign2"]])),
    axis=1
)

df_ers = pd.read_csv("../Data/elektronisk-rapportering-ers-2024-fangstmelding-dca.csv", sep=";", engine="python",
                     encoding="utf-8", decimal=",", usecols=["Meldingstidspunkt", "Radiokallesignal (ERS)", 
                                                             "Fartøynavn (ERS)", "Pumpet fra fartøy", "Starttidspunkt", 
                                                             "Stopptidspunkt", "Aktivitet", "Varighet"])

df_ers = df_ers.dropna(subset=["Radiokallesignal (ERS)", "Pumpet fra fartøy", "Starttidspunkt", "Stopptidspunkt"])

df_ers = df_ers.drop_duplicates(
    subset=["Radiokallesignal (ERS)", "Pumpet fra fartøy", "Starttidspunkt", "Stopptidspunkt"],
    keep="first"   # keeps the first occurrence
)

fmt = "%d.%m.%Y %H:%M:%S"
df_ers["Starttidspunkt"] = pd.to_datetime(df_ers["Starttidspunkt"], format=fmt)

df_ers = df_ers.loc[df_ers["Starttidspunkt"].between("2024-01-01", "2024-01-31 23:59:59")]

print(df_ers.shape)

df_ers["Radiokallesignal (ERS)"] = (
    df_ers["Radiokallesignal (ERS)"]
    .astype("string")
    .str.strip()
    .str.upper()
)

df_ers["Pumpet fra fartøy"] = (
    df_ers["Pumpet fra fartøy"]
    .astype("string")
    .str.strip()
    .str.upper()
)

df_ers["pair"] = df_ers.apply(
    lambda r: tuple(sorted([r["Radiokallesignal (ERS)"], r["Pumpet fra fartøy"]])),
    axis=1
)

df_ers.to_csv("sts_in_ers_jan.csv", index=False)

ers_pairs = set(df_ers["pair"])

matches = df_sts[df_sts["pair"].isin(ers_pairs)]
matches = matches.drop(columns=["run_id"])
print(matches.head())
print("Matched pairs:")
matches.to_csv("match_ais_ers_jan.csv", index=False)
print("Number of matches:", len(matches))


# Plot STS cases that i didnt find in ais


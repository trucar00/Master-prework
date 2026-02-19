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

print(df_sts.head())

df_ers = pd.read_csv("../Data/elektronisk-rapportering-ers-2024-fangstmelding-dca.csv", sep=";", 
                     encoding="utf-8", decimal=",", usecols=["Meldingstidspunkt", "Radiokallesignal (ERS)", 
                                                             "Fartøynavn (ERS)", "Pumpet fra fartøy", "Starttidspunkt", 
                                                             "Stopptidspunkt", "Aktivitet", "Varighet"])

df_ers = df_ers.dropna(subset=["Radiokallesignal (ERS)", "Pumpet fra fartøy", "Starttidspunkt", "Stopptidspunkt"])

df_ers = df_ers.drop_duplicates(
    subset=["Radiokallesignal (ERS)", "Pumpet fra fartøy", "Starttidspunkt", "Stopptidspunkt"],
    keep="first"   # keeps the first occurrence
)


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

ers_pairs = set(df_ers["pair"])

matches = df_sts[df_sts["pair"].isin(ers_pairs)]

print("Matched pairs:")
print(matches[["callsign1", "callsign2", "start_time", "end_time"]])
print("Number of matches:", len(matches))


# df_ers = df_ers[
#     df_ers["Radiokallesignal (ERS)"].isin(callsigns) |
#     df_ers["Pumpet fra fartøy"].isin(callsigns)
# ]



#df_ers.to_csv("sts_in_ais_and_registered.csv", index=False)

#print("Remaining rows:", len(df_ers))
#print("Unique callsigns:", df_ers["Radiokallesignal (ERS)"].unique())


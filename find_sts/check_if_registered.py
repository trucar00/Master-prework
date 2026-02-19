import pandas as pd

# Finds the callsigns that are close in proximity from AIS data (sts found in AIS) in the ERS
# some of them are yes, so we find true sts cases in the ais data!

callsigns = ['LKLV', 'LLAS', 'LLQX', 'LCGV', '3YVG', 'LMFZ', 'LLWG', 'LDIW', 'LALD', 'LMCW', 'JXVS', 'LJLR', 
             'LJDJ', 'LJZH', 'JXVS', 'LJLR', 'LIZI', 'LLAS', 'LEWL', 'LIZI', 'LCJG', 'LADH', 'LCJG', 'LADH', 
             'JWLM', 'LLWG', 'LDEF', 'LLAS', 'LLLP', 'LCGV', 'LCGV', 'LJZH', 'JXUE', 'LHEA', 'LCOV', 'LMFZ', 
             'LCUF', 'LFNM', 'LLWG', 'LJSY', 'LGSH', 'LMCW', 'LLMI', 'LLWF', 'LGSH', 'LLAS', 'JXVS', 'LJLR', 
             'JXVS', 'LJLR', 'JXVS', 'LJLR', '3YMI', 'LIRW', 'JXVS', 'LJLR', 'JXVS', 'LJLR', 'JXVS', 'LJLR', 
             'JXVS', 'LJLR', 'JXVS', 'LJLR', 'JXVS', 'LJLR', 'JXVS', 'LJLR']

callsigns = list(set(callsigns))


df_ers = pd.read_csv("../Data/elektronisk-rapportering-ers-2024-fangstmelding-dca.csv", sep=";", encoding="utf-8", decimal=",", 
                 usecols=["Meldingstidspunkt", "Radiokallesignal (ERS)", "Fartøynavn (ERS)", "Pumpet fra fartøy", 
                          "Starttidspunkt", "Stopptidspunkt", "Startposisjon bredde", "Startposisjon lengde", "Aktivitet", "Varighet"])

df_ers = df_ers.dropna(subset=["Radiokallesignal (ERS)", "Pumpet fra fartøy", "Meldingstidspunkt", "Starttidspunkt", "Stopptidspunkt", "Varighet"])

df_ers = df_ers.drop_duplicates(
    subset=["Radiokallesignal (ERS)", "Pumpet fra fartøy"],
    keep="first"   # keeps the first occurrence
)

df_ers["Radiokallesignal (ERS)"] = (
    df_ers["Radiokallesignal (ERS)"]
    .astype("string")
    .str.strip()
)

df_ers = df_ers[df_ers["Radiokallesignal (ERS)"].isin(callsigns)]

#print(df_ers.head(10))

df_ers.to_csv("sts_in_ais_and_registered.csv", index=False)

#print("Remaining rows:", len(df_ers))
#print("Unique callsigns:", df_ers["Radiokallesignal (ERS)"].unique())


import pandas as pd

YEAR = "2024"

ers_df = pd.read_csv(f"Data/elektronisk-rapportering-ers-{YEAR}-fangstmelding-dca.csv", sep=";", encoding="utf-8", decimal=",")
print(ers_df.dtypes)

ers_df = ers_df[["Fartøynavn (ERS)", "Fartøynasjonalitet (kode)", "Meldingstidspunkt", "Radiokallesignal (ERS)", "Aktivitet", "Starttidspunkt",
                 "Stopptidspunkt", "Varighet", "Startposisjon bredde", "Startposisjon lengde", "Stopposisjon bredde", 
                 "Stopposisjon lengde", "Hovedområde start (kode)", "Redskap - gruppe", "Redskap FAO","Redskap FDIR",  "Hovedart FAO"]]

before = len(ers_df)
ers_df = ers_df.dropna(subset=["Starttidspunkt", "Stopptidspunkt", "Redskap - gruppe", "Varighet"])
ers_df = ers_df[ers_df["Varighet"] > 0]
ers_df = ers_df.drop_duplicates(keep="first")

after = len(ers_df)

print(f"Dropped {before - after} rows ({(before-after)/before:.1%})")


fmt = "%d.%m.%Y %H:%M:%S"
ers_df["Starttidspunkt"] = pd.to_datetime(ers_df["Starttidspunkt"], format=fmt)

test = ers_df.loc[ers_df["Radiokallesignal (ERS)"] == "LEBW"].copy()
print("TESTY")
print(test[["Fartøynavn (ERS)", "Radiokallesignal (ERS)", "Starttidspunkt", "Redskap FAO"]].head())
print("----")


#ers_df.to_csv(f"Data/ers-fangstmelding-nonan-{YEAR}.csv", index=False)
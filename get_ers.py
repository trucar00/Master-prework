import pandas as pd

ers_df = pd.read_csv("Data/elektronisk-rapportering-ers-2024-fangstmelding-dca.csv", sep=";", encoding="utf-8", decimal=",")

ers_df = ers_df[["Fartøynavn (ERS)", "Meldingstidspunkt", "Radiokallesignal (ERS)", "Aktivitet", "Starttidspunkt",
                 "Stopptidspunkt", "Varighet", "Startposisjon bredde", "Startposisjon lengde", "Stopposisjon bredde", 
                 "Stopposisjon lengde", "Hovedområde start (kode)", "Redskap - gruppe", "Hovedart FAO"]]

before = len(ers_df)
ers_df = ers_df.dropna(subset=["Starttidspunkt", "Stopptidspunkt", "Redskap - gruppe", "Varighet"])
ers_df = ers_df[ers_df["Varighet"] > 0]
ers_df = ers_df.drop_duplicates(keep="first")

after = len(ers_df)

print(f"Dropped {before - after} rows ({(before-after)/before:.1%})")

ers_df.to_csv("Data/ers-fangstmelding-nonan.csv")
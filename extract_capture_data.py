import pandas as pd

df = pd.read_csv("Data/fangstdata_2024.csv", sep=";", encoding="utf-8", decimal=",")

df = df[["Fartøynavn", "Fartøy ID", "Radiokallesignal (seddel)",  "Fartøytype (kode)", "Fartøynasjonalitet (kode)", "Største lengde", 
         "Siste fangstdato", "Redskap", "Redskap - gruppe", "Fangstfelt (kode)", "Hovedområde (kode)", "Hovedområde", "Lon (hovedområde)", "Lat (hovedområde)", 
         "Nord/sør for 62 grader nord", "Landingsdato", "Art FAO", "Produktvekt"]]

df["Fartøy ID"] = df["Fartøy ID"].astype("Int64").astype("string")

df.to_csv("Data/fangstdata_2024_dropped.csv", index=False)

""" print(df["Redskap - gruppe"].unique())

for gear, d in df.groupby("Redskap - gruppe"):
    gear = gear.replace(" ", "_")
    d.to_csv(f"Data/{gear}_2024.csv") """
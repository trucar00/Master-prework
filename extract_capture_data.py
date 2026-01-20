import pandas as pd

df = pd.read_csv("Data/fangstdata_2024.csv", sep=";", encoding="utf-8", decimal=",")

df = df[["Fartøynavn", "Fartøy ID",  "Fartøytype (kode)", "Fartøynasjonalitet (kode)", "Største lengde", 
         "Siste fangstdato", "Redskap", "Redskap - gruppe", "Hovedområde (kode)", "Hovedområde", "Lon (hovedområde)", "Lat (hovedområde)", 
         "Nord/sør for 62 grader nord", "Landingsdato", "Art FAO", "Bruttovekt"]]

df["Fartøy ID"] = df["Fartøy ID"].astype("Int64").astype("string")

df.to_csv("Data/fangstdata_2024_dropped.csv", index=False)

print(df.info())
print(df.shape)
print(df.columns)
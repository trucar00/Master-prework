import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("Data/fangstdata_2024.csv", sep=";", encoding="utf-8", decimal=",")

df["Produktvekt"] = pd.to_numeric(df["Produktvekt"], errors="coerce")

gear_types = df["Redskap - gruppe"].unique()
print(gear_types)

totals_gear = (
    df.groupby("Redskap - gruppe", as_index=False)["Produktvekt"]
      .sum()
      .sort_values("Produktvekt", ascending=False)
)

totals_area = (
    df.groupby("Hovedområde (kode)", as_index=False)["Produktvekt"]
      .sum()
      .sort_values("Produktvekt", ascending=False)
)

print(totals_area)

plt.figure()
plt.bar(totals_gear["Redskap - gruppe"], totals_gear["Produktvekt"])
plt.xlabel("Redskap - gruppe")
plt.ylabel("Total produktvekt (kg)")
plt.title("Total fangst per redskapstype")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()
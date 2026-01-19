import pandas as pd

df = pd.read_csv("Data/fangstdata_2024.csv", sep=";", nrows=100, encoding="utf-8", decimal=",")

# DROP all unnecessary columns
# Save new csv to look at

print(df.head())
print(df.shape)
print(df.columns)
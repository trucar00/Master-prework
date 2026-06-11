import pandas as pd

df = pd.read_parquet("gear/not/02.parquet", engine="pyarrow")

print(df[df["speed"] < 0].head())
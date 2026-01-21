import pandas as pd

df = pd.read_parquet("Data/01-10-15.parquet", engine="pyarrow")

print(df.columns)
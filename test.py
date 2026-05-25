import pandas as pd

df = pd.read_parquet("ais_ers_sub_labels_01_2022.parquet", engine="pyarrow")
print(df["label"].unique())
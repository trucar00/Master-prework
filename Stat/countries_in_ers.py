import pandas as pd
import pyarrow.parquet as pq

def kjor(year):
    df = pd.read_csv(f"Data/ers-fangstmelding-nonan-{year}.csv")

    return df["Fartøynasjonalitet (kode)"].unique()

for y in range(2023, 2026):
    print(f"FOR THE YEAR OF: {y}")
    print(kjor(y))
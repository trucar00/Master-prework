import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


GEAR = "trawl"

dfs = []

for month in range(1, 2+1):
    path = Path(f"gear/{GEAR}/{month:02d}.parquet")
    if path.exists():
        print("Reading ", path)
        df = pd.read_parquet(path, engine="pyarrow")
        # Sample 10%
        df = df.sample(frac=0.10, random_state=42)
        dfs.append(df)
    else:
        print(path, " does not exist.")

all_df = pd.concat(dfs, ignore_index=True)
print(all_df.shape)


all_df = all_df.dropna(subset=["speed"])
all_df = all_df[(all_df["speed"] > 0) & (all_df["speed"] < 20)]

plt.figure(figsize=(8,5))

sns.kdeplot(
    all_df["speed"],
    fill=False,
    label=GEAR
)

plt.xlabel("Speed")
plt.ylabel("Density")
plt.title(f"Speed density distribution {GEAR}")

plt.legend()
plt.show()
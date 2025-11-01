import pandas as pd
import sys
import os

csv_file = "../../data/input/channels.csv"
json_file = os.path.splitext(csv_file)[0] + ".json"

df = pd.read_csv(csv_file)
df.to_json(json_file, orient="records", indent=2)

print(f"Converted {csv_file} → {json_file} ({len(df)} records).")

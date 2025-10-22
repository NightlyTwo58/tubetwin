import pandas as pd
import sys
import os

# change this or pass via command line
csv_file = "channels.csv"
json_file = os.path.splitext(csv_file)[0] + ".json"

df = pd.read_csv(csv_file)
df.to_json(json_file, orient="records", indent=2)

print(f"Converted {csv_file} → {json_file} ({len(df)} records).")

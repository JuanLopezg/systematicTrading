import pandas as pd

# Read the Parquet file
df = pd.read_parquet("dailyTop50ohclv_hl.parquet")

# Filter for symbol "PUMP" and select only 'ts' and 'market_cap' columns
pump_df = df[df["symbol"] == "PUMP"][["ts", "market_cap"]]

# Print the tail
print(pump_df.tail())

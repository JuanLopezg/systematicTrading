import pandas as pd

# Load the Parquet file
df = pd.read_parquet("dailyTop50ohclv_hl.parquet")

# Print the first few rows
print(df.head())

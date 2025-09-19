import pandas as pd
import plotly.graph_objects as go

# Load the Parquet file
df = pd.read_parquet("dailyTop50ohclv_hl.parquet")

# Print the tail
print(df.tail())

# Filter rows where symbol is "PUMP"
pump_df = df[df['symbol'] == 'BTC']

# Save to CSV
pump_df.to_csv('BTC.csv', index=False)

print(f"Saved {len(pump_df)} rows with symbol 'PUMP' to pump_data.csv")
print("Columns in the saved file:", pump_df.columns.tolist())
import pandas as pd
import plotly.graph_objects as go

# Load the Parquet file
df = pd.read_parquet("dailyTop50ohclv_hl.parquet")

# Print the first few rows
print(df.tail())

# Filter rows where column 'id' equals "1"
df_close_id1 = df.loc[df["id"] == 1, "close"]

print(df_close_id1)


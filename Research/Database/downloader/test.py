import pandas as pd
import plotly.graph_objects as go

# Load the Parquet file
df = pd.read_parquet("dailyTop50ohclv_hl.parquet")

# Print the first few rows
print(df.tail())

# Filter rows where column 'id' equals "1"
df_close_id1 = df.loc[df["id"] == 1, "close"]

print(df_close_id1)




df_id1 = df[df["id"] == 1].copy()

if df_id1.empty:
    print("⚠️ No data found for id == 1")
else:
    df_id1["ts"] = pd.to_datetime(df_id1["ts"])
    df_id1 = df_id1.sort_values("ts")

    # Make a simple candlestick chart
    fig = go.Figure(go.Candlestick(
        x=df_id1["ts"],
        open=df_id1["open"],
        high=df_id1["high"],
        low=df_id1["low"],
        close=df_id1["close"],
        name="ID 1"
    ))

    fig.update_layout(
        title="Candlestick Chart for ID 1",
        xaxis_title="Date",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False
    )

    # 👇 this shows the chart instead of printing JSON
    fig.show()
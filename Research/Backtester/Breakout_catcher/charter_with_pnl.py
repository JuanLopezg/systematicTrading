import pandas as pd
import numpy as np
import gc

import matplotlib.pyplot as plt

df = pd.read_parquet("./systematictrading/Research/Backtester/backtests/btc_breakout_catcher.parquet")

# Make sure you’re only plotting one id
# Example: df = df[df['id'] == 1].copy()

# Clean up and ensure sorted by time
df = df.dropna(subset=['close', '20dHigh']).sort_values('ts').copy()

plt.figure(figsize=(14, 7))

# Plot Close price
plt.plot(df['ts'], df['close'], label='Close', color='black', linewidth=1.3)

# Plot 20-day High
plt.plot(df['ts'], df['20dHigh'], label='20d High', color='orange', linestyle='--', linewidth=1.2)

# Plot Stop Loss (SL3ATR)
plt.plot(df['ts'], df['SL3ATR'], label='Trailing Stop (3×ATR)', color='red', linestyle=':')

# Mark entries (green triangles)
entries = df.loc[df['entry']]
plt.scatter(entries['ts'], entries['close'],
            marker='^', color='green', s=100, label='Entry', zorder=5)

# Mark exits (red triangles)
exits = df.loc[df['exit']]
plt.scatter(exits['ts'], exits['close'],
            marker='v', color='red', s=100, label='Exit', zorder=5)

# Chart aesthetics
plt.title('Backtest Visualization – Close, 20dHigh, Entries/Exits, and SL3ATR', fontsize=14)
plt.xlabel('Date')
plt.ylabel('Price')
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()
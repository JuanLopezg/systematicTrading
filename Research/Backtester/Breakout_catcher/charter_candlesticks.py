import pandas as pd
import numpy as np
import gc

import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.offline as pyo
import pandas as pd

df = pd.read_parquet("./systematictrading/Research/Backtester/backtests/btc_breakout_catcher.parquet")


# Assume df is already filtered to one id
# and has lowercase columns: open, high, low, close, ts, 20dHigh, SL3ATR, entry, exit

df['ts'] = pd.to_datetime(df['ts'])
df = df.sort_values('ts')

fig = go.Figure()

# Candlesticks
fig.add_trace(go.Candlestick(
    x=df['ts'],
    open=df['open'],
    high=df['high'],
    low=df['low'],
    close=df['close'],
    name='Price'
))

# 20-day high
fig.add_trace(go.Scatter(
    x=df['ts'], y=df['20dHigh'],
    mode='lines', name='20d High',
    line=dict(color='orange', width=1.5, dash='dot')
))

# Trailing stop
fig.add_trace(go.Scatter(
    x=df['ts'], y=df['SL3ATR'],
    mode='lines', name='Trailing Stop (3×ATR)',
    line=dict(color='red', width=1.3, dash='dash')
))

# Entries
entries = df[df['entry']]
fig.add_trace(go.Scatter(
    x=entries['ts'], y=entries['close'],
    mode='markers', name='Entry',
    marker=dict(symbol='triangle-up', color='green', size=10)
))

# Exits
exits = df[df['exit']]
fig.add_trace(go.Scatter(
    x=exits['ts'], y=exits['close'],
    mode='markers', name='Exit',
    marker=dict(symbol='triangle-down', color='red', size=10)
))

fig.update_layout(
    title='Backtest – Candlestick Chart with 20dHigh, Entries, Exits & Trailing SL',
    xaxis_title='Date',
    yaxis_title='Price',
    xaxis_rangeslider_visible=False,
    template='plotly_white',
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
)

# --- this line saves and opens automatically in your browser ---
pyo.plot(fig, filename='backtest_chart.html', auto_open=True)

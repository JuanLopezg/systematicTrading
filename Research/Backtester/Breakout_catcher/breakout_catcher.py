import pandas as pd
import numpy as np
import gc
from numba import njit

df = pd.read_parquet("./systematictrading/Research/Database/historical_data/db/dailyOHLCV.par")

# clean dataset to just get our universe
df.drop(columns=["stat", "end", "days", "market_cap"], inplace=True)
df["rank"] = df.groupby("ts")["volume"].rank(method="first", ascending=False)
top_ids = df.loc[df["rank"] <= 50, "id"].unique()
df = df[df["id"].isin(top_ids)].copy()
del top_ids
gc.collect()
df.sort_values(['id', 'ts'], inplace=True)


# Calculate 20dHighs
df['20dHigh'] = (
    df.groupby('id')['high']
      .transform(lambda x: x.shift(1).rolling(window=20, min_periods=20).max())
)
# Entry signals
df['signal'] = df['close'] > df['20dHigh']


df['ATR14'] = (
    df.groupby('id', group_keys=False)
      .apply(lambda g: pd.concat([
          g['high'] - g['low'],
          (g['high'] - g['close'].shift(1)).abs(),
          (g['low'] - g['close'].shift(1)).abs()
      ], axis=1).max(axis=1)
        .rolling(14, min_periods=14).mean())
)



# ---------------------------------------------------------------------
# Trailing-stop logic (Numba-accelerated)
# ---------------------------------------------------------------------
@njit
def trailing_sl_loop(close, atr, signal, k=3.0):
    n = len(close)
    entry = np.zeros(n, dtype=np.bool_)
    exit_ = np.zeros(n, dtype=np.bool_)
    pos = np.zeros(n, dtype=np.bool_)
    sl = np.full(n, np.nan)

    in_trade = False
    cur_sl = np.nan

    for i in range(n):
        c = close[i]
        a = atr[i]

        if not in_trade:
            # enter on first valid signal when flat
            if signal[i] and not np.isnan(a):
                in_trade = True
                entry[i] = True
                pos[i] = True
                cur_sl = c - k * a
                sl[i] = cur_sl
        else:
            # update trailing stop
            cand = c - k * a if not np.isnan(a) else np.nan
            if np.isnan(cur_sl):
                cur_sl = cand
            elif not np.isnan(cand):
                cur_sl = max(cur_sl, cand)

            sl[i] = cur_sl

            # exit if stop hit
            if c < cur_sl:
                exit_[i] = True
                in_trade = False
                pos[i] = False
                cur_sl = np.nan
            else:
                pos[i] = True

    return entry, exit_, pos, sl


def compute_with_numba(g, k=3.0):
    e, x, p, s = trailing_sl_loop(
        g['close'].to_numpy(),
        g['ATR14'].to_numpy(),
        g['signal'].to_numpy(),
        k
    )
    g['entry'], g['exit'], g['in_position'], g['SL3ATR'] = e, x, p, s
    return g


df = df.groupby('id', group_keys=False).apply(compute_with_numba)

# ---------------------------------------------------------------------
# Done: df now has columns
#   20dHigh, signal, ATR14, entry, exit, in_position, SL3ATR
# ---------------------------------------------------------------------

print(df[['id', 'ts', 'close', '20dHigh', 'signal', 'ATR14', 'entry', 'exit', 'in_position', 'SL3ATR']].head(50))

#df = df[df["id"] == 1027].copy() # just save for one asset to check
# Save to a local Parquet file
df.to_parquet("./systematictrading/Research/Backtester/backtests/breakout_catcher.par", index=False)











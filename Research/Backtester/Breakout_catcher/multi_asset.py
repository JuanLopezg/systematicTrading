"""
Volatility-based position sizing backtest
-----------------------------------------

- Each trade size = closed_capital * risk_fraction / (k * ATR)
- Closed capital updates only on trade exit
- Tracks size, closed capital, unrealized PnL, and total equity
- Fast: Numba JIT for per-row calculations
"""

import pandas as pd
import numpy as np
from numba import njit
import gc
import matplotlib.pyplot as plt



# ============================================================
# Parameters
# ============================================================

FILE = "./systematictrading/Research/Backtester/backtests/breakout_catcher.par"

initial_capital = 100_000.0
risk_fraction = 0.1       # fraction of closed equity used for next trade (per asset)
k = 3.0                   # ATR multiplier for sizing
nAssets = 10              # number of total max positions each time
threshold = 0.2           # % of move in the ATR to trigger rebalancing


# ============================================================
# Load data
# ============================================================

df = pd.read_parquet(FILE)
df = df.sort_values("ts").reset_index(drop=True)
df["ts"] = pd.to_datetime(df["ts"])       # ensure ts is a datetime dtype
df = df[df["ts"] >= "2020-01-01"].copy()

# Ensure boolean columns
for c in ["entry", "exit", "in_position"]:
    df[c] = df[c].astype(bool)


# ============================================================
# Numba JIT function
# ============================================================

@njit
def compute_with_equity(entry, exit_, close, atr, k, threshold):
    """
    Compute daily position size, closed capital, unrealized PnL, and equity.

    Args:
        entry (bool array): Entry signal
        exit_ (bool array): Exit signal
        close (float array): Close prices
        atr (float array): ATR values
        k (float): ATR multiplier
        threshold (float): % of move in the ATR to trigger rebalancing
    Returns:
        tuple of arrays: (size, closed_capital, unrealized_pnl, equity)
    """
    n = len(close)
    size_scales = np.zeros(n)
    entry_prices = np.zeros(n)
    
    in_pos = False
    entry_price = 0.0
    units = 0.0
    reference_atr = 0.0

    for i in range(n):
        if entry[i] and not in_pos:
            # --- new position ---
            units = 1 / (k * atr[i]) if atr[i] > 0 else 0.0
            reference_atr = atr[i]
            entry_price = close[i]
            entry_prices[i] = entry_price
            in_pos = True
            size_scales[i] = units

        elif in_pos:

            if exit_[i]:
                # --- close position ---
                in_pos = False
                size_scales[i] = units
                units = 0.0
                entry_prices[i] = entry_price
                entry_price = 0.0
            else :
                # modify existing position in case the volatility changes more than threshold %
                if abs(reference_atr - atr[i]) > threshold*reference_atr :
                    prev_units = units
                    units = 1 / (k * atr[i]) if atr[i] > 0 else 0.0
                    reference_atr = atr[i]
                    size_scales[i] = units
                    entry_prices[i] = entry_price
                    if  (size_scales[i-1]-size_scales[i]) < 0: # buying units
                        entry_price = (entry_price * prev_units + close[i] * (units-prev_units)) / units
                        entry_prices[i] = entry_price
                else:
                    size_scales[i] = units
                    entry_prices[i] = entry_price


        else:
            # --- no position ---
            size_scales[i] = 0.0


    return size_scales, entry_prices

# ============================================================
# Wrapper to run Numba sizing per group
# ============================================================

def compute_with_numba(g, k=3.0, threshold=0.2):
    """
    Apply the Numba ATR-based position sizing to one asset group (g).
    """
    size_scale, entry_price = compute_with_equity(
        g["entry"].to_numpy(dtype=np.bool_),
        g["exit"].to_numpy(dtype=np.bool_),
        g["close"].to_numpy(np.float64),
        g["ATR14"].to_numpy(np.float64),
        k,
        threshold
    )
    g = g.copy()
    g["size_scale"] = size_scale
    g["entry_price"] = entry_price
    return g


# ============================================================
# Run per-asset computation
# ============================================================

df = df.groupby("id", group_keys=False, sort=False).apply(compute_with_numba, k=k, threshold=threshold)

gc.collect()


df = df.sort_values(["ts", "rank"]).reset_index(drop=True) # this will be the ranking used for each ts






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

FILE = "./systematictrading/Research/Backtester/backtests/btc_breakout_catcher.parquet"

initial_capital = 100_000.0
risk_fraction = 0.1       # fraction of closed equity used for next trade
k = 3.0                   # ATR multiplier for sizing


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
def compute_with_equity(entry, exit_, close, atr, k, capital0, risk_fraction):
    """
    Compute daily position size, closed capital, unrealized PnL, and equity.

    Args:
        entry (bool array): Entry signal
        exit_ (bool array): Exit signal
        close (float array): Close prices
        atr (float array): ATR values
        k (float): ATR multiplier
        capital0 (float): Initial capital
        risk_fraction (float): Fraction of closed equity to risk each trade
    Returns:
        tuple of arrays: (size, closed_capital, unrealized_pnl, equity)
    """
    n = len(close)
    size = np.zeros(n)
    capital_arr = np.zeros(n)
    unrealized = np.zeros(n)
    equity = np.zeros(n)

    capital = capital0
    in_pos = False
    entry_price = 0.0
    units = 0.0

    for i in range(n):
        if entry[i] and not in_pos:
            # --- new position ---
            risk_capital = capital * risk_fraction
            units = risk_capital / (k * atr[i]) if atr[i] > 0 else 0.0
            entry_price = close[i]
            in_pos = True
            size[i] = units

        elif in_pos:
            # --- maintain existing position ---
            size[i] = units

            if exit_[i]:
                # --- close position ---
                pnl = (close[i] - entry_price) * units
                capital += pnl
                in_pos = False
                units = 0.0
                entry_price = 0.0

        else:
            # --- no position ---
            size[i] = 0.0

        # --- per-row bookkeeping ---
        capital_arr[i] = capital
        unrealized[i] = (close[i] - entry_price) * units if in_pos else 0.0
        equity[i] = capital + unrealized[i]

    return size, capital_arr, unrealized, equity


# ============================================================
# Run computation
# ============================================================

df["size"], df["capital"], df["unrealized_pnl"], df["equity"] = compute_with_equity(
    df["entry"].to_numpy(dtype=np.bool_),
    df["exit"].to_numpy(dtype=np.bool_),
    df["close"].to_numpy(np.float64),
    df["ATR14"].to_numpy(np.float64),
    k,
    initial_capital,
    risk_fraction,
)

# Set zero when not in position (safety)
df.loc[~df["in_position"], "size"] = 0.0

gc.collect()

# ============================================================
# Output / inspection
# ============================================================

# Example summary stats
print("\nFinal capital:", round(df["capital"].iloc[-1], 2))
print("Final equity :", round(df["equity"].iloc[-1], 2))
print("Total trades :", df["entry"].sum())


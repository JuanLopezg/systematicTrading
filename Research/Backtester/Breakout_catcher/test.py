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
def compute_with_equity(entry, exit_, close, atr, k, capital0, risk_fraction, threshold):
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
        threshold (float): % of move in the ATR to trigger rebalancing
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
    reference_atr = 0.0

    for i in range(n):
        if entry[i] and not in_pos:
            # --- new position ---
            risk_capital = capital * risk_fraction
            units = risk_capital / (k * atr[i]) if atr[i] > 0 else 0.0
            reference_atr = atr[i]
            entry_price = close[i]
            in_pos = True
            size[i] = units

        elif in_pos:

            if exit_[i]:
                # --- close position ---
                pnl = (close[i] - entry_price) * units
                capital += pnl
                in_pos = False
                units = 0.0
                entry_price = 0.0
                size[i] = 0
            else :
                # modify existing position in case the volatility changes more than threshold %
                if abs(reference_atr - atr[i]) > threshold*reference_atr :
                    prev_units = units
                    units = risk_capital / (k * atr[i]) if atr[i] > 0 else 0.0
                    reference_atr = atr[i]
                    size[i] = units
                    if  (size[i-1]-size[i]) > 0: # selling units
                        capital += (size[i-1]-size[i]) *  (close[i] - entry_price) 
                    else: # buying units
                        entry_price = (entry_price * prev_units + close[i] * (units-prev_units)) / units
                else:
                    size[i] = units


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

grouped = df.groupby('id')

for group_id, g in grouped:
    
    symbol = g["symbol"].iloc[0]

    g["size"], g["capital"], g["unrealized_pnl"], g["equity"] = compute_with_equity(
        g["entry"].to_numpy(dtype=np.bool_),
        g["exit"].to_numpy(dtype=np.bool_),
        g["close"].to_numpy(np.float64),
        g["ATR14"].to_numpy(np.float64),
        k,
        initial_capital,
        risk_fraction,
        threshold
    )

    # Set zero when not in position (safety)
    g.loc[~g["in_position"], "size"] = 0.0

    gc.collect()

    # ============================================================
    # Output / inspection
    # ============================================================

    # Example summary stats
    print("\nFinal capital:", round(g["capital"].iloc[-1], 2))
    print("Final equity :", round(g["equity"].iloc[-1], 2))
    print("Total trades :", g["entry"].sum())


    # ============================================================
    # Plotting (single y-axis: BTC vs scaled equity and capital)
    # ============================================================

    fig, ax = plt.subplots(figsize=(14, 7))

    # --- Normalize equity & capital so both start at BTC price on the first day ---
    if "equity" in g.columns:
        first_price = g["close"].iloc[0]
        g["equity_scaled"] = g["equity"] 

    if "capital" in g.columns:
        g["capital_scaled"] = g["capital"]

    # --- BTC price series ---
    ax.plot(g["ts"], g["close"], label="BTC Close", color="black", linewidth=1.3)

    # --- Optional overlays ---
    if "20dHigh" in g.columns:
        ax.plot(g["ts"], g["20dHigh"], label="20-day High", color="orange",
                linestyle="--", linewidth=1.2)
    if "SL3ATR" in g.columns:
        ax.plot(g["ts"], g["SL3ATR"], label="Trailing Stop (3×ATR)",
                color="red", linestyle=":")

    # --- Scaled equity curve ---
    if "equity_scaled" in g.columns:
        ax.plot(g["ts"], g["equity_scaled"], color="blue",
                linewidth=1.5, alpha=0.8, label="Equity (scaled)")

    # --- Scaled capital curve (closed P&L only) ---
    if "capital_scaled" in g.columns:
        ax.plot(g["ts"], g["capital_scaled"], color="green",
                linewidth=1.5, alpha=0.7, linestyle="--", label="Closed Capital (scaled)")

    # --- Entry / Exit markers ---
    if "entry" in g.columns:
        entries = g.loc[g["entry"]]
        ax.scatter(entries["ts"], entries["close"],
                marker="^", color="green", s=90, label="Entry", zorder=5)
    if "exit" in g.columns:
        exits = g.loc[df["exit"]]
        ax.scatter(exits["ts"], exits["close"],
                marker="v", color="red", s=90, label="Exit", zorder=5)

    # --- Aesthetics ---
    ax.set_xlabel("Date")
    ax.set_ylabel("BTC Price / Scaled Equity & Capital")
    ax.grid(alpha=0.3)
    ax.legend(loc="upper left", fontsize=9)
    plt.title(f"{symbol} vs Strategy – Price, Scaled Equity, and Closed Capital", fontsize=14)
    plt.tight_layout()
    plt.show()
    
    cont = input("Continue to next group? (y/n): ").strip().lower()
    if cont not in ["y", "yes"]:
        print("Stopping iteration.")
        break

import requests
import json
import pandas as pd
import time
import pandas as pd
from pathlib import Path
from datetime import datetime

import hl_api as hl_api
import cmc_api as cmc_api


def load_top50_history(basename="tracked_coins"):
    """
    Load existing history of 'daysOutOfTop50' from a CSV file with a date suffix.
    Example filename: tracked_coins_2025-09-06.csv
    
    - If no such file exists, return empty DataFrame with required columns.
    - If today's file already exists, raise an exception (to avoid overwriting).
    - Otherwise, load the latest available file.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")

    # Chercher tous les fichiers qui matchent le pattern
    files = sorted(Path(".").glob(f"{basename}_*.csv"))

    if not files:
        # Aucun fichier trouvé → dataframe vide
        return pd.DataFrame(columns=["id", "symbol", "daysOutOfTop50"])

    # On prend le plus récent (lexicographiquement ça marche car YYYY-MM-DD)
    latest_file = files[-1]

    # Extraire la date du nom de fichier
    try:
        file_date = latest_file.stem.split("_")[-1]  # ex: "2025-09-06"
    except IndexError:
        raise ValueError(f"Filename {latest_file} does not contain a valid date suffix.")

    # Vérifier si c’est aujourd’hui
    if file_date == today_str:
        raise RuntimeError(f"A history file for today already exists: {latest_file}")

    # Charger le fichier
    df = pd.read_csv(latest_file)

    # Vérifier colonnes requises
    required = {"id", "symbol", "daysOutOfTop50"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{latest_file} is missing required columns: {missing}")

    return df


def save_top50_history(df, basename="tracked_coins"):
    """Save the DataFrame to a CSV file with today's date in the filename."""
    today_str = datetime.now().strftime("%Y-%m-%d")  # ex: 2025-09-06
    filename = f"{basename}_{today_str}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved history to {filename}")

    

def matches(symbol, symbols_list, mode="exact"):
    """
    Compare a symbol (e.g., 'BTC') against the base currency of each pair.
    Strips out known quote/contract suffixes from symbol names before matching.
    Handles variations like:
    - BTCUSDT
    - BTC-USDT
    - BTC_USDT
    - KPEPEUSDT (matches 'PEPE')
    - 1000PEPEUSDT (matches 'PEPE')
    """

    suffixes = (
        "-USDT", "-USDC", "-USD",
        "_USDT", "_USDC", "_USD",
        "USDT", "USDC", "USD"
    )
    bases = set()
    for s in symbols_list:
        s_up = s.upper()
        base = s_up
        for suf in suffixes:
            if s_up.endswith(suf):
                base = s_up[: -len(suf)]
                break
        bases.add(base)

    symbol_up = symbol.upper()
    
    if mode == "exact":
        # Match if base == symbol, or base endswith(symbol)
        return any(base == symbol_up or base.endswith(symbol_up) for base in bases)
    elif mode == "startswith":
        return any(base.startswith(symbol_up) for base in bases)
    elif mode == "contains":
        return any(symbol_up in base for base in bases)
    else:
        raise ValueError("Invalid match mode.")


    


if __name__ == "__main__":
    
    # -------------------------------------------------------------------------------------------------------------
    #   Check actual top 50 of CoinMarketCap
    #   Store new coins in CSV tracker of top 50
    #   Delete coins that have been out of Top 50 for 100 days
    # -------------------------------------------------------------------------------------------------------------

    # Step 1: Fetch data from CMC
    df_top75 = cmc_api.get_top_n(100)
    df_meta = cmc_api.get_metadata(df_top75["id"].tolist())

    df_merged = df_meta.merge(df_top75, on="id", how="inner")
    df_no_stable = df_merged[~df_merged["stablecoin"]]

    df_top50_full = (
        df_no_stable.sort_values("cmc_rank", ascending=True)
        .head(50)
        .loc[:, ["id", "symbol", "market_cap"]]
        .reset_index(drop=True)
    )

    # Step 2: Load CSV history
    df_hist = load_top50_history("tracked_coins")

    # Normalize dtypes
    if not df_hist.empty:
        df_hist["id"] = df_hist["id"].astype(int)
        df_hist["daysOutOfTop50"] = df_hist["daysOutOfTop50"].astype(int)

    todays_ids = set(df_top50_full["id"].astype(int))

    if df_hist.empty:
        # Initialize from scratch
        df_hist = pd.DataFrame({
            "id": sorted(todays_ids),
            "symbol": df_top50_full.set_index("id").loc[sorted(todays_ids), "symbol"].values,
            "daysOutOfTop50": 0
        })
    else:
        # Reset to 0 if in today's Top 50
        mask_in = df_hist["id"].isin(todays_ids)
        df_hist.loc[mask_in, "daysOutOfTop50"] = 0

        # Increment if not in today's Top 50
        mask_out = ~mask_in
        df_hist.loc[mask_out, "daysOutOfTop50"] += 1

        # Drop coins that have been out for 100 days
        df_hist = df_hist[df_hist["daysOutOfTop50"] < 100].reset_index(drop=True)

        # Add new coins to history
        hist_ids = set(df_hist["id"])
        new_ids = sorted(todays_ids - hist_ids)
        if new_ids:
            symbol_map = df_top50_full.set_index("id")["symbol"].to_dict()
            df_new = pd.DataFrame({
                "id": new_ids,
                "symbol": [symbol_map.get(i, "") for i in new_ids],
                "daysOutOfTop50": 0
            })
            df_hist = pd.concat([df_hist, df_new], ignore_index=True)

    # Optional: Sort by longest out of Top 50
    df_hist = df_hist.sort_values(["daysOutOfTop50", "id"], ascending=[True, True]).reset_index(drop=True)

    # Step 3: Save updated history
    save_top50_history(df_hist, "tracked_coins")

    print(f"Top 50 tracker updated. Now tracking {len(df_hist)} coins.")



    
    #-------------------------------------------------------------------------------------------------------------
    #   Download missing OHCL hourly data for all coins (Hyperliquid)
    #-------------------------------------------------------------------------------------------------------------

    # Liste des symboles (ceux que tu veux tracker, ex: top50)
    symbols = df_hist["symbol"].tolist()

    # Hyperliquid : récupérer les perps actifs
    hl_rows  = hl_api.hyperliquid_active_perps()
    hl_perps = [r["perp"] for r in hl_rows]  

    # Filtrer df_hist pour ne garder que les coins présents dans Hyperliquid
    df_hl_only = df_hist[df_hist["symbol"].apply(lambda sym: matches(sym, hl_perps, mode="exact"))].reset_index(drop=True)


    print(f"✅ {len(df_hl_only)} symbols found in Hyperliquid")
    print(df_hl_only)
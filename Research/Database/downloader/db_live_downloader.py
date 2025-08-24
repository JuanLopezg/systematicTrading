import cmc_api
import pandas as pd
from pathlib import Path
import hl_api
import binance_api
import okx_api
import bitfinex_api
import bybit_api
import coinbase_api

def load_top50_history(basename="top50_100d"):
    """
    Load existing history of 'daysOutOfTop50' from a CSV file.
    Returns a DataFrame with columns: id, symbol, daysOutOfTop50.
    """
    path = Path(f"{basename}.csv")
    if path.exists():
        df = pd.read_csv(path)

        # Ensure required columns exist
        required = {"id", "symbol", "daysOutOfTop50"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"{path} is missing required columns: {missing}")
        
        return df
    else:
        # Return empty DataFrame with proper columns
        return pd.DataFrame(columns=["id", "symbol", "daysOutOfTop50"])


def save_top50_history(df, basename="top50_100d"):
    """Save the DataFrame to a CSV file (overwrite if exists)."""
    df.to_csv(f"{basename}.csv", index=False)
    
    


if __name__ == "__main__":
    
    # -------------------------------------------------------------------------------------------------------------
    #   Check actual top 50 of CoinMarketCap
    #   Store new coins in CSV tracker of top 50
    #   Delete coins that have been out of Top 50 for 100 days
    # -------------------------------------------------------------------------------------------------------------

    # Step 1: Fetch data from CMC
    df_top75 = cmc_api.get_top_n(75)
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
    df_hist = load_top50_history("top50_100d")

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
    save_top50_history(df_hist, "top50_100d")

    print(f"Top 50 tracker updated. Now tracking {len(df_hist)} coins.")



    
    #-------------------------------------------------------------------------------------------------------------
    #   Download missing OHCL hourly data for all coins (Binance and Hyperliquid as sources)
    #-------------------------------------------------------------------------------------------------------------

    coinsIDs = df_hist["id"].tolist()

   
    # -----------------------------------------------
    # Define helper: match symbol if it's a prefix or substring
    def matches(symbol, symbols_list, mode="startswith"):
        if mode == "startswith":
            return any(s.startswith(symbol) for s in symbols_list)
        elif mode == "contains":
            return any(symbol in s for s in symbols_list)
        else:
            raise ValueError("Invalid match mode.")

    # -----------------------------------------------
    # 1. Hyperliquid (match by contains)
    hl_perps = [row["perp"] for row in hl_api.hyperliquid_active_perps()]
    df_hist["on_hl"] = df_hist["symbol"].apply(lambda sym: matches(sym, hl_perps, mode="contains"))

    # 2. Binance (only where not on HL)
    bn_pairs = binance_api.get_all_binance_usd_pairs()
    df_hist["on_bn"] = False
    mask_bn = ~df_hist["on_hl"]
    df_hist.loc[mask_bn, "on_bn"] = df_hist.loc[mask_bn, "symbol"].apply(
        lambda sym: matches(sym, bn_pairs, mode="startswith")
    )

    # 3. OKX (only where not on HL or BN)
    okx_pairs = okx_api.get_okx_usd_pairs()
    df_hist["on_okx"] = False
    mask_okx = ~df_hist["on_hl"] & ~df_hist["on_bn"]
    df_hist.loc[mask_okx, "on_okx"] = df_hist.loc[mask_okx, "symbol"].apply(
        lambda sym: matches(sym, okx_pairs, mode="startswith")
    )

    # 4. Bitfinex (only where not on HL/BN/OKX)
    bitfinex_pairs = bitfinex_api.get_bitfinex_usd_pairs()
    df_hist["on_bitfinex"] = False
    mask_bitfinex = ~df_hist["on_hl"] & ~df_hist["on_bn"] & ~df_hist["on_okx"]
    df_hist.loc[mask_bitfinex, "on_bitfinex"] = df_hist.loc[mask_bitfinex, "symbol"].apply(
        lambda sym: matches(sym, bitfinex_pairs, mode="startswith")
    )

    # 5. Bybit (only where not on HL/BN/OKX/BFX)
    bybit_pairs = bybit_api.get_bybit_usd_pairs()
    df_hist["on_bybit"] = False
    mask_bybit = ~df_hist["on_hl"] & ~df_hist["on_bn"] & ~df_hist["on_okx"] & ~df_hist["on_bitfinex"]
    df_hist.loc[mask_bybit, "on_bybit"] = df_hist.loc[mask_bybit, "symbol"].apply(
        lambda sym: matches(sym, bybit_pairs, mode="startswith")
    )

    # 6. Coinbase (only where not on HL/BN/OKX/BFX/Bybit)
    coinbase_pairs = coinbase_api.get_coinbase_usd_pairs()
    df_hist["on_cb"] = False
    mask_cb = ~df_hist["on_hl"] & ~df_hist["on_bn"] & ~df_hist["on_okx"] & ~df_hist["on_bitfinex"] & ~df_hist["on_bybit"]
    df_hist.loc[mask_cb, "on_cb"] = df_hist.loc[mask_cb, "symbol"].apply(
        lambda sym: matches(sym, coinbase_pairs, mode="startswith")
    )

    # Optional: create a single 'venue' column based on first appearance
    df_hist["venue"] = "none"
    df_hist.loc[df_hist["on_hl"], "venue"] = "hyperliquid"
    df_hist.loc[~df_hist["on_hl"] & df_hist["on_bn"], "venue"] = "binance"
    df_hist.loc[~df_hist["on_hl"] & ~df_hist["on_bn"] & df_hist["on_okx"], "venue"] = "okx"
    df_hist.loc[~df_hist["on_hl"] & ~df_hist["on_bn"] & ~df_hist["on_okx"] & df_hist["on_bitfinex"], "venue"] = "bitfinex"
    df_hist.loc[~df_hist["on_hl"] & ~df_hist["on_bn"] & ~df_hist["on_okx"] & ~df_hist["on_bitfinex"] & df_hist["on_bybit"], "venue"] = "bybit"
    df_hist.loc[~df_hist["on_hl"] & ~df_hist["on_bn"] & ~df_hist["on_okx"] & ~df_hist["on_bitfinex"] & ~df_hist["on_bybit"] & df_hist["on_cb"], "venue"] = "coinbase"

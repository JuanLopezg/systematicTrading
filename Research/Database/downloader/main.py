import requests
import json
import pandas as pd
import time
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

import downloader as d
import hl_api as hl_api
import cmc_api as cmc_api


if __name__ == "__main__":
    
    # -------------------------------------------------------------------------------------------------------------
    #   Check actual top 50 of CoinMarketCap
    #   Store new coins in CSV tracker of top 50 in Hyperliquid
    #   Delete coins that have been out of Top 50 for 200 days
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
    
    
    
    # narrow just to those ones listed on hyperliquid------------------------------------------------------------
    
    # Liste des symboles (ceux que tu veux tracker, ex: top50)
    symbols = df_top50_full["symbol"].tolist()

    # Hyperliquid : récupérer les perps actifs
    hl_rows  = hl_api.hyperliquid_active_perps()
    hl_perps = [r["perp"] for r in hl_rows]  

    # Filtrer df_hist pour ne garder que les coins présents dans Hyperliquid
    df_hl = df_top50_full[df_top50_full["symbol"].apply(lambda sym: d.matches(sym, hl_perps))].reset_index(drop=True)

    # Ajouter une colonne "perp" avec les noms des perps HL correspondants
    df_hl["perp"] = df_hl["symbol"].apply(lambda sym: d.find_matching_perp(sym, hl_perps))
    
    df_id_perp = df_hl[["id", "perp"]]

    # Step 2: Load CSV history
    df_hist = d.load_top50_history("tracked_coins")

    # Normalize dtypes
    if not df_hist.empty:
        df_hist["id"] = df_hist["id"].astype(int)
        df_hist["daysOutOfTop50"] = df_hist["daysOutOfTop50"].astype(int)

    todays_ids = set(df_hl["id"].astype(int))

    if df_hist.empty:
        # Initialize from scratch
        df_hist = pd.DataFrame({
            "id": sorted(todays_ids),
            "symbol": df_hl.set_index("id").loc[sorted(todays_ids), "symbol"].values,
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
        df_hist = df_hist[df_hist["daysOutOfTop50"] < 200].reset_index(drop=True)

        # Add new coins to history
        hist_ids = set(df_hist["id"])
        new_ids = sorted(todays_ids - hist_ids)
        if new_ids:
            symbol_map = df_hl.set_index("id")["symbol"].to_dict()
            df_new = pd.DataFrame({
                "id": new_ids,
                "symbol": [symbol_map.get(i, "") for i in new_ids],
                "daysOutOfTop50": 0
            })
            df_hist = pd.concat([df_hist, df_new], ignore_index=True)

    # Optional: Sort by longest out of Top 50
    df_hist = df_hist.sort_values(["daysOutOfTop50", "id"], ascending=[True, True]).reset_index(drop=True)

    # Step 3: Save updated history
    d.save_top50_history(df_hist, "tracked_coins")

    print(f"Top 50 tracker updated. Now tracking {len(df_hist)} coins.")


    # Get full table of last 100 days of price action for all those coins
    ids = df_hist["id"].tolist()
    metadata  = cmc_api.get_metadata_full(ids)
    
    # Récupération du market cap + date (ts)
    df_marketcap = cmc_api.get_marketcap_snapshot(ids)

    # Fusionner les deux DataFrames sur 'id'
    metadata = pd.merge(metadata, df_marketcap, on="id", how="left")

    # Réorganiser les colonnes : ts et market_cap avant 'category'
    if "category" in metadata.columns:
        cat_idx = metadata.columns.get_loc("category")
        # Reorder columns
        cols = metadata.columns.tolist()
        for col in ["ts", "market_cap"]:
            if col in cols:
                cols.insert(cat_idx, cols.pop(cols.index(col)))
                cat_idx += 1  # décaler pour insérer l’autre juste après
        metadata = metadata[cols]


    # -------------------------------------------------------------------------------------------------------------
    #   Fetch last daily OHLCV from Hyperliquid and add it to the database
    # -------------------------------------------------------------------------------------------------------------
    
  
    
    # Apply fetchDailyHyperliquid to each perp
    ohlcv_data = df_id_perp["perp"].apply(lambda p: hl_api.fetchDailyHyperliquid(p, nDays=1, offset=0).iloc[0])

    # Combine the OHLCV columns into the DataFrame
    df_id_perp = pd.concat([df_id_perp, ohlcv_data.reset_index(drop=True)], axis=1)
        
    
    
    # Merge OHLCV and perp data into metadata on 'id'
    metadata_with_ohlcv = metadata.merge(
        df_id_perp.drop(columns=["perp"]),  # drop 'symbol' if already in metadata
        on="id",
        how="left"
    )
    
    # Desired core column order
    core_order = ["id", "symbol", "ts", "open", "high", "low", "close", "volume", "market_cap", "category"]

    # Ensure only columns that exist are used
    existing_core = [col for col in core_order if col in metadata_with_ohlcv.columns]

    # Get the rest of the columns (those not in core_order)
    remaining_cols = [col for col in metadata_with_ohlcv.columns if col not in existing_core]

    # Final column order
    final_cols = existing_core + remaining_cols

    # Reorder the DataFrame
    metadata_with_ohlcv = metadata_with_ohlcv[final_cols]
    

    # 1. Define file path
    file_path = Path("dailyTop50ohclv_hl.parquet")

    # 2. Get yesterday's date string
    yesterday = (datetime.now().date() - timedelta(days=1))
    yesterday_str = str(yesterday)

    # 3. Load existing data if it exists
    if file_path.exists():
        df_existing = pd.read_parquet(file_path)
    else:
        df_existing = pd.DataFrame()

    # 4. Check if yesterday's data already exists
    if "ts" in df_existing.columns and yesterday in pd.to_datetime(df_existing["ts"]).dt.date.values:
        print(f"Data for {yesterday_str} already exists — no update needed.")
    else:
        # 5. Append new rows
        df_combined = pd.concat([df_existing, metadata_with_ohlcv], ignore_index=True)

        # 6. Save updated data
        df_combined.to_parquet(file_path, index=False)
        print(f"Appended data for {yesterday_str} and saved to {file_path.name}.")

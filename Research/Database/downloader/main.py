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
    #  # Get today's coins from hyperliquid that are on the top 50 by marketcap from cmc excluding stables
    #  - Define the dataframe # df_hl = [id  symbol]
    # -------------------------------------------------------------------------------------------------------------
    
    # Fetch data once
    df_top = cmc_api.get_top_n(100)
    df_meta = cmc_api.get_metadata(df_top["id"].tolist())

    # Merge and filter top 50 non-stablecoins, keep only id and symbol
    df_top50 = (
        df_meta.merge(df_top, on="id")
        .loc[~df_meta["stablecoin"]]
        .sort_values("cmc_rank")
        .head(50)[["id", "symbol"]]
        .reset_index(drop=True)
    )

    # Fetch active Hyperliquid perps
    hl_perps = {r["perp"] for r in hl_api.hyperliquid_active_perps()}

    # Filter to tokens with active HL perps, keep only id and symbol
    df_hl = (
        df_top50[df_top50["symbol"].map(lambda sym: d.matches(sym, hl_perps))]
        .reset_index(drop=True)
    )
        
    
    # -------------------------------------------------------------------------------------------------------------
    #  # Maintain a rolling list of coins to track:
    #  - Reset daysOutOfTop50 to 0 if coin is in today’s Top 50
    #  - Increment if missing, and drop if out for 100+ days
    #  - Add new coins from today's Top 50
    #  This determines which coins to download historical data for. ( df_tracked = [id  symbol  daysOutOfTop50] )
    # -------------------------------------------------------------------------------------------------------------

    # Load list of tracked coins 
    df_tracked = d.load_top50_history("tracked_coins")

    # Normalize dtypes if not empty
    if not df_tracked.empty:
        df_tracked = df_tracked.astype({"id": int, "daysOutOfTop50": int})

   # Build today's ID set from df_hl
    todays_ids = set(df_hl["id"].astype(int))
    symbol_map = df_hl.set_index("id")["symbol"].to_dict()

    if df_tracked.empty:
        # Initialize tracking from scratch
        df_tracked = pd.DataFrame({
            "id": sorted(todays_ids),
            "symbol": [symbol_map.get(i, "") for i in sorted(todays_ids)],
            "daysOutOfTop50": 0
        })
    else:
        # Mark coins in/out of today's top 50
        in_today = df_tracked["id"].isin(todays_ids)
        
        df_tracked.loc[in_today, "daysOutOfTop50"] = 0
        df_tracked.loc[~in_today, "daysOutOfTop50"] += 1

        # Remove coins that have been out of the top 50 for 100+ days
        df_tracked = df_tracked[df_tracked["daysOutOfTop50"] < 101].reset_index(drop=True)

        # Add new coins not yet tracked
        existing_ids = set(df_tracked["id"])
        new_ids = sorted(todays_ids - existing_ids)
        
        if new_ids:
            df_new = pd.DataFrame({
                "id": new_ids,
                "symbol": [symbol_map.get(i, "") for i in new_ids],
                "daysOutOfTop50": 0
            })
            df_tracked = pd.concat([df_tracked, df_new], ignore_index=True)

    # Save updated tracking data
    d.save_top50_history(df_tracked, "tracked_coins")
    print(f"Top 50 tracker updated. Now tracking {len(df_tracked)} coins.")








    df_tracked = df_tracked.drop(columns=["daysOutOfTop50"]) #  df_tracked = [id  symbol]
    df_tracked["perp"] = df_tracked["symbol"].map(lambda sym: d.find_matching_perp(sym, hl_perps))
    df_tracked = df_tracked[df_tracked["perp"].notna()].reset_index(drop=True) # df_tracked = [id, symbol, perp]

    today = datetime.now().date() 

    # Load existing data if it exists
    file_path = Path("dailyTop50ohclv_hl.parquet")

    if file_path.exists():
        df_existing = pd.read_parquet(file_path)
    else:
        df_existing = pd.DataFrame()
        
    # Initialize daysToFetch column
    df_tracked["daysToFetch"] = 100  # default

    # Compute daysToFetch per id
    for i, row in df_tracked.iterrows():
        id_val = row["id"]
        df_id = df_existing[df_existing["id"] == id_val]

        if not df_id.empty:
            last_day = pd.to_datetime(df_id["ts"]).max().date()
            days_diff = (today - last_day).days
            df_tracked.at[i, "daysToFetch"] = days_diff
        
    # checkear al final que esta bien esta resta para calcular el numero de dias
    # descargar todos ohclv y poner mcap a 0 menos el ultimo timestamp, recoger metadata y mergear (orden columnas mas eficiente)
    # concatenar nuevas filas y asegurarse de que este todo en orden        
        
        
        
        
        
        
        
        
        
        

    # 4. Check if yesterday's data already exists
    if "ts" in df_existing.columns and yesterday in pd.to_datetime(df_existing["ts"]).dt.date.values:
        print(f"Data for {yesterday_str} already exists — no update needed.")
    else:
        # 5. Append new rows
        df_combined = pd.concat([df_existing, metadata_with_ohlcv], ignore_index=True)

        # 6. Save updated data
        df_combined.to_parquet(file_path, index=False)
        print(f"Appended data for {yesterday_str} and saved to {file_path.name}.")


















    tracking_ids_perp = df_tracked["id"].tolist()

    df_id_perp = df_hl[["id", "perp"]]







    # Get full table of last 100 days of price action for all those coins
    ids = df_tracked["id"].tolist()
    metadata  = cmc_api.get_metadata_full(ids)  # metadata = ['id', 'symbol', 'category', 'tags' (several columns of boolean values)]
    
    # Récupération du market cap + date (ts)
    df_marketcap = cmc_api.get_marketcap_snapshot(ids) # df_marketcap = [id, market_cap]  (df_top50 already has all coinswith cols id and market_cap, but it has more ids)

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
    
  
    
    df_id_perp = df_hl[["id", "perp"]]


    
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

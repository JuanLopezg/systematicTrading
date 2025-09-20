import requests
import json
import pandas as pd
import time
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from sys import exit
from tqdm import tqdm

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

    df_tracked = df_tracked.drop(columns=["daysOutOfTop50"])  # df_tracked = [id, symbol]






    # Match perps
    df_tracked["perp"] = df_tracked["symbol"].map(lambda sym: d.find_matching_perp(sym, hl_perps))
    df_tracked = df_tracked[df_tracked["perp"].notna()].reset_index(drop=True)  # [id, symbol, perp]

    # Use yesterday (last full day)
    yesterday = datetime.now() - timedelta(days=1)

    # Load existing data if it exists
    file_path = Path("dailyTop50ohclv_hl.parquet")
    df_existing = pd.read_parquet(file_path) if file_path.exists() else pd.DataFrame()

    # If we have existing data, prepare latest_dates
    if not df_existing.empty and "ts" in df_existing.columns:
        df_existing["ts"] = pd.to_datetime(df_existing["ts"])

        latest_dates = (
            df_existing.groupby("id")["ts"]
            .max()
            .rename("last_ts")
        )

        df_tracked = df_tracked.merge(latest_dates, on="id", how="left")
    else:
        df_tracked["last_ts"] = datetime.now() - timedelta(days=100)



    # Compute daysToFetch
    df_tracked["daysToFetch"] = (yesterday - df_tracked["last_ts"]).dt.days


    # Drop the helper column
    df_tracked = df_tracked.drop(columns=["last_ts"])

    # Check if any coins need fetching
    if not df_tracked["daysToFetch"].gt(0).any():
        print("Nothing to fetch")
        exit()

        
    # Collect OHCLV data for all ids
    ohclv_list = []

    for i, row in tqdm(df_tracked.iterrows(), total=len(df_tracked), desc="Fetching OHCLV"):
        try:
            ohclv = hl_api.fetchDailyHyperliquid(row["perp"], row["daysToFetch"], 1)
            
            if ohclv.empty:
                print(f"No data for {row['symbol']} ({row['perp']})")
                continue

            ohclv["market_cap"] = 0  # Placeholder, if you plan to enrich this later
            ohclv["id"] = row["id"]
            ohclv_list.append(ohclv)

        except Exception as e:
            print(f"Failed to fetch data for {row['symbol']} ({row['perp']}): {e}")
            continue

    # Combine all ohclv data into a single DataFrame
    if ohclv_list:
        df_ohclv = pd.concat(ohclv_list, ignore_index=True)

        # Merge with df_tracked to attach symbol to each OHCLV row
        df_final = df_ohclv.merge(  # df_final = [id, symbol, ts, open, high, low, close, volume, market_cap]
            df_tracked[["id", "symbol"]],
            on="id",
            how="left"
        )
        print(f"Fetched OHCLV data for {df_final['id'].nunique()} coins ({len(df_final)} rows).")
    else:
        print("No OHCLV data fetched. Exiting.")
        exit()
        
          
    ids = df_tracked["id"].tolist()
    metadata  = cmc_api.get_metadata_full(ids)  # metadata = ['id', 'category', 'tags' (several columns of boolean values)]
    if metadata is None or metadata.empty:
        print("ERROR: Metadata fetch failed or returned nothing. Exiting.")
        exit()
    df_final = df_final.merge(metadata, on="id", how="left") # df_final = [id, symbol, ts, open, high, low, close, volume, market_cap, category, tags (several columns of boolean values)]
    
    
    df_marketcaps = cmc_api.get_marketcap_snapshot(ids) # [id, market_cap]
    if df_marketcaps.empty:
        print("ERROR: Market cap snapshot is empty. Exiting.")
        exit()
        
    yesterday_date = yesterday.date()
    df_final["ts"] = pd.to_datetime(df_final["ts"]).dt.date
        
    df_marketcaps["market_cap"] = df_marketcaps["market_cap"].round().astype("int64")
    for id in ids:
        mask = (df_final["id"] == id) & (df_final["ts"] == yesterday_date)
        df_final.loc[mask, "market_cap"] = df_marketcaps.loc[df_marketcaps["id"] == id, "market_cap"].values[0]
        
    if  df_final.isnull().values.any():
        print("ERROR: None element found, exiting")
        exit()
    
    # Re-order columns
    core_order = ["id", "symbol", "ts", "open", "high", "low", "close", "volume", "market_cap", "category"]
    # Use df_final instead of metadata_with_ohlcv
    existing_core = [col for col in core_order if col in df_final.columns]
    remaining_cols = [col for col in df_final.columns if col not in existing_core]
    final_cols = existing_core + remaining_cols

    df_final = df_final[final_cols]
    
    df_combined = pd.concat([df_existing, df_final], ignore_index=True)
    # Force ts to proper datetime
    df_combined["ts"] = pd.to_datetime(df_combined["ts"], errors="coerce")


    # 6. Save updated data
    df_combined.to_parquet(file_path, index=False)
    print("New data downloading successful")
    
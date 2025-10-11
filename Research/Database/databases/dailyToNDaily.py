import os
import gc
import pandas as pd
from glob import glob as g

file = "systematictrading/Research/Database/databases/db/dailyOHLCV.par"


# timeframe in days
timeframes = [2,3,5,7,14,21,30]

for tf in timeframes :
    # Output folder
    file_name = f'{tf}DOHCLV.par'
    folder_path = "systematictrading/Research/Database/databases/db/"

    df = pd.read_parquet(file)

    # Filter out ids with too few rows
    ids_to_drop = df.groupby('id').filter(lambda x: len(x) < tf * 3)['id'].unique()
    df = df[~df['id'].isin(ids_to_drop)]

    # Container for all aggregated data from this file
    all_new_rows = []

    # Process each unique id separately
    for id_value, id_df in df.groupby('id', sort=False):
        id_df = id_df.copy()
        id_df['ts'] = pd.to_datetime(id_df['ts'])








        # Find valid start/end aligned to the timeframe
        valid_times = id_df[id_df['ts'].dt.hour % tf == 0]
        if valid_times.empty:
            continue
        if len(valid_times) < 2:
            continue  # not enough full groups

        valid_start_time = valid_times.iloc[0]['ts']
        valid_end_time = valid_times.iloc[-1]['ts']

        # Keep only aligned rows
        id_df = id_df[(id_df['ts'] >= valid_start_time) & (id_df['ts'] < valid_end_time)]
        id_df = id_df.sort_values('ts').reset_index(drop=True)

        # Aggregate every 'tf' rows
        for start_idx in range(0, len(id_df), tf):
            slice_df = id_df.iloc[start_idx:start_idx + tf]
            if len(slice_df) < tf:
                continue  # skip incomplete blocks

            new_rows = {
                'id': slice_df['id'].iloc[0],
                'symbol': slice_df['symbol'].iloc[0],
                'ts': slice_df['ts'].iloc[0],
                'open': slice_df['open'].iloc[0],
                'high': slice_df['high'].max(),
                'low': slice_df['low'].min(),
                'close': slice_df['close'].iloc[-1],
                'market_cap': slice_df['market_cap'].iloc[-1],
            }
            all_new_rows.append(new_rows)

    # Build new aggregated dataset for this file
    agg_df = pd.DataFrame(all_new_rows)
    
    # --- Identify IDs with more than one row where market_cap == 0 ---
    ids_to_drop = (
        agg_df[agg_df["market_cap"] == 0]
        .groupby("id")
        .size()
        .loc[lambda x: x > 1]
        .index
    )

    if len(ids_to_drop) > 0:

        # Drop all rows for those IDs
        agg_df = agg_df[~agg_df["id"].isin(ids_to_drop)]

    # Save new aggregated dataset (not the original df)
    file_name = os.path.basename(file)
    agg_df.to_parquet(f'{folder_path}/{file_name}')

    # Clean up memory
    del df, agg_df
    gc.collect()

print("✅ Done! All aggregated datasets saved to:", folder_path)
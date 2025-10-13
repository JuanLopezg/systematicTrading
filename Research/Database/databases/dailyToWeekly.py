import os
import gc
import pandas as pd
from glob import glob as g

file = "systematictrading/Research/Database/databases/db/dailyOHLCV.par"


# timeframe in days
timeframes = [7,14,21]

for tf in timeframes :
    # Output folder
    z = tf/7
    file_name = f'{z}weekOHLCV.par'
    folder_path = "systematictrading/Research/Database/databases/db/"

    df = pd.read_parquet(file)
    
    if df.isna().any().any():
        raise ValueError("❌ Dataset contains NaN values")

    # Filter out ids with too few rows
    ids_to_drop = df.groupby('id').filter(lambda x: len(x) < tf * 3)['id'].unique()
    df = df[~df['id'].isin(ids_to_drop)]

    # Container for all aggregated data from this file
    all_new_rows = []
    

    # Process each unique id separately
    for id_value, id_df in df.groupby('id', sort=False):
        id_df = id_df.copy()
        id_df['ts'] = pd.to_datetime(id_df['ts'])

        # Extract only the date (drop time)
        id_df['date'] = id_df['ts'].dt.floor('D')  

        # Keep only rows where the day aligns with n-day intervals
        valid_times = id_df[id_df['date'].dt.weekday == 0]
        
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
                'volume': slice_df['volume'].sum(),
                'market_cap': slice_df['market_cap'].iloc[-1],
                'rank': slice_df['rank'].iloc[-1]
            }
            all_new_rows.append(new_rows)

    # Build new aggregated dataset for this file
    agg_df = pd.DataFrame(all_new_rows)

    # Save new aggregated dataset (not the original df)
    file_name = os.path.basename(file_name)
    agg_df.to_parquet(f'{folder_path}/{file_name}')

    # Clean up memory
    del df, agg_df
    gc.collect()

print("✅ Done! All aggregated datasets saved to:", folder_path)
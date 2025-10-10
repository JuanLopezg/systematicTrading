import pandas as pd
from glob import glob as g
import os
import gc 

files = g("crypto hourly 2025/OHLCV*.par")
print(f"{len(files)} files")

# timeframe in hours
tf = 4


# Iterate over each file
for file in files:
    df = pd.read_parquet(file)
    df.drop(columns=['volume'], inplace=True)
    
    # Filter ids based on the length of rows
    ids_to_drop = df.groupby('id').filter(lambda x: len(x) < tf * 2)['id'].unique()
    df = df[~df['id'].isin(ids_to_drop)]
    
    # Process each unique id
    for id_value in df['id'].unique():
        id_df = df[df['id'] == id_value]
        id_df.loc[:, 'ts'] = pd.to_datetime(id_df['ts'])  # Convert to datetime
        
        valid_start_time = id_df[id_df['ts'].dt.hour % tf == 0].iloc[0]['ts']
        valid_end_time = id_df[id_df['ts'].dt.hour % tf == 0].iloc[-1]['ts']
        
        # Filter the rows between valid start and end times
        id_df = id_df[(id_df['ts'] >= valid_start_time) & (id_df['ts'] <= valid_end_time)]
        
        # Aggregating every 'tf' rows into one row
        new_rows = []
        for start_idx in range(0, len(id_df), tf):
            # Slice 'tf' rows
            slice_df = id_df.iloc[start_idx:start_idx + tf]
            
            # Create a new row with aggregated values
            new_row = {
                'id': slice_df['id'].iloc[0],  # Keep the same 'id'
                'symbol': slice_df['symbol'].iloc[0],  # Keep the same 'symbol'
                'ts': slice_df['ts'].iloc[0],  # Take the first timestamp in this block
                'open': slice_df['open'].iloc[0],  # Take the first 'open'
                'high': slice_df['high'].max(),  # Take the max 'high'
                'low': slice_df['low'].min(),  # Take the min 'low'
                'close': slice_df['close'].iloc[-1],  # Take the last 'close'
                'market_cap': slice_df['market_cap'].iloc[-1]  # Take the last 'market_cap'
            }
            
            # Append the new row to the list of new rows
            new_rows.append(new_row)
        
        # Rebuild the DataFrame with the new aggregated rows
        aggregated_df = pd.DataFrame(new_rows)
        
        # Replace the original id_df with the aggregated DataFrame
        df.loc[df['id'] == id_value, :] = aggregated_df
    
    # Trigger garbage collection after processing each file
    gc.collect()  # This will help free memory
    
    # Save the modified DataFrame to the appropriate folder
    folder_name = f'crypto{tf}H'
    folder_path = os.path.join('./', folder_name)
    os.makedirs(folder_path, exist_ok=True)

    file_name = os.path.basename(file)
    df.to_parquet(f'{folder_path}/{file_name}')
    
    # Clean up the df to ensure that it is removed from memory
    del df  # Delete the DataFrame from memory
    gc.collect()  # Force garbage collection again after deleting df
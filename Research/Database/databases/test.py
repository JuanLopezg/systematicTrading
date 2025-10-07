import pandas as pd

""" 
for i in range(1, 39):  
    path = f"crypto/OHLCV_{i}.par"
    try:
        df = pd.read_parquet(path)
        nan_counts = df.isna().sum()
        nan_counts = nan_counts[nan_counts > 0]
        if not nan_counts.empty:
            print(f"File OHLCV_{i}.par has missing values:")
            print(nan_counts, "\n")
    except Exception as e:
        print(f"Error reading {path}: {e}\n")
 """
 
df = pd.read_parquet("crypto/metadata.par")

if df.isna().any().any() :
    raise ValueError("Nan Values")
import pandas as pd
import matplotlib.pyplot as plt
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
 
""" df = pd.read_parquet("crypto/metadata.par")

if df.isna().any().any() :
    raise ValueError("Nan Values") """
    
    
# Load the previously saved data
ds = pd.read_parquet("systematictrading/Research/Database/databases/db/hourlyOHCLV.par")

the_id = 1  # BTC

fig, ax1 = plt.subplots(1, 1, figsize=(12, 6))
te = ds.reset_index()
te = te[te['id'] == the_id].set_index(['ts'])
te['close'].plot()
fig.suptitle(f"Price History for {te['symbol'].iloc[0]}")
ax1.grid()
plt.show()
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
""" ds = pd.read_parquet("systematictrading/Research/Database/databases/db/hourlyOHCLV.par")

the_id = 1  # BTC
theid2 = 36507

fig, ax1 = plt.subplots(1, 1, figsize=(12, 6))
te = ds.reset_index()
te = te[te['id'] == theid2].set_index(['ts'])
te['close'].plot()
fig.suptitle(f"Price History for {te['symbol'].iloc[0]}")
ax1.grid()
plt.show() """

""" ds = pd.read_parquet("crypto4H/OHLCV_1.par")
print(ds.tail(20))
del ds """
ds = pd.read_parquet("crypto hourly 2025/OHLCV_1.par")
print(ds.tail(10))
ds.drop(columns=['volume'], inplace=True)
if ds.isna().any().any() :
    raise ValueError("Nan Values") 
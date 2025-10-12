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
""" 
ds = pd.read_parquet("crypto4H/OHLCV_1.par")
print(ds.tail(20))

# --- Check for coins with market_cap == 0 ---
zero_mc = ds[ds["market_cap"] == 0]

if not zero_mc.empty:
    zero_symbols = zero_mc["symbol"].unique()
    print(f"\n⚠️ {len(zero_symbols)} coins have at least one row with market_cap = 0:")
    print(", ".join(zero_symbols))
else:
    print("\n✅ No coins have market_cap = 0")

# --- Check for NaN values ---
if ds.isna().any().any():
    raise ValueError("❌ Dataset contains NaN values")

print("\n✅ Dataset passed validation checks!")

# --- Identify IDs with more than one row where market_cap == 0 ---
ids_to_drop = (
    ds[ds["market_cap"] == 0]
    .groupby("id")
    .size()
    .loc[lambda x: x > 1]
    .index
)

print(f"\n⚠️ {len(ids_to_drop)} IDs have more than one market_cap = 0")

if len(ids_to_drop) > 0:
    # Show the symbols (for context)
    symbols_to_drop = ds.loc[ds["id"].isin(ids_to_drop), "symbol"].unique()
    print("Symbols to drop:", ", ".join(symbols_to_drop))

    # Drop all rows for those IDs
    ds = ds[~ds["id"].isin(ids_to_drop)]
    print(f"✅ Dropped {len(ids_to_drop)} coins with >1 zero market_cap rows") """
    
    
ds = pd.read_parquet("systematictrading/Research/Database/databases/db/30dOHLCV.par")
print(ds.head(30))
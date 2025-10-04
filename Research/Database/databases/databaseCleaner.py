import pandas as pd

# Read the Parquet file
df = pd.read_parquet("../crypto2/OHLCV_with_metadata.par")

df = df.reset_index()

df = df[~df["stablecoin"]]
df = df[~df["wrapped"]]

df.drop(columns=[ 'category', 'wrapped', 'stablecoin',
       'collectibles-nfts', 'memes', 'iot', 'dao', 'governance', 'mineable',
       'pow', 'pos', 'sha-256', 'store-of-value', 'medium-of-exchange',
       'scrypt', 'layer-1', 'layer-2'], inplace=True)

if df.isna().any().any() :
    raise ValueError("Nan Values")


# 2️⃣ Rank coins by market cap for each day
df["rank"] = df.groupby("ts")["market_cap"].rank(method="first", ascending=False)

# 3️⃣ Get IDs that ever appear in the top 50
top50_ids = df.loc[df["rank"] <= 50, "id"].unique()

# 4️⃣ Keep only those coins (drop all others completely)
df = df[df["id"].isin(top50_ids)]

df.to_parquet("./Research/Database/databases/db/dailyOHLCVclean.par")
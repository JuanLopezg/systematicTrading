import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import json
import numpy as np

# --- Configure your CoinMarketCap API creds ---
CMC_API_KEY = '179c21ec-d0b1-472c-9443-4a8b371e2629'
BASE_URL = "https://pro-api.coinmarketcap.com/"

HEADERS = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": CMC_API_KEY,
}

def get_top_n(n=75):
    """Get the top N cryptocurrencies by market cap without stablecoins:
    [ID | SYMBOL | CMC_RANK]"""
    
    url = BASE_URL + "v1/cryptocurrency/listings/latest"
    params = {
        "start": "1",
        "limit": str(n),
        "convert": "USD",
        "sort": "market_cap",
        "sort_dir": "desc",
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()["data"]

    return pd.DataFrame([{
        "id": item["id"],
        "cmc_rank": item["cmc_rank"],
        "market_cap": item["quote"]["USD"]["market_cap"],
    } for item in data])



def get_metadata(ids):
    """Fetch metadata for given IDs (name, symbol, category, tags)."""
    url = BASE_URL + "v2/cryptocurrency/info"
    params = {"id": ",".join(map(str, ids)), "skip_invalid": "true"}
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()["data"]

    rows = []
    for cid, entry in data.items():
        rows.append({
            "id": entry.get("id"),
            "name": entry.get("name"),
            "symbol": entry.get("symbol"),
            "category": entry.get("category"),
            "tags": entry.get("tags") or [],
        })
    df = pd.DataFrame(rows)

    # Stablecoin flag
    df["stablecoin"] = df["tags"].apply(lambda tags: "stablecoin" in tags)

    return df.drop(columns=["tags"])



relevant_tags = [
    'wrapped', 'stablecoin', 'collectibles-nfts', 'memes', 'iot', 'dao',
    'governance', 'mineable', 'pow', 'pos', 'sha-256', 'store-of-value',
    'medium-of-exchange', 'scrypt', 'layer-1', 'layer-2'
]

cols = ['id', 'symbol', 'category', 'tags']

def get_metadata_full(the_ids):
    if not isinstance(the_ids, (list, np.ndarray, pd.Series)):
        the_ids = list(the_ids)

    if not the_ids:
        print("Info: No IDs provided for metadata fetch.")
        return None

    time.sleep(2)  # Depends on subscription tier
    url = BASE_URL + 'v2/cryptocurrency/info'
    parameters = {'id': ",".join(map(str, the_ids)), 'skip_invalid': 'true'}

    try:
        response = requests.get(url, headers=HEADERS, params=parameters)
        response.raise_for_status()
        data = json.loads(response.text).get('data', {})

        if not data:
            return None

        valid_ids = [str(x) for x in the_ids if str(x) in data]
        if not valid_ids:
            return None

        processed = [{col: data[i].get(col) for col in cols} for i in valid_ids]
        df = pd.DataFrame(processed)

        df['tags'] = df['tags'].apply(lambda x: x if isinstance(x, list) else [])

        for tag in relevant_tags:
            df[tag] = df['tags'].apply(lambda x: tag in x)

        return df.drop('tags', axis=1)

    except Exception as e:
        print(f"Warning: Metadata fetch failed for IDs near {the_ids[0]}: {e}")
        return None



def get_marketcap_snapshot(ids):
    """
    Fetch market cap for given CoinMarketCap IDs.
    Returns a DataFrame with columns: id, market_cap.
    """
    if not ids:
        return pd.DataFrame(columns=["id", "market_cap"])

    url = BASE_URL + "v2/cryptocurrency/quotes/latest"
    params = {"id": ",".join(map(str, ids)), "skip_invalid": "true"}

    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()["data"]

    rows = []
    for cid, entry in data.items():
        quote = entry.get("quote", {}).get("USD", {})
        rows.append({
            "id": entry.get("id"),
            "market_cap": quote.get("market_cap")
        })

    return pd.DataFrame(rows)

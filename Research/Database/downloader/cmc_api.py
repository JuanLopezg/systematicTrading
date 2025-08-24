import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone



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


import requests
import pandas as pd


def get_binance_ohlc(symbol="BTCUSDT", interval="1h", limit=100):
    """
    Fetch OHLC data from Binance public API.
    - symbol: trading pair (e.g., BTCUSDT)
    - interval: 1m, 5m, 15m, 1h, 4h, 1d, etc.
    - limit: number of candles (max 1000)
    """
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame(data, columns=[
        "OpenTime", "Open", "High", "Low", "Close", "Volume",
        "CloseTime", "QuoteAssetVolume", "NumberOfTrades",
        "TakerBuyBaseVol", "TakerBuyQuoteVol", "Ignore"
    ])

    # Convert timestamps and numeric columns
    df["Date"] = pd.to_datetime(df["OpenTime"], unit="ms")
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].astype({
        "Open": float, "High": float, "Low": float, "Close": float, "Volume": float
    })

    return df

def get_all_binance_usd_pairs():
    """
    Returns a combined list of all Binance symbols (spot and futures) that trade
    against USD, USDT, or USDC. Includes only active pairs.
    """
    pairs = set()

    # Get PERPETUAL contracts from Binance Futures
    futures_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    r_futures = requests.get(futures_url)
    r_futures.raise_for_status()
    futures_data = r_futures.json()

    for s in futures_data["symbols"]:
        if s["contractType"] == "PERPETUAL" and s["quoteAsset"] == "USDT":
            pairs.add(s["symbol"])

    # Get SPOT trading pairs from Binance Spot
    spot_url = "https://api.binance.com/api/v3/exchangeInfo"
    r_spot = requests.get(spot_url)
    r_spot.raise_for_status()
    spot_data = r_spot.json()

    for s in spot_data["symbols"]:
        if s["status"] == "TRADING" and s["quoteAsset"] in {"USDT", "USDC", "USD"}:
            pairs.add(s["symbol"])

    return sorted(pairs)
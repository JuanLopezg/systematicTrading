import requests

def get_bybit_usd_pairs():
    """
    Fetch all Bybit trading pairs (spot + perps) quoted in USDT, USDC, or USD.
    Returns a sorted list of symbols like 'BTCUSDT', 'ETHUSDC', etc.
    """
    quote_targets = {"USDT", "USDC", "USD"}
    symbols = set()

    # --- Spot pairs ---
    spot_url = "https://api.bybit.com/v5/market/instruments-info"
    r_spot = requests.get(spot_url, params={"category": "spot"})
    r_spot.raise_for_status()
    spot_data = r_spot.json()["result"]["list"]
    for item in spot_data:
        if item["quoteCoin"] in quote_targets:
            symbols.add(item["symbol"])

    # --- USDT/USDC perpetuals (linear contracts) ---
    linear_url = "https://api.bybit.com/v5/market/instruments-info"
    r_linear = requests.get(linear_url, params={"category": "linear"})
    r_linear.raise_for_status()
    linear_data = r_linear.json()["result"]["list"]
    for item in linear_data:
        if item["quoteCoin"] in quote_targets:
            symbols.add(item["symbol"])

    # --- Inverse contracts (e.g. BTCUSD) ---
    inverse_url = "https://api.bybit.com/v5/market/instruments-info"
    r_inverse = requests.get(inverse_url, params={"category": "inverse"})
    r_inverse.raise_for_status()
    inverse_data = r_inverse.json()["result"]["list"]
    for item in inverse_data:
        if item["quoteCoin"] in quote_targets:
            symbols.add(item["symbol"])

    return sorted(symbols)

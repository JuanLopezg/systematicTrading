import requests

def get_bitfinex_usd_pairs():
    """
    Fetch all Bitfinex trading pairs (spot + perps) quoted in USD, USDT, or USDC.
    Returns a sorted list of trading symbols (e.g., 'tBTCUSD', 'tETHUSDT', etc.).
    """
    url = "https://api.bitfinex.com/v1/symbols_details"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()

    quote_targets = {"usd", "usdt", "usdc"}
    symbols = set()

    for item in data:
        pair = item.get("pair", "")
        if len(pair) < 6:
            continue
        base = pair[:-3]
        quote = pair[-3:]
        if quote in quote_targets:
            symbols.add(f"t{base.upper()}{quote.upper()}")

    # Add derivative symbols (e.g. perpetual swaps) — optional
    # You can hit v2 funding/instrument endpoints if needed

    return sorted(symbols)

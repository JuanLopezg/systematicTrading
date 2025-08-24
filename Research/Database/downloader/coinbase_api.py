import requests

def get_coinbase_usd_pairs():
    """
    Fetch all Coinbase spot trading pairs quoted in USD, USDC, or USDT.
    Returns a sorted list of trading symbols like 'BTC-USD', 'ETH-USDC'.
    """
    url = "https://api.exchange.coinbase.com/products"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()

    quote_targets = {"USD", "USDC", "USDT"}
    symbols = set()

    for item in data:
        if item.get("quote_currency") in quote_targets and item.get("status") == "online":
            symbols.add(item["id"])  # e.g., 'BTC-USD', 'ETH-USDC'

    return sorted(symbols)
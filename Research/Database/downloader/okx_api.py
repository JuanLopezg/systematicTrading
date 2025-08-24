import requests

def get_okx_usd_pairs():
    """
    Fetch all OKX trading pairs (spot + perps) quoted in USDT, USDC, or USD.
    Returns a sorted list of unique trading symbols.
    """
    url = "https://www.okx.com/api/v5/public/instruments"

    # Fetch spot instruments
    spot_resp = requests.get(url, params={"instType": "SPOT"})
    spot_resp.raise_for_status()
    spot_data = spot_resp.json()["data"]

    # Fetch perpetual futures instruments
    perp_resp = requests.get(url, params={"instType": "SWAP"})
    perp_resp.raise_for_status()
    perp_data = perp_resp.json()["data"]

    # Filter by quote currencies
    target_quotes = {"USDT", "USDC", "USD"}

    symbols = set()

    for inst in spot_data + perp_data:
        if inst["quoteCcy"] in target_quotes:
            symbols.add(inst["instId"])  # e.g. BTC-USDT or ETH-USDC-SWAP

    return sorted(symbols)

from hyperliquid.info import Info
from typing import List, Dict, Any
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone


def fetchDailyHyperliquid(perp: str, nDays: int):
    """
    Fetch the last nDays OHLC data from a certain perp
    """
    info = Info(skip_ws=True)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days = nDays)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    
    candles = info.candles_snapshot(perp, "1d", start_ms, end_ms)
    
    df = pd.DataFrame([
        {
            "Date": datetime.fromtimestamp(c.get("t", 0) // 1000, tz=timezone.utc),
            "Open": float(c["o"]),
            "High": float(c["h"]),
            "Low": float(c["l"]),
            "Close": float(c["c"])
        }
        for c in candles
    ])
    
    return df

def fetchHourlyHyperliquid(perp: str, nHours: int):
    """
    Fetch the last nDays OHLC data from a certain perp
    """
    info = Info(skip_ws=True)
    end = (datetime.now(timezone.utc)).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours = nHours)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    
    candles = info.candles_snapshot(perp, "1h", start_ms, end_ms)
    
    df = pd.DataFrame([
        {
            "Date": datetime.fromtimestamp(c.get("t", 0) // 1000, tz=timezone.utc),
            "Open": float(c["o"]),
            "High": float(c["h"]),
            "Low": float(c["l"]),
            "Close": float(c["c"])
        }
        for c in candles
    ])
    
    return df


def get_perp_universe_hl(dex: str = "") -> List[Dict[str, Any]]:
    """Return the perp 'universe' (listed perps) for the given DEX ('' = main)."""
    info = Info(skip_ws=True)
    meta = info.meta(dex=dex)  # perp-only metadata
    return meta["universe"]

def get_active_mids_hl() -> Dict[str, str]:
    """Return {symbol: mid_price_str} for actively trading perps (may include '@' synthetics)."""
    info = Info(skip_ws=True)
    return info.all_mids()


def hyperliquid_active_perps() -> List[Dict[str, Any]]:
    """Return a list of dicts with active perps and their details."""
    # Build a lookup: perp -> size_decimals (from full perp universe)
    universe = get_perp_universe_hl()
    size_decimals_by_perp = {a.get("name"): a.get("szDecimals") for a in universe}

    # Active mids (filter out synthetic '@...' markets)
    mids_raw = get_active_mids_hl()
    rows = []
    for name, mid in mids_raw.items():
        if name.startswith("@"):
            continue  # skip synthetic/system markets
        sz_dec = size_decimals_by_perp.get(name)
        if sz_dec is None:
            continue
        try:
            mid_price = float(mid)
        except (TypeError, ValueError):
            continue

        rows.append({
            "perp": name,
            "size_decimals": sz_dec,
            "mid_price": mid_price,
        })

    return rows

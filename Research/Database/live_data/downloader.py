import datetime
import pandas as pd
from pathlib import Path
from datetime import datetime

def load_top50_history(basename="tracked_coins"):
    """
    Load existing history of 'daysOutOfTop50' from a CSV file with a date suffix.
    Example filename: tracked_coins_2025-09-06.csv
    
    - If no such file exists, return empty DataFrame with required columns.
    - If today's file already exists, raise an exception (to avoid overwriting).
    - Otherwise, load the latest available file.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")

    # Chercher tous les fichiers qui matchent le pattern
    files = sorted(Path(".").glob(f"{basename}_*.csv"))

    if not files:
        # Aucun fichier trouvé → dataframe vide
        return pd.DataFrame(columns=["id", "symbol", "daysOutOfTop50"])

    # On prend le plus récent (lexicographiquement ça marche car YYYY-MM-DD)
    latest_file = files[-1]

    # Extraire la date du nom de fichier
    try:
        file_date = latest_file.stem.split("_")[-1]  # ex: "2025-09-06"
    except IndexError:
        raise ValueError(f"Filename {latest_file} does not contain a valid date suffix.")

    # Vérifier si c’est aujourd’hui
    if file_date == today_str:
        raise RuntimeError(f"A history file for today already exists: {latest_file}")

    # Charger le fichier
    df = pd.read_csv(latest_file)
    
    latest_file.unlink()

    # Vérifier colonnes requises
    required = {"id", "symbol", "daysOutOfTop50"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{latest_file} is missing required columns: {missing}")

    return df


def save_top50_history(df, basename="tracked_coins"):
    """Save the DataFrame to a CSV file with today's date in the filename."""
    today_str = datetime.now().strftime("%Y-%m-%d")  # ex: 2025-09-06
    filename = f"{basename}_{today_str}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved history to {filename}")

    

def matches(symbol, symbols_list):
    """
    Compare a symbol (e.g., 'PEPE') against base names stripped from symbols_list.
    Accepts matches if:
    - base == symbol
    - base == 'K' + symbol
    - base == '1000' + symbol
    """
    suffixes = (
        "-USDT", "-USDC", "-USD",
        "_USDT", "_USDC", "_USD",
        "USDT", "USDC", "USD"
    )

    bases = set()
    for s in symbols_list:
        s_up = s.upper()
        base = s_up
        for suf in suffixes:
            if s_up.endswith(suf):
                base = s_up[: -len(suf)]
                break
        bases.add(base)

    symbol_up = symbol.upper()

    return any(
        base == symbol_up or
        base == "K" + symbol_up or
        base == "1000" + symbol_up
        for base in bases
    )
    

def find_matching_perp(symbol: str, symbols_list: list[str]) -> str | None:
    """
    Return the matching perp name from symbols_list for a given symbol.
    Match if base == symbol, or base == 'K' + symbol, or base == '1000' + symbol.
    Returns the full perp name (with suffix), or None if no match.
    """
    suffixes = (
        "-USDT", "-USDC", "-USD",
        "_USDT", "_USDC", "_USD",
        "USDT", "USDC", "USD"
    )

    symbol_up = symbol.upper()

    for s in symbols_list:
        s_up = s.upper()
        base = s_up
        for suf in suffixes:
            if s_up.endswith(suf):
                base = s_up[: -len(suf)]
                break

        if base == symbol_up or base == "K" + symbol_up or base == "1000" + symbol_up:
            return s  # Return original perp name (with suffix)

    return None


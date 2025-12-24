"""
MEXC Token Matcher - автоматическое сопоставление токенов MEXC с Jupiter.

Логика:
1. Получаем список всех фьючерсных пар с MEXC API
2. Ищем все возможные варианты по имени токена
3. Для каждого варианта делаем GET запрос на страницу MEXC
4. Парсим HTML, извлекаем contract из ссылки solscan.io
5. Сравниваем с jupiter_mint - если совпало, возвращаем mexc_symbol
"""

import re
import httpx
import logging
from typing import Optional, List, Dict, Tuple

logger = logging.getLogger(__name__)

MEXC_CONTRACT_DETAIL_URL = "https://contract.mexc.com/api/v1/contract/detail"
MEXC_FUTURES_PAGE_URL = "https://www.mexc.com/ru-RU/futures/{symbol}?type=linear_swap"

# Browser-like headers
BROWSER_HEADERS = {
    "Host": "www.mexc.com",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.8",
}

# Cache for MEXC symbols
_mexc_symbols_cache: List[Dict] = []
_cache_loaded: bool = False

# Специальные символы, которые нужно удалять при поиске
SPECIAL_CHARS = re.compile(r'[$#@!%^&*()\-+=/\\|<>?~`]')


def clean_token_name(name: str) -> str:
    """Удаляет специальные символы из имени токена для поиска на MEXC."""
    return SPECIAL_CHARS.sub('', name).strip()


def load_mexc_symbols(force_reload: bool = False) -> List[Dict]:
    """Load all MEXC futures symbols from API."""
    global _mexc_symbols_cache, _cache_loaded
    
    if _cache_loaded and not force_reload:
        return _mexc_symbols_cache
    
    try:
        with httpx.Client(timeout=30) as client:
            response = client.get(MEXC_CONTRACT_DETAIL_URL)
            response.raise_for_status()
            data = response.json()
            
            if data.get("success") and data.get("data"):
                _mexc_symbols_cache = data["data"]
                _cache_loaded = True
                logger.info(f"Loaded {len(_mexc_symbols_cache)} MEXC symbols")
                return _mexc_symbols_cache
    except Exception as e:
        logger.error(f"Failed to load MEXC symbols: {e}")
    
    return []


def find_potential_mexc_symbols(base_token: str, quote: str = "USDT") -> List[str]:
    """
    Find all potential MEXC symbols for a given base token.
    Returns list of symbols sorted by relevance (exact > prefix > suffix > contains).
    Автоматически удаляет специальные символы ($, # и т.д.) из имени токена.
    """
    symbols = load_mexc_symbols()
    if not symbols:
        return []
    
    # Очищаем имя токена от специальных символов
    base_upper = clean_token_name(base_token).upper()
    logger.info(f"Searching MEXC symbols for '{base_token}' -> cleaned: '{base_upper}'")
    
    if not base_upper:
        return []
    quote_upper = quote.upper()
    
    exact = []
    prefix = []
    suffix = []
    contains = []
    
    for sym_info in symbols:
        symbol = sym_info.get("symbol", "")
        base_coin = sym_info.get("baseCoin", "")
        quote_coin = sym_info.get("quoteCoin", "")
        
        if quote_coin != quote_upper:
            continue
        
        if base_coin == base_upper:
            exact.append(symbol)
        elif base_coin.startswith(base_upper):
            prefix.append(symbol)
        elif base_coin.endswith(base_upper):
            suffix.append(symbol)
        elif base_upper in base_coin:
            contains.append(symbol)
    
    # Return sorted by relevance
    return exact + prefix + suffix + contains


def extract_solana_contract(mexc_symbol: str, proxy_url: Optional[str] = None) -> Optional[str]:
    """
    Extract Solana contract address from MEXC futures page.
    Makes GET request and parses HTML for solscan.io link.
    """
    url = MEXC_FUTURES_PAGE_URL.format(symbol=mexc_symbol)
    
    try:
        client_kwargs = {"timeout": 30, "follow_redirects": True}
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        
        with httpx.Client(**client_kwargs) as client:
            response = client.get(url, headers=BROWSER_HEADERS)
            response.raise_for_status()
            html = response.text
            
            # Find solscan.io/token/ADDRESS pattern
            match = re.search(r'solscan\.io/token/([A-Za-z0-9]+)', html)
            if match:
                contract = match.group(1)
                logger.info(f"Found contract for {mexc_symbol}: {contract}")
                return contract
            
            logger.warning(f"No Solana contract found for {mexc_symbol}")
            return None
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP {e.response.status_code} for {mexc_symbol}")
        return None
    except Exception as e:
        logger.error(f"Error fetching {mexc_symbol}: {e}")
        return None


def find_matching_mexc_symbol(
    base_token: str, 
    jupiter_mint: str,
    proxy_url: Optional[str] = None
) -> Optional[str]:
    """
    Find MEXC symbol that matches the given Jupiter mint.
    
    Args:
        base_token: Token symbol (e.g., "ARC")
        jupiter_mint: Jupiter mint address to match
        proxy_url: Optional proxy URL for requests
    
    Returns:
        MEXC base symbol (e.g., "ARCSOL") if found, None otherwise
    """
    # Get all potential MEXC symbols
    potential_symbols = find_potential_mexc_symbols(base_token)
    
    if not potential_symbols:
        logger.warning(f"No MEXC symbols found for {base_token}")
        return None
    
    logger.info(f"Checking {len(potential_symbols)} potential symbols for {base_token}: {potential_symbols}")
    
    # Try each symbol
    for mexc_symbol in potential_symbols:
        contract = extract_solana_contract(mexc_symbol, proxy_url)
        
        if contract and contract == jupiter_mint:
            # Found match! Extract base coin from symbol
            base_coin = mexc_symbol.replace("_USDT", "")
            logger.info(f"✓ Match found: {base_token} -> {base_coin} (contract verified)")
            return base_coin
        elif contract:
            logger.info(f"✗ Contract mismatch for {mexc_symbol}: {contract} != {jupiter_mint}")
    
    logger.warning(f"No matching MEXC symbol found for {base_token} with mint {jupiter_mint}")
    return None


def get_all_mexc_usdt_symbols() -> List[str]:
    """Get list of all MEXC USDT futures base symbols."""
    symbols = load_mexc_symbols()
    return [
        s["baseCoin"] for s in symbols 
        if s.get("quoteCoin") == "USDT" and s.get("state") == 0
    ]


# Test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("\n=== Testing MEXC Symbol Search ===\n")
    
    # Test finding potential symbols
    test_tokens = ["ARC", "AVA", "AUDIO", "BAN"]
    for token in test_tokens:
        symbols = find_potential_mexc_symbols(token)
        print(f"{token}: {symbols}")

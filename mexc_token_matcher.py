"""
MEXC Token Matcher - автоматическое сопоставление токенов MEXC с Jupiter/PancakeSwap.

Логика:
1. Получаем список всех фьючерсных пар с MEXC API (contract.mexc.com)
2. Ищем все возможные варианты по имени токена
3. Для каждого варианта получаем contractId из кэша
4. Получаем BSC/Solana адрес через coin introduce API
5. Сравниваем с введённым адресом - если совпало, возвращаем mexc_symbol
"""

import re
import httpx
import logging
import ssl
from typing import Optional, List, Dict, Tuple

logger = logging.getLogger(__name__)

MEXC_CONTRACT_DETAIL_URL = "https://contract.mexc.com/api/v1/contract/detail"
MEXC_FUTURES_PAGE_URL = "https://www.mexc.com/ru-RU/futures/{symbol}?type=linear_swap"
MEXC_COIN_INTRO_URL = "https://www.mexc.com/api/activity/contract/coin/introduce/v2"

# Browser-like headers
BROWSER_HEADERS = {
    "Host": "www.mexc.com",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
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


def get_contract_id_from_cache(mexc_symbol: str) -> Optional[int]:
    """
    Получает contractId из кэша символов (без дополнительного API запроса).
    
    Args:
        mexc_symbol: MEXC символ (e.g., "BEAMX_USDT")
    
    Returns:
        contractId или None
    """
    symbols = load_mexc_symbols()
    for sym_info in symbols:
        if sym_info.get("symbol") == mexc_symbol:
            contract_id = sym_info.get("id")
            if contract_id:
                logger.info(f"Got contractId from cache for {mexc_symbol}: {contract_id}")
                return contract_id
    
    logger.warning(f"No contractId found in cache for {mexc_symbol}")
    return None


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


# ========== BSC/PancakeSwap поддержка ==========

def _make_request_with_fallback(url: str, params: dict, proxy_url: Optional[str] = None) -> Optional[dict]:
    """
    Делает HTTP запрос с fallback: сначала с прокси, потом без.
    
    Args:
        url: URL для запроса
        params: Query параметры
        proxy_url: Optional proxy URL
    
    Returns:
        JSON response или None
    """
    attempts = []
    
    # Первая попытка - с прокси (если есть)
    if proxy_url:
        attempts.append(("with proxy", {"timeout": 30, "proxy": proxy_url}))
    
    # Вторая попытка - без прокси
    attempts.append(("without proxy", {"timeout": 30}))
    
    for attempt_name, client_kwargs in attempts:
        try:
            with httpx.Client(**client_kwargs) as client:
                response = client.get(url, params=params, headers=BROWSER_HEADERS)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Request successful {attempt_name}: {url}")
                return data
        except ssl.SSLError as e:
            logger.warning(f"SSL error {attempt_name} for {url}: {e}")
            continue
        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP {e.response.status_code} {attempt_name} for {url}")
            continue
        except Exception as e:
            logger.warning(f"Error {attempt_name} for {url}: {e}")
            continue
    
    logger.error(f"All attempts failed for {url}")
    return None


def get_bsc_address_from_contract_id(contract_id: int, proxy_url: Optional[str] = None) -> Optional[str]:
    """
    Получает BSC адрес токена через MEXC coin introduce API.
    Использует fallback: сначала с прокси, потом без.
    
    Args:
        contract_id: MEXC contractId
        proxy_url: Optional proxy URL
    
    Returns:
        BSC адрес (0x...) или None
    """
    data = _make_request_with_fallback(
        MEXC_COIN_INTRO_URL,
        {"language": "en-US", "contractId": contract_id},
        proxy_url
    )
    
    if not data:
        return None
    
    if data.get("success") and data.get("data"):
        # Проверяем explorerUrls (массив)
        explorer_urls = data["data"].get("explorerUrls", [])
        if not explorer_urls:
            # Пробуем explorerUrl (строка)
            explorer_url = data["data"].get("explorerUrl", "")
            if explorer_url:
                explorer_urls = [explorer_url]
        
        # Ищем BSC адрес в bscscan.com URLs
        for url in explorer_urls:
            if "bscscan.com/token/" in url:
                match = re.search(r'bscscan\.com/token/(0x[a-fA-F0-9]+)', url)
                if match:
                    bsc_address = match.group(1).lower()
                    logger.info(f"Found BSC address from contractId {contract_id}: {bsc_address}")
                    return bsc_address
        
        logger.warning(f"No BSC address found in explorerUrls for contractId {contract_id}")
        return None
    
    logger.warning(f"No data for contractId {contract_id}")
    return None


def extract_bsc_contract(mexc_symbol: str, proxy_url: Optional[str] = None) -> Optional[str]:
    """
    Извлекает BSC contract адрес для MEXC символа.
    
    Логика:
    1. Получаем contractId из кэша (без API запроса!)
    2. Получаем BSC адрес через coin introduce API (с fallback)
    
    Args:
        mexc_symbol: MEXC символ (e.g., "COAI_USDT")
        proxy_url: Optional proxy URL
    
    Returns:
        BSC адрес (0x...) или None
    """
    # Шаг 1: Получаем contractId из кэша
    contract_id = get_contract_id_from_cache(mexc_symbol)
    if not contract_id:
        return None
    
    # Шаг 2: Получаем BSC адрес
    return get_bsc_address_from_contract_id(contract_id, proxy_url)


def find_matching_bsc_address(
    base_token: str, 
    bsc_address: str,
    proxy_url: Optional[str] = None
) -> Optional[str]:
    """
    Находит MEXC символ, который соответствует данному BSC адресу.
    
    Args:
        base_token: Символ токена (e.g., "COAI")
        bsc_address: BSC адрес для сравнения (0x...)
        proxy_url: Optional proxy URL
    
    Returns:
        MEXC base symbol если найден, None иначе
    """
    # Нормализуем BSC адрес для сравнения
    bsc_address_lower = bsc_address.lower()
    
    # Получаем все возможные MEXC символы
    potential_symbols = find_potential_mexc_symbols(base_token)
    
    if not potential_symbols:
        logger.warning(f"No MEXC symbols found for {base_token}")
        return None
    
    logger.info(f"Checking {len(potential_symbols)} potential symbols for BSC match: {potential_symbols}")
    
    # Проверяем каждый символ
    for mexc_symbol in potential_symbols:
        found_bsc = extract_bsc_contract(mexc_symbol, proxy_url)
        
        if found_bsc and found_bsc.lower() == bsc_address_lower:
            # Нашли совпадение!
            base_coin = mexc_symbol.replace("_USDT", "")
            logger.info(f"✓ BSC match found: {base_token} -> {base_coin} (BSC verified)")
            return base_coin
        elif found_bsc:
            logger.info(f"✗ BSC mismatch for {mexc_symbol}: {found_bsc} != {bsc_address_lower}")
    
    logger.warning(f"No matching MEXC symbol found for {base_token} with BSC {bsc_address}")
    return None


# Test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("\n=== Testing MEXC Symbol Search ===\n")
    
    # Test finding potential symbols
    test_tokens = ["ARC", "AVA", "AUDIO", "BAN", "BEAM"]
    for token in test_tokens:
        symbols = find_potential_mexc_symbols(token)
        print(f"{token}: {symbols}")
    
    print("\n=== Testing contractId from cache ===\n")
    test_symbols = ["BEAMX_USDT", "BANK_USDT", "COAI_USDT"]
    for sym in test_symbols:
        cid = get_contract_id_from_cache(sym)
        print(f"{sym}: contractId = {cid}")

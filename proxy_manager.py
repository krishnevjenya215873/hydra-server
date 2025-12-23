"""
Proxy manager for server-side requests.
Handles proxy rotation, failure tracking, and protocol selection.
"""

import random
import logging
import threading
import time
import requests
from datetime import datetime
from typing import Optional, Dict, List
from sqlalchemy.orm import Session

from models import Proxy

logger = logging.getLogger(__name__)

# URL для проверки прокси
PROXY_CHECK_URL = "https://api.ipify.org?format=json"
PROXY_CHECK_TIMEOUT = 10  # секунд
PROXY_CHECK_INTERVAL = 3600  # 1 час в секундах


class ProxyManager:
    """Manages proxy pool for API requests."""
    
    def __init__(self):
        self._current_proxy: Optional[Dict] = None
        self._proxy_cache: List[Dict] = []
        self._cache_updated: Optional[datetime] = None
        self._cache_ttl_seconds = 60  # Refresh cache every minute
    
    def _refresh_cache(self, db: Session) -> None:
        """Refresh proxy cache from database."""
        proxies = db.query(Proxy).filter(
            Proxy.is_active == True,
            Proxy.fail_count < 5  # Skip proxies with too many failures
        ).all()
        
        self._proxy_cache = [
            {
                "id": p.id,
                "proxy_string": p.proxy_string,
                "protocol": p.protocol,
                "fail_count": p.fail_count
            }
            for p in proxies
        ]
        self._cache_updated = datetime.utcnow()
        logger.info(f"Proxy cache refreshed: {len(self._proxy_cache)} active proxies")
    
    def _should_refresh_cache(self) -> bool:
        """Check if cache needs refresh."""
        if not self._cache_updated:
            return True
        delta = (datetime.utcnow() - self._cache_updated).total_seconds()
        return delta > self._cache_ttl_seconds
    
    def get_proxy(self, db: Session) -> Optional[Dict]:
        """Get a random proxy from the pool."""
        if self._should_refresh_cache():
            self._refresh_cache(db)
        
        if not self._proxy_cache:
            logger.warning("No active proxies available")
            return None
        
        # Pick random proxy, preferring ones with fewer failures
        weights = [max(1, 5 - p["fail_count"]) for p in self._proxy_cache]
        proxy = random.choices(self._proxy_cache, weights=weights, k=1)[0]
        
        self._current_proxy = proxy
        return proxy
    
    def get_proxy_url(self, db: Session) -> Optional[str]:
        """Get proxy URL ready for requests."""
        proxy = self.get_proxy(db)
        if not proxy:
            logger.warning("get_proxy_url: No proxy available")
            return None
        
        protocol = proxy["protocol"].lower()
        proxy_string = proxy["proxy_string"]
        
        # Build URL
        if "://" in proxy_string:
            url = proxy_string
        else:
            scheme = "socks5" if protocol.startswith("socks") else "http"
            url = f"{scheme}://{proxy_string}"
        
        # Log which proxy is being used (hide password)
        display_url = url.split('@')[-1] if '@' in url else url
        logger.info(f"Selected proxy ID={proxy['id']}: {display_url}")
        return url
    
    def get_proxies_dict(self, db: Session) -> Dict[str, str]:
        """Get proxies dict for requests library."""
        url = self.get_proxy_url(db)
        if not url:
            return {}
        return {"http": url, "https": url}
    
    def mark_proxy_failed(self, db: Session, reason: str = "") -> None:
        """Mark current proxy as failed and pick a new one."""
        if not self._current_proxy:
            return
        
        proxy_id = self._current_proxy["id"]
        
        # Update fail count in database
        proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
        if proxy:
            proxy.fail_count += 1
            if proxy.fail_count >= 5:
                proxy.is_active = False
                logger.warning(f"Proxy {proxy_id} disabled after 5 failures")
            db.commit()
        
        # Update cache
        for p in self._proxy_cache:
            if p["id"] == proxy_id:
                p["fail_count"] += 1
                break
        
        # Remove from cache if too many failures
        self._proxy_cache = [p for p in self._proxy_cache if p["fail_count"] < 5]
        
        logger.info(f"Proxy {proxy_id} marked as failed: {reason}")
        
        # Pick new proxy
        self._current_proxy = None
    
    def mark_proxy_success(self, db: Session) -> None:
        """Mark current proxy as successful (reset fail count)."""
        if not self._current_proxy:
            return
        
        proxy_id = self._current_proxy["id"]
        
        proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
        if proxy and proxy.fail_count > 0:
            proxy.fail_count = 0
            proxy.last_used = datetime.utcnow()
            db.commit()
        
        # Update cache
        for p in self._proxy_cache:
            if p["id"] == proxy_id:
                p["fail_count"] = 0
                break
    
    def get_safe_host(self, proxy_url: str) -> str:
        """Get proxy host without credentials for logging."""
        try:
            rest = proxy_url.split("://", 1)[1]
            if "@" in rest:
                rest = rest.split("@", 1)[1]
            return rest
        except Exception:
            return proxy_url
    
    def check_proxy_health(self, proxy: Dict) -> Dict:
        """
        Проверяет работоспособность одного прокси.
        Возвращает словарь с результатами проверки.
        """
        protocol = proxy.get("protocol", "socks5").lower()
        proxy_string = proxy.get("proxy_string", "")
        
        # Строим URL прокси
        if "://" in proxy_string:
            proxy_url = proxy_string
        else:
            scheme = "socks5" if protocol.startswith("socks") else "http"
            proxy_url = f"{scheme}://{proxy_string}"
        
        proxies = {"http": proxy_url, "https": proxy_url}
        
        result = {
            "id": proxy.get("id"),
            "working": False,
            "response_time": None,
            "ip": None,
            "error": None,
            "checked_at": datetime.utcnow().isoformat()
        }
        
        try:
            start_time = time.time()
            response = requests.get(
                PROXY_CHECK_URL,
                proxies=proxies,
                timeout=PROXY_CHECK_TIMEOUT
            )
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                result["working"] = True
                result["response_time"] = round(response_time * 1000)  # в миллисекундах
                try:
                    result["ip"] = response.json().get("ip")
                except:
                    pass
                logger.info(f"Proxy {proxy.get('id')} OK: {result['response_time']}ms, IP: {result['ip']}")
            else:
                result["error"] = f"HTTP {response.status_code}"
                logger.warning(f"Proxy {proxy.get('id')} failed: HTTP {response.status_code}")
        except requests.exceptions.Timeout:
            result["error"] = "Timeout"
            logger.warning(f"Proxy {proxy.get('id')} failed: Timeout")
        except requests.exceptions.ProxyError as e:
            result["error"] = f"Proxy error: {str(e)[:50]}"
            logger.warning(f"Proxy {proxy.get('id')} failed: Proxy error")
        except Exception as e:
            result["error"] = str(e)[:100]
            logger.warning(f"Proxy {proxy.get('id')} failed: {e}")
        
        return result
    
    def check_all_proxies(self, db: Session) -> List[Dict]:
        """
        Проверяет все активные прокси и обновляет их статус в БД.
        """
        proxies = db.query(Proxy).filter(Proxy.is_active == True).all()
        results = []
        
        for proxy in proxies:
            proxy_dict = {
                "id": proxy.id,
                "proxy_string": proxy.proxy_string,
                "protocol": proxy.protocol
            }
            result = self.check_proxy_health(proxy_dict)
            results.append(result)
            
            # Обновляем статус в БД
            if result["working"]:
                proxy.fail_count = 0
                proxy.last_used = datetime.utcnow()
            else:
                proxy.fail_count += 1
                if proxy.fail_count >= 5:
                    proxy.is_active = False
                    logger.warning(f"Proxy {proxy.id} disabled after health check failures")
        
        db.commit()
        logger.info(f"Health check completed: {len([r for r in results if r['working']])}/{len(results)} proxies working")
        return results


class ProxyHealthChecker:
    """
    Фоновый процесс для периодической проверки прокси.
    """
    
    def __init__(self, proxy_manager: 'ProxyManager'):
        self._proxy_manager = proxy_manager
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._interval = PROXY_CHECK_INTERVAL
        self._last_check_results: List[Dict] = []
        self._last_check_time: Optional[datetime] = None
        self._lock = threading.Lock()
    
    def start(self):
        """Запускает фоновую проверку."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Proxy health checker started")
    
    def stop(self):
        """Останавливает фоновую проверку."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Proxy health checker stopped")
    
    def get_last_results(self) -> Dict:
        """Возвращает результаты последней проверки."""
        with self._lock:
            return {
                "results": list(self._last_check_results),
                "last_check": self._last_check_time.isoformat() if self._last_check_time else None,
                "next_check": (self._last_check_time.timestamp() + self._interval) if self._last_check_time else None
            }
    
    def force_check(self) -> List[Dict]:
        """Принудительно запускает проверку всех прокси."""
        return self._do_check()
    
    def _run_loop(self):
        """Основной цикл проверки."""
        # Первая проверка через 30 секунд после запуска
        time.sleep(30)
        
        while self._running:
            try:
                self._do_check()
            except Exception as e:
                logger.error(f"Proxy health check error: {e}")
            
            # Спим с проверкой флага остановки
            for _ in range(int(self._interval / 10)):
                if not self._running:
                    break
                time.sleep(10)
    
    def _do_check(self) -> List[Dict]:
        """Выполняет проверку прокси."""
        from models import SessionLocal
        
        db = SessionLocal()
        try:
            results = self._proxy_manager.check_all_proxies(db)
            with self._lock:
                self._last_check_results = results
                self._last_check_time = datetime.utcnow()
            return results
        finally:
            db.close()


# Global proxy manager instance
proxy_manager = ProxyManager()

# Global health checker instance
proxy_health_checker = ProxyHealthChecker(proxy_manager)

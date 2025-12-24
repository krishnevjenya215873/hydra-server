"""
Background worker for continuous price fetching.
Runs in a separate thread and broadcasts updates via WebSocket.
OPTIMIZED: No delays, immediate updates per token, maximum parallelism.
"""

import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Set, Callable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy.orm import Session

from models import Token, SpreadHistory
import models  # For accessing SessionLocal dynamically
from price_fetcher import price_fetcher

logger = logging.getLogger(__name__)

# Keep 2 days of history
HISTORY_RETENTION_HOURS = 48


class PriceWorker:
    """Background worker that continuously fetches prices."""
    
    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._interval = 0.0  # NO DELAY between cycles for real-time updates
        self._max_workers = 15  # Balanced: enough for parallel but not overloading
        self._latest_data: Dict[str, Dict] = {}
        self._callbacks: Set[Callable] = set()
        self._lock = threading.Lock()
        self._history_lock = threading.Lock()
        self._last_cleanup = 0
    
    def start(self):
        """Start the worker thread."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Price worker started (optimized: no delays, 50 workers)")
    
    def stop(self):
        """Stop the worker thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Price worker stopped")
    
    def set_interval(self, interval: float):
        """Set polling interval in seconds (0 = no delay)."""
        self._interval = max(0.0, interval)
    
    def register_callback(self, callback: Callable):
        """Register callback for data updates."""
        with self._lock:
            self._callbacks.add(callback)
    
    def unregister_callback(self, callback: Callable):
        """Unregister callback."""
        with self._lock:
            self._callbacks.discard(callback)
    
    def get_latest_data(self) -> Dict[str, Dict]:
        """Get latest price data for all tokens."""
        with self._lock:
            return dict(self._latest_data)
    
    def get_token_data(self, token_name: str) -> Optional[Dict]:
        """Get latest data for specific token."""
        with self._lock:
            return self._latest_data.get(token_name)
    
    def _run_loop(self):
        """Main worker loop - continuous fetching without delays."""
        while self._running:
            try:
                self._fetch_all_prices_streaming()
            except Exception as e:
                logger.error(f"Worker error: {e}")
                time.sleep(1)  # Only sleep on error
    
    def _fetch_all_prices_streaming(self):
        """Fetch prices for all tokens with IMMEDIATE updates as each completes."""
        if models.SessionLocal is None:
            logger.warning("Database not available, skipping price fetch")
            time.sleep(1)
            return
        
        db = models.SessionLocal()
        try:
            tokens = db.query(Token).filter(Token.is_active == True).all()
            
            if not tokens:
                time.sleep(1)
                return
            
            # Create list of token IDs to fetch
            token_ids = [(t.id, t.name) for t in tokens]
            
        finally:
            db.close()
        
        # Use thread pool for parallel fetching - STREAM results as they complete
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # Submit all tasks
            future_to_token = {
                executor.submit(self._fetch_token_safe, token_id): (token_id, token_name)
                for token_id, token_name in token_ids
            }
            
            # Process results AS THEY COMPLETE (not waiting for all)
            for future in as_completed(future_to_token):
                if not self._running:
                    break
                    
                token_id, token_name = future_to_token[future]
                try:
                    result = future.result(timeout=30)
                    if result:
                        # Update latest data immediately
                        with self._lock:
                            self._latest_data[token_name] = result
                        
                        # Notify callbacks immediately for this single token
                        self._notify_callbacks_single(token_name, result)
                        
                        # Save to history asynchronously
                        self._save_history_single(token_name, result)
                        
                except Exception as e:
                    logger.error(f"Error fetching {token_name}: {e}")
        
        # Cleanup old history periodically (every 5 minutes)
        current_time = time.time()
        if current_time - self._last_cleanup > 300:
            self._cleanup_old_history_async()
            self._last_cleanup = current_time
    
    def _fetch_token_safe(self, token_id: int) -> Optional[Dict]:
        """Fetch token data with its own DB session."""
        if models.SessionLocal is None:
            logger.warning("Database not available for token fetch")
            return None
        db = models.SessionLocal()
        try:
            token = db.query(Token).filter(Token.id == token_id).first()
            if not token:
                return None
            return price_fetcher.fetch_token_data(db, token)
        except Exception as e:
            logger.error(f"Error fetching token {token_id}: {e}")
            return None
        finally:
            db.close()
    
    def _save_history_single(self, token_name: str, data: Dict):
        """Save spread data for single token to history (async)."""
        def save():
            if models.SessionLocal is None:
                return
            db = models.SessionLocal()
            try:
                token = db.query(Token).filter(Token.name == token_name).first()
                if not token:
                    return
                
                timestamp = data.get("timestamp", time.time())
                spreads = data.get("spreads", {})
                
                for dex_name, spread_data in spreads.items():
                    history_entry = SpreadHistory(
                        token_id=token.id,
                        dex_name=dex_name,
                        timestamp=timestamp,
                        direct_spread=spread_data.get("direct"),
                        reverse_spread=spread_data.get("reverse"),
                        dex_price=spread_data.get("dex_price"),
                        cex_bid=spread_data.get("cex_bid"),
                        cex_ask=spread_data.get("cex_ask"),
                    )
                    db.add(history_entry)
                
                db.commit()
            except Exception as e:
                logger.error(f"Error saving history for {token_name}: {e}")
                db.rollback()
            finally:
                db.close()
        
        # Run in background thread to not block
        threading.Thread(target=save, daemon=True).start()
    
    def _cleanup_old_history_async(self):
        """Remove history older than 2 days (async)."""
        def cleanup():
            if models.SessionLocal is None:
                return
            db = models.SessionLocal()
            try:
                cutoff = time.time() - (HISTORY_RETENTION_HOURS * 3600)
                deleted = db.query(SpreadHistory).filter(
                    SpreadHistory.timestamp < cutoff
                ).delete(synchronize_session=False)
                db.commit()
                if deleted > 0:
                    logger.info(f"Cleaned up {deleted} old history entries")
            except Exception as e:
                logger.error(f"Error cleaning up history: {e}")
                db.rollback()
            finally:
                db.close()
        
        # Run in background thread
        threading.Thread(target=cleanup, daemon=True).start()
    
    def _notify_callbacks_single(self, token_name: str, data: Dict):
        """Notify all registered callbacks for a single token update."""
        with self._lock:
            callbacks = list(self._callbacks)
        
        # Send as single-token update
        snapshot = {token_name: data}
        
        for callback in callbacks:
            try:
                callback(snapshot)
            except Exception as e:
                logger.error(f"Callback error: {e}")
    
    def _notify_callbacks(self, snapshot: Dict[str, Dict]):
        """Notify all registered callbacks (batch mode - legacy)."""
        with self._lock:
            callbacks = list(self._callbacks)
        
        for callback in callbacks:
            try:
                callback(snapshot)
            except Exception as e:
                logger.error(f"Callback error: {e}")


# Global worker instance
price_worker = PriceWorker()

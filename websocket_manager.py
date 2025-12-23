"""
WebSocket manager for real-time data broadcasting.
"""

import asyncio
import json
import logging
from typing import Dict, Set, List, Any, Optional
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and subscriptions."""
    
    def __init__(self):
        # All active connections
        self._connections: Set[WebSocket] = set()
        # Token subscriptions: token_name -> set of websockets
        self._subscriptions: Dict[str, Set[WebSocket]] = {}
        # Reverse mapping: websocket -> set of token names
        self._client_subscriptions: Dict[WebSocket, Set[str]] = {}
        self._lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket):
        """Accept new WebSocket connection."""
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)
            self._client_subscriptions[websocket] = set()
        logger.info(f"Client connected. Total: {len(self._connections)}")
    
    async def disconnect(self, websocket: WebSocket):
        """Handle WebSocket disconnection."""
        async with self._lock:
            self._connections.discard(websocket)
            
            # Remove from all subscriptions
            if websocket in self._client_subscriptions:
                tokens = self._client_subscriptions.pop(websocket)
                for token in tokens:
                    if token in self._subscriptions:
                        self._subscriptions[token].discard(websocket)
                        if not self._subscriptions[token]:
                            del self._subscriptions[token]
        
        logger.info(f"Client disconnected. Total: {len(self._connections)}")
    
    async def subscribe(self, websocket: WebSocket, tokens: List[str]):
        """Subscribe client to token updates."""
        async with self._lock:
            for token in tokens:
                if token not in self._subscriptions:
                    self._subscriptions[token] = set()
                self._subscriptions[token].add(websocket)
                
                if websocket in self._client_subscriptions:
                    self._client_subscriptions[websocket].add(token)
        
        logger.debug(f"Client subscribed to: {tokens}")
    
    async def unsubscribe(self, websocket: WebSocket, tokens: List[str]):
        """Unsubscribe client from token updates."""
        async with self._lock:
            for token in tokens:
                if token in self._subscriptions:
                    self._subscriptions[token].discard(websocket)
                    if not self._subscriptions[token]:
                        del self._subscriptions[token]
                
                if websocket in self._client_subscriptions:
                    self._client_subscriptions[websocket].discard(token)
        
        logger.debug(f"Client unsubscribed from: {tokens}")
    
    async def subscribe_all(self, websocket: WebSocket):
        """Subscribe client to all token updates."""
        # Special marker for "all tokens"
        async with self._lock:
            if "__all__" not in self._subscriptions:
                self._subscriptions["__all__"] = set()
            self._subscriptions["__all__"].add(websocket)
            
            if websocket in self._client_subscriptions:
                self._client_subscriptions[websocket].add("__all__")
    
    async def broadcast_update(self, data: Dict[str, Any]):
        """Broadcast price update to subscribed clients."""
        if not data:
            return
        
        # Prepare message
        message = json.dumps({
            "type": "data",
            "payload": data
        })
        
        async with self._lock:
            # Get clients subscribed to "all"
            all_subscribers = self._subscriptions.get("__all__", set())
            
            # Get clients subscribed to specific tokens
            specific_subscribers: Set[WebSocket] = set()
            for token_name in data.keys():
                if token_name in self._subscriptions:
                    specific_subscribers.update(self._subscriptions[token_name])
            
            # Combine all subscribers
            all_clients = all_subscribers | specific_subscribers
        
        # Send to all relevant clients
        disconnected = []
        for websocket in all_clients:
            try:
                await websocket.send_text(message)
            except Exception as e:
                logger.warning(f"Error sending to client: {e}")
                disconnected.append(websocket)
        
        # Clean up disconnected clients
        for ws in disconnected:
            await self.disconnect(ws)
    
    async def send_personal(self, websocket: WebSocket, message: Dict):
        """Send message to specific client."""
        try:
            await websocket.send_text(json.dumps(message))
        except Exception as e:
            logger.warning(f"Error sending personal message: {e}")
    
    def get_connection_count(self) -> int:
        """Get number of active connections."""
        return len(self._connections)
    
    def get_subscribed_tokens(self) -> Set[str]:
        """Get set of all subscribed tokens."""
        return set(self._subscriptions.keys()) - {"__all__"}


# Global connection manager
connection_manager = ConnectionManager()


async def handle_websocket_message(websocket: WebSocket, message: str):
    """Handle incoming WebSocket message."""
    try:
        data = json.loads(message)
        msg_type = data.get("type", "")
        payload = data.get("payload", {})
        
        if msg_type == "subscribe":
            tokens = payload.get("tokens", [])
            if tokens:
                await connection_manager.subscribe(websocket, tokens)
                await connection_manager.send_personal(websocket, {
                    "type": "subscribed",
                    "payload": {"tokens": tokens}
                })
        
        elif msg_type == "subscribe_all":
            await connection_manager.subscribe_all(websocket)
            await connection_manager.send_personal(websocket, {
                "type": "subscribed",
                "payload": {"all": True}
            })
        
        elif msg_type == "unsubscribe":
            tokens = payload.get("tokens", [])
            if tokens:
                await connection_manager.unsubscribe(websocket, tokens)
                await connection_manager.send_personal(websocket, {
                    "type": "unsubscribed",
                    "payload": {"tokens": tokens}
                })
        
        elif msg_type == "ping":
            await connection_manager.send_personal(websocket, {
                "type": "pong",
                "payload": {}
            })
        
        else:
            await connection_manager.send_personal(websocket, {
                "type": "error",
                "payload": {"message": f"Unknown message type: {msg_type}"}
            })
    
    except json.JSONDecodeError:
        await connection_manager.send_personal(websocket, {
            "type": "error",
            "payload": {"message": "Invalid JSON"}
        })
    except Exception as e:
        logger.error(f"Error handling WebSocket message: {e}")
        await connection_manager.send_personal(websocket, {
            "type": "error",
            "payload": {"message": str(e)}
        })

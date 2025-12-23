#!/usr/bin/env python3
"""
HYDRA Server Launcher
============================

Run this script to start the HYDRA server.

Usage:
    python run_server.py [--host HOST] [--port PORT]

Default:
    Host: 0.0.0.0
    Port: 8000

Admin Panel:
    http://localhost:8000/admin
    Default credentials: admin / admin123
    
    IMPORTANT: Change the default password after first login!
"""

import argparse
import uvicorn


def main():
    parser = argparse.ArgumentParser(description="HYDRA Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    args = parser.parse_args()
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    HYDRA SERVER                       ║
╠══════════════════════════════════════════════════════════════╣
║  Server:     http://{args.host}:{args.port}                          ║
║  Admin:      http://{args.host}:{args.port}/admin                    ║
║  WebSocket:  ws://{args.host}:{args.port}/ws                         ║
╠══════════════════════════════════════════════════════════════╣
║  Default admin: admin / admin123                             ║
║  ⚠️  Change password after first login!                       ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    uvicorn.run(
        "main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info"
    )


if __name__ == "__main__":
    main()

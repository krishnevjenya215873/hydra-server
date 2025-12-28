import os
import subprocess
import sys
import multiprocessing

port = os.environ.get('PORT', '8000')

# Calculate optimal workers based on CPU cores
# For I/O bound tasks (API requests), use 2-4x CPU cores
cpu_count = multiprocessing.cpu_count()
workers = int(os.environ.get('WEB_CONCURRENCY', min(cpu_count * 2, 8)))

print(f"Starting server on port {port} with {workers} workers (CPU cores: {cpu_count})")

# Use Gunicorn with Uvicorn workers for production
# This provides better process management and stability
subprocess.run([
    sys.executable, '-m', 'uvicorn', 
    'main:app', 
    '--host', '0.0.0.0', 
    '--port', port,
    '--workers', str(workers),
    '--loop', 'uvloop',  # Faster event loop
    '--http', 'httptools',  # Faster HTTP parser
    '--timeout-keep-alive', '30',
    '--limit-concurrency', '1000',  # Max concurrent connections
])

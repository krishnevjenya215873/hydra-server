FROM python:3.11-slim

WORKDIR /app

# Install Node.js for cloudscraper nodejs interpreter
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip install PySocks

# Copy application code
COPY . .

# Create admin panel templates directory
RUN mkdir -p admin_panel/templates

# Expose port
EXPOSE 8000

# Run the application
CMD ["python", "start_server.py"]

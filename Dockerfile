FROM python:3.11.9-slim

WORKDIR /app

# Install system dependencies for better performance
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create admin panel templates directory
RUN mkdir -p admin_panel/templates

# Expose port
EXPOSE 8000

# Environment variables for optimization
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Run the application with optimized settings
CMD ["python", "start_server.py"]

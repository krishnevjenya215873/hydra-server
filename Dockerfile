FROM python:3.11.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create admin panel templates directory
RUN mkdir -p admin_panel/templates

# Expose port
EXPOSE 8000

# Run the application
CMD ["python", "start_server.py"]

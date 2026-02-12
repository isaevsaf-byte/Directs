# Slim Python image (much lighter than Playwright base - saves ~1GB)
FROM python:3.11-slim-bookworm

# Set working directory
WORKDIR /app

# Install minimal system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (for caching)
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Railway injects $PORT at runtime; default to 8000 for local Docker
ENV PORT=8000

# Use explicit sh -c to guarantee $PORT is expanded at runtime
CMD ["sh", "-c", "uvicorn src.api.main:app --host 0.0.0.0 --port ${PORT}"]

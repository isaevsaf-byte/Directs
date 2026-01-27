# Use the official Playwright Python image (includes Chromium/Firefox/Webkit)
# This is crucial: It solves the "missing browser" errors automatically.
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# Set working directory
WORKDIR /app

# Install system dependencies (if any extra are needed, usually covered by base image)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (for caching)
COPY requirements.txt ./

# Install Python dependencies
# No need to install playwright browsers again; the base image has them.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port FastAPI will run on
EXPOSE 8000

# Command to run the application (using the entrypoint we created)
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

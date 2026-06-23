FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if any are needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose port 7860 for Hugging Face Spaces
EXPOSE 7860

# Make entrypoint script executable
RUN chmod +x entrypoint.sh

CMD ["./entrypoint.sh"]

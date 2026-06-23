#!/bin/bash

# Start Celery worker in the background
echo "Starting Celery worker..."
celery -A tasks worker --loglevel=info &

# Start Uvicorn FastAPI server on port 7860 (Hugging Face Spaces default port)
echo "Starting FastAPI gateway on port 7860..."
exec uvicorn main:app --host 0.0.0.0 --port 7860

#!/bin/bash

# Census Grid Stats API Startup Script

echo "Starting Census Grid Stats API..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -e .

# Set environment variables (optional)
if [ -f ".env" ]; then
    echo "Loading environment variables from .env file..."
    export $(cat .env | xargs)
fi

# Start the server
echo "Starting FastAPI server..."
echo "API will be available at: http://localhost:8000"
echo "Interactive docs at: http://localhost:8000/docs"
echo "Press Ctrl+C to stop the server"

uvicorn main:app --host 0.0.0.0 --port 8000 --reload

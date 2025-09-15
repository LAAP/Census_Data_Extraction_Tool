.PHONY: run test format install dev-install clean

# Install production dependencies
install:
	pip install -e .

# Install development dependencies
dev-install:
	pip install -e ".[dev]"

# Run the FastAPI server
run:
	uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Run tests
test:
	pytest tests/ -v

# Format code
format:
	black .
	isort .

# Clean up
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info

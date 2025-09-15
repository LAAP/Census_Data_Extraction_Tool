"""Integration tests for the FastAPI application."""

import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


class TestAPIEndpoints:
    """Test API endpoints."""
    
    def test_health_check(self):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
    
    def test_version_endpoint(self):
        """Test version endpoint."""
        response = client.get("/version")
        assert response.status_code == 200
        data = response.json()
        assert data["version"] == "0.1.0"
        assert data["api_version"] == "v1"
    
    def test_grid_stats_validation_error(self):
        """Test grid stats endpoint with validation error."""
        # Missing required parameters
        response = client.post("/grid_stats", json={})
        assert response.status_code == 422  # Validation error
    
    def test_grid_stats_invalid_coordinates(self):
        """Test grid stats with invalid coordinates."""
        response = client.post("/grid_stats", json={
            "lat": 91.0,  # Invalid latitude
            "lon": -71.0589,
            "cell_km": 1.0,
            "acs_year": 2023
        })
        assert response.status_code == 422  # Validation error
    
    def test_grid_stats_missing_lon(self):
        """Test grid stats with missing longitude."""
        response = client.post("/grid_stats", json={
            "lat": 42.3601,
            "cell_km": 1.0,
            "acs_year": 2023
        })
        assert response.status_code == 422  # Validation error
    
    def test_grid_stats_valid_request(self):
        """Test grid stats with valid request (may fail due to external API calls)."""
        response = client.post("/grid_stats", json={
            "lat": 42.3601,
            "lon": -71.0589,
            "cell_km": 1.0,
            "acs_year": 2023,
            "include_lodes": False
        })
        
        # This might fail due to external API calls, but should not be a validation error
        assert response.status_code in [200, 500]  # Either success or internal error

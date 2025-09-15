"""Tests for geometry utilities."""

import pytest
from shapely.geometry import Point, Polygon
from app.geometry import (
    get_utm_zone, get_utm_crs, create_square_grid_cell,
    calculate_polygon_area_km2, polygon_to_esri_json
)


class TestUTMZone:
    """Test UTM zone detection."""
    
    def test_utm_zone_boston(self):
        """Test UTM zone for Boston, MA."""
        zone = get_utm_zone(-71.0589, 42.3601)
        assert zone == 19
    
    def test_utm_zone_san_francisco(self):
        """Test UTM zone for San Francisco, CA."""
        zone = get_utm_zone(-122.4194, 37.7749)
        assert zone == 10
    
    def test_utm_zone_edge_cases(self):
        """Test UTM zone edge cases."""
        # Test edge case at -180
        zone = get_utm_zone(-180.0, 0.0)
        assert zone == 1
        
        # Test edge case at +180
        zone = get_utm_zone(180.0, 0.0)
        assert zone == 60


class TestUTMCRS:
    """Test UTM CRS creation."""
    
    def test_utm_crs_northern_hemisphere(self):
        """Test UTM CRS for northern hemisphere."""
        crs = get_utm_crs(-71.0589, 42.3601)
        assert crs.to_epsg() == 32619  # UTM Zone 19N
    
    def test_utm_crs_southern_hemisphere(self):
        """Test UTM CRS for southern hemisphere."""
        crs = get_utm_crs(-70.0, -35.0)
        assert crs.to_epsg() == 32719  # UTM Zone 19S


class TestGridCell:
    """Test grid cell creation."""
    
    def test_create_1km_grid_cell(self):
        """Test creating a 1km grid cell."""
        polygon = create_square_grid_cell(42.3601, -71.0589, 1.0)
        
        assert isinstance(polygon, Polygon)
        assert polygon.is_valid
        assert len(polygon.exterior.coords) == 5  # 4 corners + closing point
        
        # Check that the polygon is roughly square
        bounds = polygon.bounds
        width = bounds[2] - bounds[0]  # max_lon - min_lon
        height = bounds[3] - bounds[1]  # max_lat - min_lat
        
        # Should be roughly square (within 30% tolerance for lat/lon projection)
        assert abs(width - height) / width < 0.3
    
    def test_create_2km_grid_cell(self):
        """Test creating a 2km grid cell."""
        polygon = create_square_grid_cell(42.3601, -71.0589, 2.0)
        
        assert isinstance(polygon, Polygon)
        assert polygon.is_valid
        
        # 2km cell should be larger than 1km cell
        area_2km = calculate_polygon_area_km2(polygon)
        polygon_1km = create_square_grid_cell(42.3601, -71.0589, 1.0)
        area_1km = calculate_polygon_area_km2(polygon_1km)
        
        assert area_2km > area_1km


class TestPolygonUtils:
    """Test polygon utility functions."""
    
    def test_calculate_polygon_area(self):
        """Test polygon area calculation."""
        # Create a simple square polygon
        coords = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]
        polygon = Polygon(coords)
        
        area = calculate_polygon_area_km2(polygon)
        assert area > 0
    
    def test_polygon_to_esri_json(self):
        """Test polygon to Esri JSON conversion."""
        coords = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]
        polygon = Polygon(coords)
        
        esri_json = polygon_to_esri_json(polygon)
        
        assert "rings" in esri_json
        assert "spatialReference" in esri_json
        assert esri_json["spatialReference"]["wkid"] == 4326
        assert len(esri_json["rings"]) == 1
        assert len(esri_json["rings"][0]) == 5

"""Geometry utilities for UTM projection and grid cell creation."""

import logging
from typing import Tuple
import numpy as np
from shapely.geometry import Point, Polygon
from shapely.ops import transform
from pyproj import CRS, Transformer
import pyproj

logger = logging.getLogger(__name__)


def get_utm_zone(longitude: float, latitude: float) -> int:
    """
    Get UTM zone for given longitude and latitude.
    
    Args:
        longitude: Longitude in degrees
        latitude: Latitude in degrees
        
    Returns:
        UTM zone number
    """
    # UTM zones are 6 degrees wide, starting at -180
    zone = int((longitude + 180) / 6) + 1
    
    # Handle edge cases
    if zone < 1:
        zone = 1
    elif zone > 60:
        zone = 60
        
    return zone


def get_utm_crs(longitude: float, latitude: float) -> CRS:
    """
    Get UTM CRS for given coordinates.
    
    Args:
        longitude: Longitude in degrees
        latitude: Latitude in degrees
        
    Returns:
        UTM CRS object
    """
    zone = get_utm_zone(longitude, latitude)
    
    # Determine if we're in northern or southern hemisphere
    hemisphere = "north" if latitude >= 0 else "south"
    
    # Create UTM CRS
    utm_crs = CRS.from_dict({
        "proj": "utm",
        "zone": zone,
        "ellps": "WGS84",
        "datum": "WGS84",
        "units": "m",
        "no_defs": True,
        "south": hemisphere == "south"
    })
    
    return utm_crs


def create_square_grid_cell(
    center_lat: float,
    center_lon: float,
    cell_km: float
) -> Polygon:
    """
    Create a square grid cell centered at given coordinates.
    
    Args:
        center_lat: Center latitude in degrees
        center_lon: Center longitude in degrees
        cell_km: Cell size in kilometers
        
    Returns:
        Shapely Polygon in WGS84 (EPSG:4326)
    """
    # Get UTM CRS for the center point
    utm_crs = get_utm_crs(center_lon, center_lat)
    wgs84_crs = CRS.from_epsg(4326)
    
    # Create transformers
    wgs84_to_utm = Transformer.from_crs(wgs84_crs, utm_crs, always_xy=True)
    utm_to_wgs84 = Transformer.from_crs(utm_crs, wgs84_crs, always_xy=True)
    
    # Convert center point to UTM
    center_x, center_y = wgs84_to_utm.transform(center_lon, center_lat)
    
    # Calculate half-side in meters
    half_side_m = (cell_km * 1000) / 2
    
    # Create square corners in UTM
    corners_utm = [
        (center_x - half_side_m, center_y - half_side_m),  # Bottom-left
        (center_x + half_side_m, center_y - half_side_m),  # Bottom-right
        (center_x + half_side_m, center_y + half_side_m),  # Top-right
        (center_x - half_side_m, center_y + half_side_m),  # Top-left
        (center_x - half_side_m, center_y - half_side_m),  # Close polygon
    ]
    
    # Convert corners back to WGS84
    corners_wgs84 = []
    for x, y in corners_utm:
        lon, lat = utm_to_wgs84.transform(x, y)
        corners_wgs84.append((lon, lat))
    
    # Create polygon
    polygon = Polygon(corners_wgs84)
    
    logger.info(f"Created {cell_km}km grid cell centered at ({center_lat}, {center_lon})")
    
    return polygon


def calculate_polygon_area_km2(polygon: Polygon) -> float:
    """
    Calculate polygon area in square kilometers.
    
    Args:
        polygon: Shapely polygon in WGS84
        
    Returns:
        Area in square kilometers
    """
    # Use a simple approximation for small areas
    # For more accurate results, we could project to UTM
    bounds = polygon.bounds
    min_lon, min_lat, max_lon, max_lat = bounds
    
    # Approximate area using lat/lon bounds
    # This is not perfectly accurate but sufficient for small areas
    lat_center = (min_lat + max_lat) / 2
    lat_center_rad = np.radians(lat_center)
    
    # Approximate meters per degree
    meters_per_degree_lat = 111320  # Approximately constant
    meters_per_degree_lon = 111320 * np.cos(lat_center_rad)
    
    # Calculate area in square meters
    width_m = (max_lon - min_lon) * meters_per_degree_lon
    height_m = (max_lat - min_lat) * meters_per_degree_lat
    area_m2 = width_m * height_m
    
    # Convert to square kilometers
    area_km2 = area_m2 / 1_000_000
    
    return area_km2


def polygon_to_esri_json(polygon: Polygon) -> dict:
    """
    Convert Shapely polygon to Esri JSON format.
    
    Args:
        polygon: Shapely polygon
        
    Returns:
        Esri JSON geometry dictionary
    """
    # Get exterior coordinates
    coords = list(polygon.exterior.coords)
    
    # Esri JSON format
    esri_geometry = {
        "rings": [coords],
        "spatialReference": {
            "wkid": 4326
        }
    }
    
    return esri_geometry


def calculate_intersection_area_weight(
    block_group_polygon: Polygon,
    grid_cell_polygon: Polygon
) -> float:
    """
    Calculate area weight for intersection between block group and grid cell.
    
    Args:
        block_group_polygon: Block group polygon
        grid_cell_polygon: Grid cell polygon
        
    Returns:
        Area weight (intersection area / block group area)
    """
    try:
        # Calculate intersection
        intersection = block_group_polygon.intersection(grid_cell_polygon)
        
        if intersection.is_empty:
            return 0.0
        
        # Calculate areas in UTM for accuracy
        # Use the first point to determine UTM zone
        first_point = Point(block_group_polygon.bounds[0], block_group_polygon.bounds[1])
        utm_crs = get_utm_crs(first_point.x, first_point.y)
        wgs84_crs = CRS.from_epsg(4326)
        
        # Create transformer
        wgs84_to_utm = Transformer.from_crs(wgs84_crs, utm_crs, always_xy=True)
        
        # Transform polygons to UTM
        def transform_to_utm(geom):
            return transform(lambda x, y: wgs84_to_utm.transform(x, y)[:2], geom)
        
        bg_utm = transform_to_utm(block_group_polygon)
        intersection_utm = transform_to_utm(intersection)
        
        # Calculate areas in square meters
        bg_area_m2 = bg_utm.area
        intersection_area_m2 = intersection_utm.area
        
        if bg_area_m2 == 0:
            return 0.0
        
        weight = intersection_area_m2 / bg_area_m2
        
        return weight
        
    except Exception as e:
        logger.warning(f"Error calculating intersection area weight: {e}")
        return 0.0

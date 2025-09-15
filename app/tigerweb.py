"""TIGERweb integration for Census Block Groups."""

import logging
from typing import List, Dict, Any, Tuple
import json
import httpx
from shapely.geometry import shape
from tenacity import retry, stop_after_attempt, wait_exponential

from .geometry import polygon_to_esri_json, calculate_intersection_area_weight

logger = logging.getLogger(__name__)

TIGERWEB_BASE_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/10"


class TIGERwebError(Exception):
    """Raised when TIGERweb query fails."""
    pass


class BlockGroup:
    """Represents a Census Block Group with geometry and attributes."""
    
    def __init__(self, geoid: str, state: str, county: str, tract: str, blkgrp: str, geometry: Any):
        self.geoid = geoid
        self.state = state
        self.county = county
        self.tract = tract
        self.blkgrp = blkgrp
        self.geometry = geometry
        self.area_weight = 0.0
    
    def __repr__(self):
        return f"BlockGroup(geoid={self.geoid}, state={self.state}, county={self.county})"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
async def query_block_groups(
    grid_cell_polygon: Any,
    max_records: int = 1000
) -> List[BlockGroup]:
    """
    Query TIGERweb for Census Block Groups that intersect the grid cell.
    
    Args:
        grid_cell_polygon: Shapely polygon representing the grid cell
        max_records: Maximum number of records to return
        
    Returns:
        List of BlockGroup objects
        
    Raises:
        TIGERwebError: If query fails
    """
    # Convert polygon to Esri JSON
    esri_geometry = polygon_to_esri_json(grid_cell_polygon)
    
    # Build query parameters
    params = {
        "f": "geojson",
        "where": "1=1",
        "geometry": json.dumps(esri_geometry),
        "geometryType": "esriGeometryPolygon",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "GEOID,STATE,COUNTY,TRACT,BLKGRP",
        "returnGeometry": "true",
        "maxRecordCount": str(max_records)
    }
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(TIGERWEB_BASE_URL + "/query", params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if "error" in data:
                error_msg = data["error"].get("message", "Unknown TIGERweb error")
                raise TIGERwebError(f"TIGERweb error: {error_msg}")
            
            if "features" not in data:
                raise TIGERwebError("No features in TIGERweb response")
            
            features = data["features"]
            block_groups = []
            
            for feature in features:
                try:
                    # Extract attributes
                    props = feature.get("properties", {})
                    geoid = props.get("GEOID", "")
                    state = props.get("STATE", "")
                    county = props.get("COUNTY", "")
                    tract = props.get("TRACT", "")
                    blkgrp = props.get("BLKGRP", "")
                    
                    # Extract geometry
                    geometry_data = feature.get("geometry", {})
                    if not geometry_data:
                        logger.warning(f"No geometry for block group {geoid}")
                        continue
                    
                    # Convert to Shapely geometry
                    geometry = shape(geometry_data)
                    
                    # Create BlockGroup object
                    bg = BlockGroup(
                        geoid=geoid,
                        state=state,
                        county=county,
                        tract=tract,
                        blkgrp=blkgrp,
                        geometry=geometry
                    )
                    
                    # Calculate area weight
                    bg.area_weight = calculate_intersection_area_weight(
                        bg.geometry, grid_cell_polygon
                    )
                    
                    # Only include block groups with non-zero intersection
                    if bg.area_weight > 0:
                        block_groups.append(bg)
                    
                except Exception as e:
                    logger.warning(f"Error processing block group feature: {e}")
                    continue
            
            logger.info(f"Found {len(block_groups)} intersecting block groups")
            return block_groups
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error during TIGERweb query: {e}")
        raise TIGERwebError(f"HTTP error during TIGERweb query: {e}")
    except httpx.RequestError as e:
        logger.error(f"Request error during TIGERweb query: {e}")
        raise TIGERwebError(f"Request error during TIGERweb query: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during TIGERweb query: {e}")
        raise TIGERwebError(f"Unexpected error during TIGERweb query: {e}")


def get_census_api_geography(block_groups: List[BlockGroup]) -> List[Dict[str, str]]:
    """
    Get Census API geography parameters for block groups.
    
    Args:
        block_groups: List of BlockGroup objects
        
    Returns:
        List of geography dictionaries for Census API calls
    """
    geographies = []
    
    for bg in block_groups:
        geography = {
            "state": bg.state,
            "county": bg.county,
            "tract": bg.tract,
            "block_group": bg.blkgrp,
            "geoid": bg.geoid,
            "area_weight": bg.area_weight
        }
        geographies.append(geography)
    
    return geographies


def group_block_groups_by_state_county(block_groups: List[BlockGroup]) -> Dict[Tuple[str, str], List[BlockGroup]]:
    """
    Group block groups by state and county for efficient API calls.
    
    Args:
        block_groups: List of BlockGroup objects
        
    Returns:
        Dictionary mapping (state, county) tuples to lists of BlockGroup objects
    """
    grouped = {}
    
    for bg in block_groups:
        key = (bg.state, bg.county)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(bg)
    
    return grouped

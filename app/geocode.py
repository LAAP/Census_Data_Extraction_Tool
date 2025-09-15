"""Geocoding utilities using Census Geocoder."""

import logging
from typing import Tuple, Optional
from urllib.parse import quote_plus
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

CENSUS_GEOCODER_BASE = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"


class GeocodingError(Exception):
    """Raised when geocoding fails."""
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
async def geocode_address(
    address: str,
    benchmark: str = "Public_AR_Current"
) -> Tuple[float, float]:
    """
    Geocode an address using Census Geocoder.
    
    Args:
        address: Address string to geocode
        benchmark: Geocoding benchmark to use
        
    Returns:
        Tuple of (latitude, longitude)
        
    Raises:
        GeocodingError: If geocoding fails
    """
    params = {
        "address": address,
        "benchmark": benchmark,
        "format": "json"
    }
    
    url = f"{CENSUS_GEOCODER_BASE}?address={quote_plus(address)}&benchmark={benchmark}&format=json"
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            
            data = response.json()
            
            # Parse the response
            if "result" not in data:
                raise GeocodingError("No result in geocoding response")
                
            result = data["result"]
            if "addressMatches" not in result:
                raise GeocodingError("No address matches found")
                
            matches = result["addressMatches"]
            if not matches:
                raise GeocodingError(f"No address matches found for: {address}")
                
            # Get the first match
            match = matches[0]
            if "coordinates" not in match:
                raise GeocodingError("No coordinates in geocoding result")
                
            coords = match["coordinates"]
            if "x" not in coords or "y" not in coords:
                raise GeocodingError("Invalid coordinates in geocoding result")
                
            # Census Geocoder returns x=longitude, y=latitude
            longitude = float(coords["x"])
            latitude = float(coords["y"])
            
            logger.info(f"Successfully geocoded '{address}' to ({latitude}, {longitude})")
            return latitude, longitude
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error during geocoding: {e}")
        raise GeocodingError(f"HTTP error during geocoding: {e}")
    except httpx.RequestError as e:
        logger.error(f"Request error during geocoding: {e}")
        raise GeocodingError(f"Request error during geocoding: {e}")
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Error parsing geocoding response: {e}")
        raise GeocodingError(f"Error parsing geocoding response: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during geocoding: {e}")
        raise GeocodingError(f"Unexpected error during geocoding: {e}")


async def get_coordinates(
    address: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None
) -> Tuple[float, float]:
    """
    Get coordinates from either address or lat/lon.
    
    Args:
        address: Address to geocode
        lat: Latitude
        lon: Longitude
        
    Returns:
        Tuple of (latitude, longitude)
        
    Raises:
        GeocodingError: If geocoding fails
        ValueError: If neither address nor lat/lon provided
    """
    if address:
        return await geocode_address(address)
    elif lat is not None and lon is not None:
        return lat, lon
    else:
        raise ValueError("Either address or both lat/lon must be provided")

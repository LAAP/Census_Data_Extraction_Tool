"""LODES WAC data fetching and spatial filtering."""

import logging
import os
import zipfile
import tempfile
from typing import Dict, Any, List, Optional
import httpx
import pandas as pd
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

LODES_BASE_URL = "https://lehd.ces.census.gov/data/lodes/LODES8"


class LODESError(Exception):
    """Raised when LODES data fetching fails."""
    pass


class LODESClient:
    """Client for LODES WAC data with caching."""
    
    def __init__(self, cache_dir: Optional[str] = None):
        self.cache_dir = Path(cache_dir) if cache_dir else Path(tempfile.gettempdir()) / "lodes_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_state_abbrev(self, state_fips: str) -> str:
        """Convert state FIPS code to postal abbreviation."""
        state_mapping = {
            "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO",
            "09": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI",
            "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
            "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
            "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
            "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
            "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
            "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
            "54": "WV", "55": "WI", "56": "WY"
        }
        return state_mapping.get(state_fips, state_fips)
    
    def _get_latest_year(self, state_abbrev: str) -> int:
        """Get the latest available year for LODES data."""
        # For now, return 2022 as it's typically the latest available
        # In production, you might want to check available years dynamically
        return 2022
    
    def _get_cache_path(self, state_abbrev: str, year: int) -> Path:
        """Get cache file path for state and year."""
        return self.cache_dir / f"{state_abbrev}_{year}_wac.csv"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def _download_lodes_data(self, state_abbrev: str, year: int) -> Path:
        """Download LODES WAC data for a state and year."""
        url = f"{LODES_BASE_URL}/{state_abbrev.lower()}/wac/{state_abbrev.lower()}_wac_S000_JT00_{year}.csv.gz"
        cache_path = self._get_cache_path(state_abbrev, year)
        
        if cache_path.exists():
            logger.info(f"Using cached LODES data for {state_abbrev} {year}")
            return cache_path
        
        try:
            logger.info(f"Downloading LODES data for {state_abbrev} {year}")
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                
                # Save to cache
                with open(cache_path, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"Downloaded and cached LODES data for {state_abbrev} {year}")
                return cache_path
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error downloading LODES data: {e}")
            raise LODESError(f"HTTP error downloading LODES data: {e}")
        except httpx.RequestError as e:
            logger.error(f"Request error downloading LODES data: {e}")
            raise LODESError(f"Request error downloading LODES data: {e}")
        except Exception as e:
            logger.error(f"Unexpected error downloading LODES data: {e}")
            raise LODESError(f"Unexpected error downloading LODES data: {e}")
    
    async def get_lodes_data(
        self,
        state_fips: str,
        year: Optional[int] = None,
        block_geoids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get LODES WAC data for a state.
        
        Args:
            state_fips: State FIPS code
            year: LODES year (defaults to latest available)
            block_geoids: List of block GEOIDs to filter by (optional)
            
        Returns:
            Dictionary with LODES metrics
        """
        state_abbrev = self._get_state_abbrev(state_fips)
        
        if year is None:
            year = self._get_latest_year(state_abbrev)
        
        try:
            # Download or get cached data
            cache_path = await self._download_lodes_data(state_abbrev, year)
            
            # Read the CSV data
            df = pd.read_csv(cache_path)
            
            # Filter by block GEOIDs if provided
            if block_geoids:
                df = df[df['createdate'].str[:15].isin(block_geoids)]
            
            # Calculate metrics
            metrics = self._calculate_lodes_metrics(df)
            
            return {
                "enabled": True,
                "year": year,
                "state": state_abbrev,
                "metrics": metrics
            }
            
        except Exception as e:
            logger.error(f"Error processing LODES data: {e}")
            return {
                "enabled": False,
                "year": year,
                "state": state_abbrev,
                "error": str(e),
                "metrics": {
                    "total_jobs": 0,
                    "earnings_bands": {"E1": 0, "E2": 0, "E3": 0},
                    "by_sector": {"NAICS11": 0, "NAICS21": 0}
                }
            }
    
    def _calculate_lodes_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate LODES metrics from dataframe."""
        if df.empty:
            return {
                "total_jobs": 0,
                "earnings_bands": {"E1": 0, "E2": 0, "E3": 0},
                "by_sector": {"NAICS11": 0, "NAICS21": 0}
            }
        
        # Total jobs
        total_jobs = int(df['C000'].sum())
        
        # Earnings bands
        earnings_bands = {
            "E1": int(df['CE01'].sum()),  # $1,250/month or less
            "E2": int(df['CE02'].sum()),  # $1,251 to $3,333/month
            "E3": int(df['CE03'].sum())   # $3,334/month or more
        }
        
        # By sector (NAICS 2-digit)
        by_sector = {
            "NAICS11": int(df['CNS01'].sum()),  # Agriculture, forestry, fishing and hunting
            "NAICS21": int(df['CNS02'].sum())   # Mining, quarrying, and oil and gas extraction
        }
        
        return {
            "total_jobs": total_jobs,
            "earnings_bands": earnings_bands,
            "by_sector": by_sector
        }
    
    async def get_block_geoids_for_polygon(
        self,
        polygon: Any,
        state_fips: str
    ) -> List[str]:
        """
        Get block GEOIDs that intersect with a polygon.
        
        This is a simplified implementation that would need to be enhanced
        with actual spatial intersection logic in production.
        
        Args:
            polygon: Shapely polygon
            state_fips: State FIPS code
            
        Returns:
            List of block GEOIDs
        """
        # For now, return empty list
        # In production, you would:
        # 1. Query TIGERweb for Census Blocks layer
        # 2. Find blocks that intersect with the polygon
        # 3. Return their GEOIDs
        
        logger.warning("Block GEOID filtering not implemented - returning empty list")
        return []


async def get_lodes_metrics(
    block_group_data: List[Dict[str, Any]],
    grid_cell_polygon: Any,
    lodes_year: Optional[int] = None
) -> Dict[str, Any]:
    """
    Get LODES metrics for a grid cell.
    
    Args:
        block_group_data: List of block group data
        grid_cell_polygon: Grid cell polygon
        lodes_year: LODES year
        
    Returns:
        LODES metrics dictionary
    """
    if not block_group_data:
        return {
            "total_jobs": 0,
            "earnings_bands": {"E1": 0, "E2": 0, "E3": 0},
            "by_sector": {"NAICS11": 0, "NAICS21": 0}
        }
    
    # Get state FIPS from first block group
    state_fips = block_group_data[0].get("geography", {}).get("state", "")
    
    if not state_fips:
        logger.warning("No state FIPS found in block group data")
        return {
            "total_jobs": 0,
            "earnings_bands": {"E1": 0, "E2": 0, "E3": 0},
            "by_sector": {"NAICS11": 0, "NAICS21": 0}
        }
    
    # Create LODES client
    client = LODESClient()
    
    # Get block GEOIDs (simplified - would need spatial intersection)
    block_geoids = []  # client.get_block_geoids_for_polygon(grid_cell_polygon, state_fips)
    
    # Get LODES data
    try:
        lodes_data = await client.get_lodes_data(state_fips, lodes_year, block_geoids)
        return lodes_data.get("metrics", {
            "total_jobs": 0,
            "earnings_bands": {"E1": 0, "E2": 0, "E3": 0},
            "by_sector": {"NAICS11": 0, "NAICS21": 0}
        })
    except Exception as e:
        logger.error(f"Error getting LODES data: {e}")
        return {
            "total_jobs": 0,
            "earnings_bands": {"E1": 0, "E2": 0, "E3": 0},
            "by_sector": {"NAICS11": 0, "NAICS21": 0}
        }

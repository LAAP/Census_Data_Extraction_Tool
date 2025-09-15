"""Census Data API client with batching and variable mapping."""

import logging
import os
from typing import List, Dict, Any, Optional, Tuple
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

CENSUS_API_BASE = "https://api.census.gov/data"


class CensusAPIError(Exception):
    """Raised when Census API call fails."""
    pass


# ACS Variable definitions
ACS_VARIABLES = {
    # Population & households
    "B01003_001E": "total_population",
    "B11001_001E": "total_households",
    
    # Age distribution (from B01001)
    "B01001_003E": "male_0_4",
    "B01001_004E": "male_5_9", 
    "B01001_005E": "male_10_14",
    "B01001_006E": "male_15_17",
    "B01001_007E": "male_18_19",
    "B01001_008E": "male_20",
    "B01001_009E": "male_21",
    "B01001_010E": "male_22_24",
    "B01001_011E": "male_25_29",
    "B01001_012E": "male_30_34",
    "B01001_013E": "male_35_39",
    "B01001_014E": "male_40_44",
    "B01001_015E": "male_45_49",
    "B01001_016E": "male_50_54",
    "B01001_017E": "male_55_59",
    "B01001_018E": "male_60_61",
    "B01001_019E": "male_62_64",
    "B01001_020E": "male_65_66",
    "B01001_021E": "male_67_69",
    "B01001_022E": "male_70_74",
    "B01001_023E": "male_75_79",
    "B01001_024E": "male_80_84",
    "B01001_025E": "male_85_plus",
    "B01001_027E": "female_0_4",
    "B01001_028E": "female_5_9",
    "B01001_029E": "female_10_14",
    "B01001_030E": "female_15_17",
    "B01001_031E": "female_18_19",
    "B01001_032E": "female_20",
    "B01001_033E": "female_21",
    "B01001_034E": "female_22_24",
    "B01001_035E": "female_25_29",
    "B01001_036E": "female_30_34",
    "B01001_037E": "female_35_39",
    "B01001_038E": "female_40_44",
    "B01001_039E": "female_45_49",
    "B01001_040E": "female_50_54",
    "B01001_041E": "female_55_59",
    "B01001_042E": "female_60_61",
    "B01001_043E": "female_62_64",
    "B01001_044E": "female_65_66",
    "B01001_045E": "female_67_69",
    "B01001_046E": "female_70_74",
    "B01001_047E": "female_75_79",
    "B01001_048E": "female_80_84",
    "B01001_049E": "female_85_plus",
    
    # Income
    "B19013_001E": "median_household_income",
    "B19025_001E": "aggregate_household_income",
    
    # Income brackets
    "B19001_002E": "income_lt_10k",
    "B19001_003E": "income_10_15k",
    "B19001_004E": "income_15_20k",
    "B19001_005E": "income_20_25k",
    "B19001_006E": "income_25_30k",
    "B19001_007E": "income_30_35k",
    "B19001_008E": "income_35_40k",
    "B19001_009E": "income_40_45k",
    "B19001_010E": "income_45_50k",
    "B19001_011E": "income_50_60k",
    "B19001_012E": "income_60_75k",
    "B19001_013E": "income_75_100k",
    "B19001_014E": "income_100_125k",
    "B19001_015E": "income_125_150k",
    "B19001_016E": "income_150_200k",
    "B19001_017E": "income_200k_plus",
    
    # Employment
    "B23025_001E": "civilian_labor_force",
    "B23025_002E": "employed",
    "B23025_003E": "unemployed",
    "B23025_004E": "armed_forces",
    "B23025_005E": "not_in_labor_force",
    
    # Education (subset of B15003)
    "B15003_022E": "bachelors_degree",
    "B15003_023E": "masters_degree",
    "B15003_024E": "professional_degree",
    "B15003_025E": "doctorate_degree",
    "B15003_001E": "total_education_population",
    
    # Housing
    "B25002_001E": "total_housing_units",
    "B25002_002E": "occupied_housing_units",
    "B25002_003E": "vacant_housing_units",
    "B25003_001E": "total_tenure_units",
    "B25003_002E": "owner_occupied",
    "B25003_003E": "renter_occupied",
    # Note: B25010_* variables (housing structure) are not available in 2023 ACS
    # "B25010_001E": "total_units_in_structure",
    # "B25010_002E": "units_1_detached", 
    # "B25010_003E": "units_1_attached",
    # "B25010_004E": "units_2",
    # "B25010_005E": "units_3_4",
    # "B25010_006E": "units_5_9",
    # "B25010_007E": "units_10_19",
    # "B25010_008E": "units_20_49",
    # "B25010_009E": "units_50_plus",
    
    # Costs & values
    "B25064_001E": "median_gross_rent",
    "B25077_001E": "median_home_value",
}

# Income brackets from B19001
INCOME_BRACKET_VARIABLES = {
    "B19001_001E": "total_households_income",
    "B19001_002E": "income_lt_10k",
    "B19001_003E": "income_10_15k",
    "B19001_004E": "income_15_20k",
    "B19001_005E": "income_20_25k",
    "B19001_006E": "income_25_30k",
    "B19001_007E": "income_30_35k",
    "B19001_008E": "income_35_40k",
    "B19001_009E": "income_40_45k",
    "B19001_010E": "income_45_50k",
    "B19001_011E": "income_50_60k",
    "B19001_012E": "income_60_75k",
    "B19001_013E": "income_75_100k",
    "B19001_014E": "income_100_125k",
    "B19001_015E": "income_125_150k",
    "B19001_016E": "income_150_200k",
    "B19001_017E": "income_200k_plus",
}

# Rent burden from B25070
RENT_BURDEN_VARIABLES = {
    "B25070_001E": "total_rent_burden",
    "B25070_007E": "rent_burden_30_35",
    "B25070_008E": "rent_burden_35_40",
    "B25070_009E": "rent_burden_40_50",
    "B25070_010E": "rent_burden_50_plus",
}


class CensusAPIClient:
    """Client for Census Data API with batching support."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("CENSUS_API_KEY")
        if not self.api_key:
            logger.warning("No Census API key provided. Some requests may be rate limited.")
    
    def _build_url(self, year: int, dataset: str = "acs/acs5") -> str:
        """Build base URL for Census API."""
        return f"{CENSUS_API_BASE}/{year}/{dataset}"
    
    def _build_params(self, variables: List[str], geography: Dict[str, str]) -> Dict[str, str]:
        """Build query parameters for Census API."""
        params = {
            "get": ",".join(variables),
            "for": f"block group:{geography['block_group']}",
            "in": f"state:{geography['state']}+county:{geography['county']}+tract:{geography['tract']}"
        }
        
        if self.api_key:
            params["key"] = self.api_key
        
        return params
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def _make_request(self, url: str, params: Dict[str, str]) -> Dict[str, Any]:
        """Make a single Census API request."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if isinstance(data, list) and len(data) > 0:
                    # Success - return the data
                    return {"data": data}
                else:
                    logger.warning(f"Empty response from Census API: {data}")
                    raise CensusAPIError("Empty or invalid response from Census API")
                    
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limited by Census API")
                raise CensusAPIError("Rate limited by Census API")
            else:
                logger.error(f"HTTP error during Census API call: {e}")
                logger.error(f"URL: {url}")
                logger.error(f"Params: {params}")
                logger.error(f"Response: {e.response.text}")
                raise CensusAPIError(f"HTTP error during Census API call: {e}")
        except httpx.RequestError as e:
            logger.error(f"Request error during Census API call: {e}")
            raise CensusAPIError(f"Request error during Census API call: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during Census API call: {e}")
            raise CensusAPIError(f"Unexpected error during Census API call: {e}")
    
    def _chunk_variables(self, variables: List[str], chunk_size: int = 50) -> List[List[str]]:
        """Split variables into chunks for batching."""
        return [variables[i:i + chunk_size] for i in range(0, len(variables), chunk_size)]
    
    async def get_acs_data(
        self,
        year: int,
        geographies: List[Dict[str, str]],
        variables: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get ACS data for multiple geographies and variables.
        
        Args:
            year: ACS year
            geographies: List of geography dictionaries
            variables: List of variable names
            
        Returns:
            Dictionary mapping GEOID to variable values
        """
        url = self._build_url(year)
        results = {}
        
        # Group geographies by state/county for efficient API calls
        grouped_geos = {}
        for geo in geographies:
            key = (geo["state"], geo["county"])
            if key not in grouped_geos:
                grouped_geos[key] = []
            grouped_geos[key].append(geo)
        
        # Process each state/county group
        for (state, county), geo_list in grouped_geos.items():
            # Chunk variables to stay under API limits
            variable_chunks = self._chunk_variables(variables)
            
            for chunk in variable_chunks:
                # Build tract-level query for all block groups in this state/county
                tracts = list(set(geo["tract"] for geo in geo_list))
                
                for tract in tracts:
                    tract_geos = [geo for geo in geo_list if geo["tract"] == tract]
                    
                    # Get all block groups for this tract
                    block_groups = [geo["block_group"] for geo in tract_geos]
                    
                    params = {
                        "get": ",".join(chunk),
                        "for": f"block group:{','.join(block_groups)}",
                        "in": f"state:{state}+county:{county}+tract:{tract}"
                    }
                    
                    if self.api_key:
                        params["key"] = self.api_key
                    
                    try:
                        response_data = await self._make_request(url, params)
                        data = response_data["data"]
                        
                        # Parse response
                        if len(data) > 1:  # Has header row
                            headers = data[0]
                            rows = data[1:]
                            
                            for row in rows:
                                row_dict = dict(zip(headers, row))
                                
                                # Extract GEOID and other fields
                                geoid = f"{row_dict['state']}{row_dict['county']}{row_dict['tract']}{row_dict['block group']}"
                                
                                if geoid not in results:
                                    results[geoid] = {}
                                
                                # Store variable values
                                for var in chunk:
                                    if var in row_dict:
                                        try:
                                            value = int(row_dict[var]) if row_dict[var] else 0
                                            results[geoid][var] = value
                                        except (ValueError, TypeError):
                                            results[geoid][var] = 0
                        
                    except CensusAPIError as e:
                        logger.error(f"Error fetching data for tract {tract}: {e}")
                        continue
        
        return results
    
    async def get_income_brackets(
        self,
        year: int,
        geographies: List[Dict[str, str]]
    ) -> Dict[str, Dict[str, int]]:
        """Get income bracket data using group query."""
        url = self._build_url(year)
        results = {}
        
        # Group by state/county
        grouped_geos = {}
        for geo in geographies:
            key = (geo["state"], geo["county"])
            if key not in grouped_geos:
                grouped_geos[key] = []
            grouped_geos[key].append(geo)
        
        for (state, county), geo_list in grouped_geos.items():
            tracts = list(set(geo["tract"] for geo in geo_list))
            
            for tract in tracts:
                tract_geos = [geo for geo in geo_list if geo["tract"] == tract]
                block_groups = [geo["block_group"] for geo in tract_geos]
                
                params = {
                    "get": "group(B19001)",
                    "for": f"block group:{','.join(block_groups)}",
                    "in": f"state:{state}+county:{county}+tract:{tract}"
                }
                
                if self.api_key:
                    params["key"] = self.api_key
                
                try:
                    response_data = await self._make_request(url, params)
                    data = response_data["data"]
                    
                    if len(data) > 1:
                        headers = data[0]
                        rows = data[1:]
                        
                        for row in rows:
                            row_dict = dict(zip(headers, row))
                            geoid = f"{row_dict['state']}{row_dict['county']}{row_dict['tract']}{row_dict['block group']}"
                            
                            if geoid not in results:
                                results[geoid] = {}
                            
                            # Map B19001 variables
                            for var, key in INCOME_BRACKET_VARIABLES.items():
                                if var in row_dict:
                                    try:
                                        value = int(row_dict[var]) if row_dict[var] else 0
                                        results[geoid][key] = value
                                    except (ValueError, TypeError):
                                        results[geoid][key] = 0
                
                except CensusAPIError as e:
                    logger.error(f"Error fetching income brackets for tract {tract}: {e}")
                    continue
        
        return results

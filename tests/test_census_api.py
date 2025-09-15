"""Tests for Census API client."""

import pytest
from unittest.mock import AsyncMock, patch
from app.census_api import CensusAPIClient, ACS_VARIABLES, INCOME_BRACKET_VARIABLES


class TestCensusAPIClient:
    """Test Census API client functionality."""
    
    def test_init_with_api_key(self):
        """Test client initialization with API key."""
        client = CensusAPIClient("test_key")
        assert client.api_key == "test_key"
    
    def test_init_without_api_key(self):
        """Test client initialization without API key."""
        with patch.dict("os.environ", {}, clear=True):
            client = CensusAPIClient()
            assert client.api_key is None
    
    def test_init_with_env_key(self):
        """Test client initialization with environment API key."""
        with patch.dict("os.environ", {"CENSUS_API_KEY": "env_key"}):
            client = CensusAPIClient()
            assert client.api_key == "env_key"
    
    def test_build_url(self):
        """Test URL building."""
        client = CensusAPIClient()
        url = client._build_url(2023, "acs/acs5")
        assert url == "https://api.census.gov/data/2023/acs/acs5"
    
    def test_build_params(self):
        """Test parameter building."""
        client = CensusAPIClient("test_key")
        variables = ["B01003_001E", "B11001_001E"]
        geography = {
            "state": "25",
            "county": "017",
            "tract": "000100",
            "block_group": "1"
        }
        
        params = client._build_params(variables, geography)
        
        assert params["get"] == "B01003_001E,B11001_001E"
        assert params["for"] == "block group:1"
        assert params["in"] == "state:25+county:017+tract:000100"
        assert params["key"] == "test_key"
    
    def test_chunk_variables(self):
        """Test variable chunking."""
        client = CensusAPIClient()
        variables = [f"B01001_{i:03d}E" for i in range(1, 101)]  # 100 variables
        
        chunks = client._chunk_variables(variables, chunk_size=50)
        
        assert len(chunks) == 2
        assert len(chunks[0]) == 50
        assert len(chunks[1]) == 50
        assert chunks[0] + chunks[1] == variables
    
    def test_chunk_variables_small(self):
        """Test variable chunking with small list."""
        client = CensusAPIClient()
        variables = ["B01003_001E", "B11001_001E"]
        
        chunks = client._chunk_variables(variables, chunk_size=50)
        
        assert len(chunks) == 1
        assert chunks[0] == variables


class TestACSVariables:
    """Test ACS variable definitions."""
    
    def test_acs_variables_defined(self):
        """Test that ACS variables are properly defined."""
        assert "B01003_001E" in ACS_VARIABLES
        assert "B11001_001E" in ACS_VARIABLES
        assert "B19013_001E" in ACS_VARIABLES
        assert ACS_VARIABLES["B01003_001E"] == "total_population"
        assert ACS_VARIABLES["B11001_001E"] == "total_households"
    
    def test_income_bracket_variables_defined(self):
        """Test that income bracket variables are properly defined."""
        assert "B19001_001E" in INCOME_BRACKET_VARIABLES
        assert "B19001_002E" in INCOME_BRACKET_VARIABLES
        assert "B19001_017E" in INCOME_BRACKET_VARIABLES
        assert INCOME_BRACKET_VARIABLES["B19001_001E"] == "total_households_income"
        assert INCOME_BRACKET_VARIABLES["B19001_002E"] == "income_lt_10k"


@pytest.mark.asyncio
class TestCensusAPIIntegration:
    """Test Census API integration (mocked)."""
    
    async def test_make_request_success(self):
        """Test successful API request."""
        client = CensusAPIClient("test_key")
        
        mock_response_data = {
            "data": [
                ["B01003_001E", "state", "county", "tract", "block group"],
                ["1000", "25", "017", "000100", "1"]
            ]
        }
        
        with patch("httpx.AsyncClient") as mock_client:
            mock_response = AsyncMock()
            mock_response.json.return_value = mock_response_data["data"]
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await client._make_request("https://api.census.gov/data/2023/acs/acs5", {})
            
            assert "data" in result
            assert len(result["data"]) == 2
    
    async def test_make_request_http_error(self):
        """Test API request with HTTP error."""
        client = CensusAPIClient("test_key")
        
        with patch("httpx.AsyncClient") as mock_client:
            mock_response = AsyncMock()
            mock_response.raise_for_status.side_effect = Exception("HTTP 500")
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            with pytest.raises(Exception):
                await client._make_request("https://api.census.gov/data/2023/acs/acs5", {})

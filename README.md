# Census Grid Stats API

A production-ready FastAPI service that returns ACS + LODES statistics for 1×1 km grid cells around user-supplied addresses or coordinates.

## Features

- **Geocoding**: Uses Census Geocoder to convert addresses to coordinates
- **Spatial Analysis**: Creates accurate 1km grid cells using UTM projection
- **Census Data**: Fetches ACS 5-year estimates with area-weighted aggregation
- **LODES Integration**: Optional workplace employment data from LODES
- **Robust Error Handling**: Comprehensive error handling and logging
- **Production Ready**: Includes retry logic, caching, and monitoring

## Quick Start

### Installation

```bash
# Install dependencies
make install

# Install development dependencies
make dev-install
```

### Running the Service

```bash
# Start the development server
make run

# Or run directly
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000`

### API Documentation

- Interactive docs: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## API Usage

### Basic Request

```bash
curl -X POST "http://localhost:8000/grid_stats" \
  -H "Content-Type: application/json" \
  -d '{
    "address": "1600 Pennsylvania Avenue NW, Washington, DC",
    "cell_km": 1.0,
    "acs_year": 2023,
    "include_lodes": false
  }'
```

### Using Coordinates

```bash
curl -X POST "http://localhost:8000/grid_stats" \
  -H "Content-Type: application/json" \
  -d '{
    "lat": 42.3601,
    "lon": -71.0589,
    "cell_km": 2.0,
    "acs_year": 2023,
    "include_lodes": true
  }'
```

### Response Format

```json
{
  "input": {
    "address": "1600 Pennsylvania Avenue NW, Washington, DC",
    "cell_km": 1.0,
    "acs_year": 2023,
    "include_lodes": false
  },
  "area": {
    "type": "Polygon",
    "area_km2": 1.0,
    "crs": "EPSG:4326"
  },
  "sources": {
    "acs_dataset": "acs/acs5",
    "acs_year": 2023,
    "tigerweb_layer": "tigerWMS_Current/MapServer/10",
    "geocoder_benchmark": "Public_AR_Current",
    "lodes": {
      "enabled": false,
      "year": 2022
    }
  },
  "estimation": {
    "method": "areal_weighting",
    "notes": "Statistics estimated using area-weighted interpolation from Census Block Groups"
  },
  "metrics": {
    "population": {
      "total": 1500,
      "age": {
        "0_4": 120,
        "5_14": 200,
        "15_24": 300,
        "25_44": 400,
        "45_64": 350,
        "65p": 130
      }
    },
    "households": {
      "total": 600,
      "avg_size": {
        "overall": 2.5,
        "owner": 2.8,
        "renter": 2.1
      }
    },
    "income": {
      "median": 75000,
      "mean": 82000,
      "brackets": {
        "lt_10k": 50,
        "10_15k": 30,
        "15_25k": 80,
        "25_35k": 100,
        "35_50k": 120,
        "50_75k": 150,
        "75_100k": 100,
        "100_125k": 80,
        "125_150k": 60,
        "150_200k": 40,
        "200k_plus": 20
      }
    },
    "employment": {
      "labor_force": 900,
      "employed": 850,
      "unemployed": 50
    },
    "education": {
      "hs_or_less": 200,
      "some_college": 300,
      "ba_plus": 400
    },
    "housing": {
      "units_total": 650,
      "occupied": 600,
      "vacant": 50,
      "tenure": {
        "owner": 400,
        "renter": 200
      },
      "units_in_structure": {
        "1_det": 300,
        "2_units": 100,
        "3_4": 150,
        "5_9": 80,
        "10_19": 20,
        "20p": 0
      }
    },
    "costs": {
      "median_gross_rent": 1800,
      "rent_burden_pct": {
        "gt_30": 45.0,
        "gt_50": 20.0
      },
      "median_home_value": 450000
    },
    "jobs_workplace": {
      "total_jobs": 0,
      "earnings_bands": {
        "E1": 0,
        "E2": 0,
        "E3": 0
      },
      "by_sector": {
        "NAICS11": 0,
        "NAICS21": 0
      }
    }
  },
  "moe": true
}
```

## Configuration

### Environment Variables

- `CENSUS_API_KEY`: Census Data API key (optional but recommended)

### Request Parameters

- `address` (optional): Address to geocode
- `lat` (optional): Latitude in decimal degrees
- `lon` (optional): Longitude in decimal degrees
- `cell_km` (default: 1.0): Grid cell size in kilometers (0.1 to 10.0)
- `acs_year` (default: 2023): ACS data year (2010 to 2023)
- `include_lodes` (default: false): Include LODES workplace data
- `lodes_year` (optional): LODES data year (defaults to latest available)

## Development

### Running Tests

```bash
# Run all tests
make test

# Run specific test file
pytest tests/test_geometry.py -v
```

### Code Formatting

```bash
# Format code
make format
```

### Project Structure

```
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application
│   ├── schemas.py           # Pydantic models
│   ├── geocode.py           # Census Geocoder integration
│   ├── geometry.py          # UTM projection and grid creation
│   ├── tigerweb.py          # TIGERweb integration
│   ├── census_api.py        # Census Data API client
│   ├── lodes.py             # LODES data integration
│   └── aggregation.py       # Area-weighted aggregation
├── tests/
│   ├── test_geometry.py
│   ├── test_aggregation.py
│   └── test_census_api.py
├── main.py                  # Entry point
├── pyproject.toml          # Project configuration
├── Makefile                # Build commands
└── README.md
```

## Data Sources

### ACS (American Community Survey)
- **Dataset**: 5-year estimates
- **Geography**: Census Block Groups
- **Variables**: Population, households, income, employment, education, housing
- **Method**: Area-weighted interpolation

### LODES (LEHD Origin-Destination Employment Statistics)
- **Dataset**: Workplace Area Characteristics (WAC)
- **Geography**: Census Blocks
- **Variables**: Jobs by earnings bands and NAICS sectors
- **Method**: Spatial intersection with grid cell

### TIGERweb
- **Service**: Census Block Groups layer
- **Purpose**: Spatial intersection with grid cell
- **Method**: Esri REST API with spatial queries

## Technical Notes

### Areal Weighting vs Population Weighting

This API uses **areal weighting** for interpolation, which:
- Weights each block group by the area of intersection with the grid cell
- Is appropriate for area-based statistics (housing, land use)
- May not be optimal for population-based statistics

For population-weighted interpolation, you would need to:
1. Obtain population density data for each block group
2. Weight by population density × intersection area
3. This is more complex but potentially more accurate for demographic statistics

### ACS Data Vintage

- **2023**: Most recent 5-year estimates (2019-2023)
- **2022**: 2018-2022 estimates
- **2021**: 2017-2021 estimates
- **2020**: 2016-2020 estimates (includes 2020 Census)

### Error Handling

The API includes comprehensive error handling for:
- Invalid addresses or coordinates
- Network timeouts and retries
- Census API rate limits
- Empty spatial intersections
- Data parsing errors

### Performance Considerations

- Census API calls are batched (≤50 variables per request)
- LODES data is cached locally
- TIGERweb queries use spatial indexing
- UTM projection ensures accurate area calculations

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run the test suite
6. Submit a pull request

## Support

For issues and questions:
1. Check the API documentation at `/docs`
2. Review the test cases in `tests/`
3. Check the logs for detailed error messages
4. Open an issue on GitHub

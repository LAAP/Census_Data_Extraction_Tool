"""Pydantic schemas for request/response models."""

from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, field_validator, model_validator
from geojson_pydantic import Polygon


class GridStatsRequest(BaseModel):
    """Request schema for /grid_stats endpoint."""
    
    # Either address OR lat/lon must be provided
    address: Optional[str] = Field(None, description="Address to geocode")
    lat: Optional[float] = Field(None, ge=-90, le=90, description="Latitude")
    lon: Optional[float] = Field(None, ge=-180, le=180, description="Longitude")
    
    # Optional parameters
    cell_km: float = Field(1.0, ge=0.1, le=10.0, description="Grid cell size in kilometers")
    acs_year: int = Field(2023, ge=2010, le=2023, description="ACS data year")
    include_lodes: bool = Field(False, description="Include LODES workplace data")
    lodes_year: Optional[int] = Field(None, ge=2015, le=2023, description="LODES data year (defaults to latest)")
    
    @field_validator('lat', 'lon')
    @classmethod
    def validate_coordinates(cls, v, info):
        """Ensure either address OR both lat/lon are provided."""
        # This validator runs for each field individually
        return v
    
    @field_validator('lat')
    @classmethod
    def validate_lat_when_lon_provided(cls, v, info):
        """Ensure lat is provided when lon is provided."""
        if info.data.get('lon') is not None and v is None:
            raise ValueError("lat must be provided when lon is provided")
        return v
    
    @field_validator('lon')
    @classmethod
    def validate_lon_when_lat_provided(cls, v, info):
        """Ensure lon is provided when lat is provided."""
        if info.data.get('lat') is not None and v is None:
            raise ValueError("lon must be provided when lat is provided")
        return v
    
    @model_validator(mode='after')
    def validate_coordinates_or_address(self):
        """Ensure either address OR both lat/lon are provided."""
        if self.address is not None:
            return self
        if self.lat is not None and self.lon is not None:
            return self
        raise ValueError("Either address or both lat/lon must be provided")


class InputInfo(BaseModel):
    """Input information for response."""
    address: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    cell_km: float
    acs_year: int
    include_lodes: bool
    lodes_year: Optional[int] = None


class AreaInfo(BaseModel):
    """Area information for response."""
    type: str = "Polygon"
    area_km2: float
    crs: str = "EPSG:4326"
    intersecting_block_groups: Optional[Dict[str, Any]] = None


class SourcesInfo(BaseModel):
    """Data sources information."""
    acs_dataset: str = "acs/acs5"
    acs_year: int
    tigerweb_layer: str = "tigerWMS_Current/MapServer/10"
    geocoder_benchmark: str = "Public_AR_Current"
    lodes: Dict[str, Any]


class EstimationInfo(BaseModel):
    """Estimation method information."""
    method: str = "areal_weighting"
    notes: str
    land_use_context: Optional[str] = None


class AgeDistribution(BaseModel):
    """Age distribution metrics."""
    age_0_4: int = Field(alias="0_4")
    age_5_14: int = Field(alias="5_14")
    age_15_24: int = Field(alias="15_24")
    age_25_44: int = Field(alias="25_44")
    age_45_64: int = Field(alias="45_64")
    age_65p: int = Field(alias="65p")


class PopulationMetrics(BaseModel):
    """Population metrics."""
    total: int
    density_per_km2: float
    coverage_ratio: float
    age: AgeDistribution


class HouseholdSizeMetrics(BaseModel):
    """Household size metrics."""
    overall: float
    owner: float
    renter: float


class HouseholdMetrics(BaseModel):
    """Household metrics."""
    total: int
    avg_size: HouseholdSizeMetrics


class IncomeBrackets(BaseModel):
    """Income bracket distribution."""
    lt_10k: int
    income_10_15k: int
    income_15_20k: int
    income_20_25k: int
    income_25_30k: int
    income_30_35k: int
    income_35_40k: int
    income_40_45k: int
    income_45_50k: int
    income_50_60k: int
    income_60_75k: int
    income_75_100k: int
    income_100_125k: int
    income_125_150k: int
    income_150_200k: int
    income_200k_plus: int


class IncomeMetrics(BaseModel):
    """Income metrics."""
    median: int
    mean: int
    brackets: IncomeBrackets


class EmploymentMetrics(BaseModel):
    """Employment metrics."""
    labor_force: int
    employed: int
    employment_rate: float


class EducationMetrics(BaseModel):
    """Education metrics."""
    hs_or_less: int
    some_college: int
    ba_plus: int


class TenureMetrics(BaseModel):
    """Housing tenure metrics."""
    owner: int
    renter: int


class UnitsInStructure(BaseModel):
    """Units in structure metrics."""
    units_1_det: int = Field(alias="1_det")
    units_2: int = Field(alias="2_units")
    units_3_4: int = Field(alias="3_4")
    units_5_9: int = Field(alias="5_9")
    units_10_19: int = Field(alias="10_19")
    units_20p: int = Field(alias="20p")


class HousingMetrics(BaseModel):
    """Housing metrics."""
    units_total: int
    occupied: int
    vacant: int
    tenure: TenureMetrics
    units_in_structure: UnitsInStructure


class RentBurdenPct(BaseModel):
    """Rent burden percentages."""
    gt_30: float
    gt_50: float


class CostsMetrics(BaseModel):
    """Costs and values metrics."""
    median_gross_rent: int
    rent_burden_pct: RentBurdenPct
    median_home_value: int


class EarningsBands(BaseModel):
    """Earnings bands for jobs."""
    E1: int
    E2: int
    E3: int


class JobsBySector(BaseModel):
    """Jobs by NAICS sector."""
    NAICS11: int
    NAICS21: int


class JobsWorkplaceMetrics(BaseModel):
    """Jobs and workplace metrics."""
    total_jobs: int
    earnings_bands: EarningsBands
    by_sector: JobsBySector


class ValidationInfo(BaseModel):
    """Data validation information."""
    employment_mismatch: bool
    income_suspicious: bool
    validation_note: str


class Metrics(BaseModel):
    """All metrics combined."""
    population: PopulationMetrics
    households: HouseholdMetrics
    income: IncomeMetrics
    employment: EmploymentMetrics
    education: EducationMetrics
    housing: HousingMetrics
    costs: CostsMetrics
    jobs_workplace: JobsWorkplaceMetrics
    validation: ValidationInfo


class GridStatsResponse(BaseModel):
    """Response schema for /grid_stats endpoint."""
    input: InputInfo
    area: AreaInfo
    sources: SourcesInfo
    estimation: EstimationInfo
    metrics: Metrics
    moe: bool = True


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = "healthy"
    timestamp: str


class VersionResponse(BaseModel):
    """Version information response."""
    version: str
    api_version: str = "v1"

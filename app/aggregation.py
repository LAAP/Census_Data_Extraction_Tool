"""Area-weighted aggregation and median calculation utilities."""

import logging
from typing import Dict, Any, List, Tuple
import numpy as np

logger = logging.getLogger(__name__)


def area_weighted_sum(
    block_group_data: List[Dict[str, Any]],
    variable: str
) -> float:
    """
    Calculate area-weighted sum for a variable across block groups.
    
    Args:
        block_group_data: List of block group data dictionaries
        variable: Variable name to sum
        
    Returns:
        Area-weighted sum
    """
    total = 0.0
    
    for bg_data in block_group_data:
        value = bg_data.get("data", {}).get(variable, 0)
        weight = bg_data.get("area_weight", 0.0)
        total += value * weight
    
    return total


def area_weighted_median(
    block_group_data: List[Dict[str, Any]],
    variable: str
) -> float:
    """
    Calculate area-weighted median for a variable across block groups.
    
    Args:
        block_group_data: List of block group data dictionaries
        variable: Variable name to calculate median for
        
    Returns:
        Area-weighted median
    """
    # For simple median calculation, we'll use the area-weighted average
    # A more sophisticated approach would use the distribution tables
    total_weight = sum(bg_data.get("area_weight", 0.0) for bg_data in block_group_data)
    
    if total_weight == 0:
        return 0.0
    
    weighted_sum = 0.0
    for bg_data in block_group_data:
        value = bg_data.get("data", {}).get(variable, 0)
        weight = bg_data.get("area_weight", 0.0)
        weighted_sum += value * weight
    
    return weighted_sum / total_weight


def calculate_age_distribution(block_group_data: List[Dict[str, Any]]) -> Dict[str, int]:
    """Calculate age distribution from B01001 variables."""
    age_groups = {
        "0_4": 0,
        "5_14": 0,
        "15_24": 0,
        "25_44": 0,
        "45_64": 0,
        "65p": 0
    }
    
    # Age group mappings from B01001 variables
    age_mappings = {
        "0_4": ["male_0_4", "female_0_4"],
        "5_14": ["male_5_9", "male_10_14", "female_5_9", "female_10_14"],
        "15_24": ["male_15_17", "male_18_19", "male_20", "male_21", "male_22_24",
                  "female_15_17", "female_18_19", "female_20", "female_21", "female_22_24"],
        "25_44": ["male_25_29", "male_30_34", "male_35_39", "male_40_44",
                  "female_25_29", "female_30_34", "female_35_39", "female_40_44"],
        "45_64": ["male_45_49", "male_50_54", "male_55_59", "male_60_61", "male_62_64",
                  "female_45_49", "female_50_54", "female_55_59", "female_60_61", "female_62_64"],
        "65p": ["male_65_66", "male_67_69", "male_70_74", "male_75_79", "male_80_84", "male_85_plus",
                "female_65_66", "female_67_69", "female_70_74", "female_75_79", "female_80_84", "female_85_plus"]
    }
    
    for age_group, variables in age_mappings.items():
        for bg_data in block_group_data:
            data = bg_data.get("data", {})
            weight = bg_data.get("area_weight", 0.0)
            
            group_total = 0
            for var in variables:
                group_total += data.get(var, 0)
            
            age_groups[age_group] += int(group_total * weight)
    
    return age_groups


def calculate_income_brackets(block_group_data: List[Dict[str, Any]]) -> Dict[str, int]:
    """Calculate income bracket distribution."""
    brackets = {
        "lt_10k": 0,
        "10_15k": 0,
        "15_25k": 0,
        "25_35k": 0,
        "35_50k": 0,
        "50_75k": 0,
        "75_100k": 0,
        "100_125k": 0,
        "125_150k": 0,
        "150_200k": 0,
        "200k_plus": 0
    }
    
    # Income bracket mappings
    bracket_mappings = {
        "lt_10k": ["income_lt_10k"],
        "10_15k": ["income_10_15k"],
        "15_25k": ["income_15_20k", "income_20_25k"],
        "25_35k": ["income_25_30k", "income_30_35k"],
        "35_50k": ["income_35_40k", "income_40_45k", "income_45_50k"],
        "50_75k": ["income_50_60k", "income_60_75k"],
        "75_100k": ["income_75_100k"],
        "100_125k": ["income_100_125k"],
        "125_150k": ["income_125_150k"],
        "150_200k": ["income_150_200k"],
        "200k_plus": ["income_200k_plus"]
    }
    
    for bracket, variables in bracket_mappings.items():
        for bg_data in block_group_data:
            data = bg_data.get("data", {})
            weight = bg_data.get("area_weight", 0.0)
            
            bracket_total = 0
            for var in variables:
                bracket_total += data.get(var, 0)
            
            brackets[bracket] += int(bracket_total * weight)
    
    return brackets


def calculate_education_levels(block_group_data: List[Dict[str, Any]]) -> Dict[str, int]:
    """Calculate education level distribution."""
    education = {
        "hs_or_less": 0,
        "some_college": 0,
        "ba_plus": 0
    }
    
    # This is a simplified calculation
    # In practice, you'd need more B15003 variables for accurate education levels
    for bg_data in block_group_data:
        data = bg_data.get("data", {})
        weight = bg_data.get("area_weight", 0.0)
        
        # Use available education variables
        ba_plus = data.get("bachelors_degree", 0) + data.get("masters_degree", 0) + \
                 data.get("professional_degree", 0) + data.get("doctorate_degree", 0)
        
        total_education = data.get("total_education_population", 0)
        
        if total_education > 0:
            ba_plus_pct = ba_plus / total_education
            # Rough approximation for other levels
            some_college_pct = min(0.3, 1.0 - ba_plus_pct)  # Assume 30% some college
            hs_or_less_pct = 1.0 - ba_plus_pct - some_college_pct
            
            education["ba_plus"] += int(total_education * ba_plus_pct * weight)
            education["some_college"] += int(total_education * some_college_pct * weight)
            education["hs_or_less"] += int(total_education * hs_or_less_pct * weight)
    
    return education


def calculate_units_in_structure(block_group_data: List[Dict[str, Any]]) -> Dict[str, int]:
    """Calculate units in structure distribution."""
    units = {
        "1_det": 0,
        "2_units": 0,
        "3_4": 0,
        "5_9": 0,
        "10_19": 0,
        "20p": 0
    }
    
    unit_mappings = {
        "1_det": ["units_1_detached"],
        "2_units": ["units_2"],
        "3_4": ["units_3_4"],
        "5_9": ["units_5_9"],
        "10_19": ["units_10_19"],
        "20p": ["units_20_49", "units_50_plus"]
    }
    
    for unit_type, variables in unit_mappings.items():
        for bg_data in block_group_data:
            data = bg_data.get("data", {})
            weight = bg_data.get("area_weight", 0.0)
            
            unit_total = 0
            for var in variables:
                unit_total += data.get(var, 0)
            
            units[unit_type] += int(unit_total * weight)
    
    return units


def calculate_rent_burden(block_group_data: List[Dict[str, Any]]) -> Dict[str, float]:
    """Calculate rent burden percentages."""
    total_renters = 0
    burden_30_plus = 0
    burden_50_plus = 0
    
    for bg_data in block_group_data:
        data = bg_data.get("data", {})
        weight = bg_data.get("area_weight", 0.0)
        
        # Use available rent burden variables
        total_rent_burden = data.get("total_rent_burden", 0)
        burden_30_35 = data.get("rent_burden_30_35", 0)
        burden_35_40 = data.get("rent_burden_35_40", 0)
        burden_40_50 = data.get("rent_burden_40_50", 0)
        burden_50_plus = data.get("rent_burden_50_plus", 0)
        
        if total_rent_burden > 0:
            total_renters += int(total_rent_burden * weight)
            burden_30_plus += int((burden_30_35 + burden_35_40 + burden_40_50 + burden_50_plus) * weight)
            burden_50_plus += int(burden_50_plus * weight)
    
    if total_renters == 0:
        return {"gt_30": 0.0, "gt_50": 0.0}
    
    return {
        "gt_30": (burden_30_plus / total_renters) * 100,
        "gt_50": (burden_50_plus / total_renters) * 100
    }


def quantile_from_brackets(
    brackets: Dict[str, int],
    bracket_ranges: Dict[str, Tuple[float, float]],
    quantile: float = 0.5
) -> float:
    """
    Calculate quantile from bracket distribution.
    
    Args:
        brackets: Dictionary of bracket names to counts
        bracket_ranges: Dictionary of bracket names to (min, max) ranges
        quantile: Quantile to calculate (0.0 to 1.0)
        
    Returns:
        Quantile value
    """
    # Convert to sorted list
    bracket_list = []
    for name, count in brackets.items():
        if count > 0 and name in bracket_ranges:
            min_val, max_val = bracket_ranges[name]
            bracket_list.append((min_val, max_val, count))
    
    if not bracket_list:
        return 0.0
    
    # Sort by minimum value
    bracket_list.sort(key=lambda x: x[0])
    
    # Calculate cumulative distribution
    total_count = sum(count for _, _, count in bracket_list)
    if total_count == 0:
        return 0.0
    
    target_count = quantile * total_count
    cumulative_count = 0.0
    
    for min_val, max_val, count in bracket_list:
        if cumulative_count + count >= target_count:
            # Interpolate within this bracket
            remaining = target_count - cumulative_count
            if count == 0:
                return min_val
            
            # Linear interpolation
            bracket_fraction = remaining / count
            bracket_width = max_val - min_val
            return min_val + (bracket_fraction * bracket_width)
        
        cumulative_count += count
    
    # If we get here, return the maximum value
    return bracket_list[-1][1]


def calculate_income_median(block_group_data: List[Dict[str, Any]]) -> int:
    """Calculate median household income using bracket distribution."""
    brackets = calculate_income_brackets(block_group_data)
    
    # Income bracket ranges (in thousands)
    bracket_ranges = {
        "lt_10k": (0, 10),
        "10_15k": (10, 15),
        "15_25k": (15, 25),
        "25_35k": (25, 35),
        "35_50k": (35, 50),
        "50_75k": (50, 75),
        "75_100k": (75, 100),
        "100_125k": (100, 125),
        "125_150k": (125, 150),
        "150_200k": (150, 200),
        "200k_plus": (200, 300)  # Assume max of 300k for open-ended bracket
    }
    
    median = quantile_from_brackets(brackets, bracket_ranges, 0.5)
    return int(median * 1000)  # Convert back to dollars


def validate_employment_data(labor_force: float, employed: float, unemployed: float, total_population: float) -> Tuple[float, float, float]:
    """
    Validate and correct employment data to ensure logical consistency.
    
    Args:
        labor_force: Total labor force
        employed: Number of employed people
        unemployed: Number of unemployed people
        total_population: Total population for sanity check
        
    Returns:
        Tuple of (corrected_labor_force, corrected_employed, corrected_unemployed)
    """
    # Ensure labor_force = employed + unemployed
    if labor_force != employed + unemployed:
        logger.warning(f"Employment data inconsistency: labor_force ({labor_force}) != employed ({employed}) + unemployed ({unemployed})")
        # Correct by adjusting unemployed to maintain the relationship
        unemployed = max(0, labor_force - employed)
    
    # Ensure employment data doesn't exceed total population
    if labor_force > total_population:
        logger.warning(f"Labor force ({labor_force}) exceeds total population ({total_population})")
        # Scale down proportionally
        scale_factor = total_population / labor_force
        labor_force = int(labor_force * scale_factor)
        employed = int(employed * scale_factor)
        unemployed = int(unemployed * scale_factor)
    
    # Final validation
    if labor_force != employed + unemployed:
        # If still inconsistent, set unemployed to the difference
        unemployed = max(0, labor_force - employed)
    
    return labor_force, employed, unemployed


def aggregate_metrics(block_group_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate all metrics from block group data."""
    if not block_group_data:
        return _empty_metrics()
    
    # Basic counts
    total_population = area_weighted_sum(block_group_data, "total_population")
    total_households = area_weighted_sum(block_group_data, "total_households")
    
    # Age distribution
    age_dist = calculate_age_distribution(block_group_data)
    
    # Income metrics
    income_median = calculate_income_median(block_group_data)
    income_mean = area_weighted_median(block_group_data, "aggregate_household_income")
    income_brackets = calculate_income_brackets(block_group_data)
    
    # Employment
    labor_force = area_weighted_sum(block_group_data, "civilian_labor_force")
    employed = area_weighted_sum(block_group_data, "employed")
    unemployed = area_weighted_sum(block_group_data, "unemployed")
    
    # Validate employment data
    labor_force, employed, unemployed = validate_employment_data(
        labor_force, employed, unemployed, total_population
    )
    
    # Education
    education = calculate_education_levels(block_group_data)
    
    # Housing
    total_units = area_weighted_sum(block_group_data, "total_housing_units")
    occupied_units = area_weighted_sum(block_group_data, "occupied_housing_units")
    vacant_units = area_weighted_sum(block_group_data, "vacant_housing_units")
    owner_occupied = area_weighted_sum(block_group_data, "owner_occupied")
    renter_occupied = area_weighted_sum(block_group_data, "renter_occupied")
    
    # Units in structure
    units_in_structure = calculate_units_in_structure(block_group_data)
    
    # Costs
    median_rent = area_weighted_median(block_group_data, "median_gross_rent")
    median_home_value = area_weighted_median(block_group_data, "median_home_value")
    rent_burden = calculate_rent_burden(block_group_data)
    
    # Calculate household sizes
    avg_household_size = total_population / total_households if total_households > 0 else 0
    avg_owner_size = total_population / owner_occupied if owner_occupied > 0 else 0
    avg_renter_size = total_population / renter_occupied if renter_occupied > 0 else 0
    
    return {
        "population": {
            "total": int(total_population),
            "age": age_dist
        },
        "households": {
            "total": int(total_households),
            "avg_size": {
                "overall": round(avg_household_size, 2),
                "owner": round(avg_owner_size, 2),
                "renter": round(avg_renter_size, 2)
            }
        },
        "income": {
            "median": income_median,
            "mean": int(income_mean),
            "brackets": income_brackets
        },
        "employment": {
            "labor_force": int(labor_force),
            "employed": int(employed),
            "unemployed": int(unemployed)
        },
        "education": education,
        "housing": {
            "units_total": int(total_units),
            "occupied": int(occupied_units),
            "vacant": int(vacant_units),
            "tenure": {
                "owner": int(owner_occupied),
                "renter": int(renter_occupied)
            },
            "units_in_structure": units_in_structure
        },
        "costs": {
            "median_gross_rent": int(median_rent),
            "rent_burden_pct": rent_burden,
            "median_home_value": int(median_home_value)
        },
        "jobs_workplace": {
            "total_jobs": 0,  # Will be filled by LODES data
            "earnings_bands": {"E1": 0, "E2": 0, "E3": 0},
            "by_sector": {"NAICS11": 0, "NAICS21": 0}
        }
    }


def _empty_metrics() -> Dict[str, Any]:
    """Return empty metrics structure."""
    return {
        "population": {
            "total": 0,
            "age": {"0_4": 0, "5_14": 0, "15_24": 0, "25_44": 0, "45_64": 0, "65p": 0}
        },
        "households": {
            "total": 0,
            "avg_size": {"overall": 0.0, "owner": 0.0, "renter": 0.0}
        },
        "income": {
            "median": 0,
            "mean": 0,
            "brackets": {"lt_10k": 0, "10_15k": 0, "15_25k": 0, "25_35k": 0, "35_50k": 0,
                        "50_75k": 0, "75_100k": 0, "100_125k": 0, "125_150k": 0, "150_200k": 0, "200k_plus": 0}
        },
        "employment": {"labor_force": 0, "employed": 0, "unemployed": 0},
        "education": {"hs_or_less": 0, "some_college": 0, "ba_plus": 0},
        "housing": {
            "units_total": 0, "occupied": 0, "vacant": 0,
            "tenure": {"owner": 0, "renter": 0},
            "units_in_structure": {"1_det": 0, "2_units": 0, "3_4": 0, "5_9": 0, "10_19": 0, "20p": 0}
        },
        "costs": {
            "median_gross_rent": 0,
            "rent_burden_pct": {"gt_30": 0.0, "gt_50": 0.0},
            "median_home_value": 0
        },
        "jobs_workplace": {
            "total_jobs": 0,
            "earnings_bands": {"E1": 0, "E2": 0, "E3": 0},
            "by_sector": {"NAICS11": 0, "NAICS21": 0}
        }
    }

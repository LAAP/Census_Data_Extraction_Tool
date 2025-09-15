"""CSV export utilities for Census Grid Stats."""

import csv
import io
from typing import Dict, Any, List
from fastapi.responses import Response


def create_csv_row(data: Dict[str, Any]) -> str:
    """
    Create a single CSV row with the exact header specification.
    
    Args:
        data: Dictionary containing all the metrics data
        
    Returns:
        CSV string with single row
    """
    # Define the exact header order
    header = [
        "query", "address", "lat", "lon", "cell_km", "acs_year", "lodes_year",
        "population_total", "households_total",
        "age_0_4", "age_5_14", "age_15_24", "age_25_44", "age_45_64", "age_65p",
        "age_pct_0_4", "age_pct_5_14", "age_pct_15_24", "age_pct_25_44", "age_pct_45_64", "age_pct_65p",
        "income_median", "income_mean",
        "inc_lt_10k", "inc_10_15k", "inc_15_25k", "inc_25_35k", "inc_35_50k", "inc_50_75k", "inc_75_100k", "inc_100_125k", "inc_125_150k", "inc_150_200k", "inc_200k_plus",
        "inc_pct_lt_10k", "inc_pct_10_15k", "inc_pct_15_25k", "inc_pct_25_35k", "inc_pct_35_50k", "inc_pct_50_75k", "inc_pct_75_100k", "inc_pct_100_125k", "inc_pct_125_150k", "inc_pct_150_200k", "inc_pct_200k_plus",
        "labor_force", "employed", "unemployed", "unemployment_rate",
        "edu_hs_or_less", "edu_some_college", "edu_ba_plus",
        "housing_units", "occupied", "vacant", "owner", "renter",
        "units_1_det", "units_2", "units_3_4", "units_5_9", "units_10_19", "units_20p",
        "avg_hh_size_overall", "avg_hh_size_owner", "avg_hh_size_renter",
        "median_gross_rent", "median_home_value",
        "rent_burden_gt30_pct", "rent_burden_gt50_pct",
        "jobs_total", "E1_lt_1250", "E2_1251_3333", "E3_gt_3333"
    ]
    
    # Create output buffer
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(header)
    
    # Extract values in the exact order
    row_data = [
        data.get("query", ""),
        data.get("address", ""),
        data.get("lat", ""),
        data.get("lon", ""),
        data.get("cell_km", ""),
        data.get("acs_year", ""),
        data.get("lodes_year", ""),
        data.get("population_total", 0),
        data.get("households_total", 0),
        data.get("age_0_4", 0),
        data.get("age_5_14", 0),
        data.get("age_15_24", 0),
        data.get("age_25_44", 0),
        data.get("age_45_64", 0),
        data.get("age_65p", 0),
        data.get("age_pct_0_4", 0.0),
        data.get("age_pct_5_14", 0.0),
        data.get("age_pct_15_24", 0.0),
        data.get("age_pct_25_44", 0.0),
        data.get("age_pct_45_64", 0.0),
        data.get("age_pct_65p", 0.0),
        data.get("income_median", 0),
        data.get("income_mean", 0),
        data.get("inc_lt_10k", 0),
        data.get("inc_10_15k", 0),
        data.get("inc_15_25k", 0),
        data.get("inc_25_35k", 0),
        data.get("inc_35_50k", 0),
        data.get("inc_50_75k", 0),
        data.get("inc_75_100k", 0),
        data.get("inc_100_125k", 0),
        data.get("inc_125_150k", 0),
        data.get("inc_150_200k", 0),
        data.get("inc_200k_plus", 0),
        data.get("inc_pct_lt_10k", 0.0),
        data.get("inc_pct_10_15k", 0.0),
        data.get("inc_pct_15_25k", 0.0),
        data.get("inc_pct_25_35k", 0.0),
        data.get("inc_pct_35_50k", 0.0),
        data.get("inc_pct_50_75k", 0.0),
        data.get("inc_pct_75_100k", 0.0),
        data.get("inc_pct_100_125k", 0.0),
        data.get("inc_pct_125_150k", 0.0),
        data.get("inc_pct_150_200k", 0.0),
        data.get("inc_pct_200k_plus", 0.0),
        data.get("labor_force", 0),
        data.get("employed", 0),
        data.get("unemployed", 0),
        data.get("unemployment_rate", 0.0),
        data.get("edu_hs_or_less", 0),
        data.get("edu_some_college", 0),
        data.get("edu_ba_plus", 0),
        data.get("housing_units", 0),
        data.get("occupied", 0),
        data.get("vacant", 0),
        data.get("owner", 0),
        data.get("renter", 0),
        data.get("units_1_det", 0),
        data.get("units_2", 0),
        data.get("units_3_4", 0),
        data.get("units_5_9", 0),
        data.get("units_10_19", 0),
        data.get("units_20p", 0),
        data.get("avg_hh_size_overall", 0.0),
        data.get("avg_hh_size_owner", 0.0),
        data.get("avg_hh_size_renter", 0.0),
        data.get("median_gross_rent", 0),
        data.get("median_home_value", 0),
        data.get("rent_burden_gt30_pct", 0.0),
        data.get("rent_burden_gt50_pct", 0.0),
        data.get("jobs_total", 0),
        data.get("E1_lt_1250", 0),
        data.get("E2_1251_3333", 0),
        data.get("E3_gt_3333", 0)
    ]
    
    # Write data row
    writer.writerow(row_data)
    
    return output.getvalue()


def create_csv_response(data: Dict[str, Any]) -> Response:
    """
    Create a FastAPI Response object for CSV download.
    
    Args:
        data: Dictionary containing all the metrics data
        
    Returns:
        FastAPI Response with CSV content
    """
    csv_content = create_csv_row(data)
    
    return Response(
        content=csv_content,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": "attachment; filename=\"census_1km.csv\""
        }
    )


def prepare_data_for_csv(
    input_data: Dict[str, Any],
    area_data: Dict[str, Any],
    metrics_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Prepare data for CSV export by flattening the nested structure.
    
    Args:
        input_data: Input parameters
        area_data: Area information
        metrics_data: Metrics data
        
    Returns:
        Flattened dictionary ready for CSV export
    """
    # Start with input data
    csv_data = {
        "query": input_data.get("address", f"{input_data.get('lat')},{input_data.get('lon')}"),
        "address": input_data.get("address", ""),
        "lat": input_data.get("lat", ""),
        "lon": input_data.get("lon", ""),
        "cell_km": input_data.get("cell_km", 1.0),
        "acs_year": input_data.get("acs_year", 2023),
        "lodes_year": input_data.get("lodes_year", "")
    }
    
    # Add population data
    population = metrics_data.get("population", {})
    csv_data.update({
        "population_total": population.get("total", 0),
        "age_0_4": population.get("age", {}).get("0_4", 0),
        "age_5_14": population.get("age", {}).get("5_14", 0),
        "age_15_24": population.get("age", {}).get("15_24", 0),
        "age_25_44": population.get("age", {}).get("25_44", 0),
        "age_45_64": population.get("age", {}).get("45_64", 0),
        "age_65p": population.get("age", {}).get("65p", 0)
    })
    
    # Calculate age percentages
    total_pop = csv_data["population_total"]
    if total_pop > 0:
        csv_data.update({
            "age_pct_0_4": round((csv_data["age_0_4"] / total_pop) * 100, 2),
            "age_pct_5_14": round((csv_data["age_5_14"] / total_pop) * 100, 2),
            "age_pct_15_24": round((csv_data["age_15_24"] / total_pop) * 100, 2),
            "age_pct_25_44": round((csv_data["age_25_44"] / total_pop) * 100, 2),
            "age_pct_45_64": round((csv_data["age_45_64"] / total_pop) * 100, 2),
            "age_pct_65p": round((csv_data["age_65p"] / total_pop) * 100, 2)
        })
    else:
        csv_data.update({
            "age_pct_0_4": 0.0, "age_pct_5_14": 0.0, "age_pct_15_24": 0.0,
            "age_pct_25_44": 0.0, "age_pct_45_64": 0.0, "age_pct_65p": 0.0
        })
    
    # Add household data
    households = metrics_data.get("households", {})
    csv_data.update({
        "households_total": households.get("total", 0),
        "avg_hh_size_overall": households.get("avg_size", {}).get("overall", 0.0),
        "avg_hh_size_owner": households.get("avg_size", {}).get("owner", 0.0),
        "avg_hh_size_renter": households.get("avg_size", {}).get("renter", 0.0)
    })
    
    # Add income data
    income = metrics_data.get("income", {})
    brackets = income.get("brackets", {})
    csv_data.update({
        "income_median": income.get("median", 0),
        "income_mean": income.get("mean", 0),
        "inc_lt_10k": brackets.get("lt_10k", 0),
        "inc_10_15k": brackets.get("10_15k", 0),
        "inc_15_25k": brackets.get("15_25k", 0),
        "inc_25_35k": brackets.get("25_35k", 0),
        "inc_35_50k": brackets.get("35_50k", 0),
        "inc_50_75k": brackets.get("50_75k", 0),
        "inc_75_100k": brackets.get("75_100k", 0),
        "inc_100_125k": brackets.get("100_125k", 0),
        "inc_125_150k": brackets.get("125_150k", 0),
        "inc_150_200k": brackets.get("150_200k", 0),
        "inc_200k_plus": brackets.get("200k_plus", 0)
    })
    
    # Calculate income percentages
    total_hh = csv_data["households_total"]
    if total_hh > 0:
        csv_data.update({
            "inc_pct_lt_10k": round((csv_data["inc_lt_10k"] / total_hh) * 100, 2),
            "inc_pct_10_15k": round((csv_data["inc_10_15k"] / total_hh) * 100, 2),
            "inc_pct_15_25k": round((csv_data["inc_15_25k"] / total_hh) * 100, 2),
            "inc_pct_25_35k": round((csv_data["inc_25_35k"] / total_hh) * 100, 2),
            "inc_pct_35_50k": round((csv_data["inc_35_50k"] / total_hh) * 100, 2),
            "inc_pct_50_75k": round((csv_data["inc_50_75k"] / total_hh) * 100, 2),
            "inc_pct_75_100k": round((csv_data["inc_75_100k"] / total_hh) * 100, 2),
            "inc_pct_100_125k": round((csv_data["inc_100_125k"] / total_hh) * 100, 2),
            "inc_pct_125_150k": round((csv_data["inc_125_150k"] / total_hh) * 100, 2),
            "inc_pct_150_200k": round((csv_data["inc_150_200k"] / total_hh) * 100, 2),
            "inc_pct_200k_plus": round((csv_data["inc_200k_plus"] / total_hh) * 100, 2)
        })
    else:
        csv_data.update({
            "inc_pct_lt_10k": 0.0, "inc_pct_10_15k": 0.0, "inc_pct_15_25k": 0.0,
            "inc_pct_25_35k": 0.0, "inc_pct_35_50k": 0.0, "inc_pct_50_75k": 0.0,
            "inc_pct_75_100k": 0.0, "inc_pct_100_125k": 0.0, "inc_pct_125_150k": 0.0,
            "inc_pct_150_200k": 0.0, "inc_pct_200k_plus": 0.0
        })
    
    # Add employment data
    employment = metrics_data.get("employment", {})
    labor_force = employment.get("labor_force", 0)
    unemployed = employment.get("unemployed", 0)
    unemployment_rate = (unemployed / labor_force * 100) if labor_force > 0 else 0.0
    
    csv_data.update({
        "labor_force": labor_force,
        "employed": employment.get("employed", 0),
        "unemployed": unemployed,
        "unemployment_rate": round(unemployment_rate, 2)
    })
    
    # Add education data
    education = metrics_data.get("education", {})
    csv_data.update({
        "edu_hs_or_less": education.get("hs_or_less", 0),
        "edu_some_college": education.get("some_college", 0),
        "edu_ba_plus": education.get("ba_plus", 0)
    })
    
    # Add housing data
    housing = metrics_data.get("housing", {})
    tenure = housing.get("tenure", {})
    units_in_structure = housing.get("units_in_structure", {})
    
    csv_data.update({
        "housing_units": housing.get("units_total", 0),
        "occupied": housing.get("occupied", 0),
        "vacant": housing.get("vacant", 0),
        "owner": tenure.get("owner", 0),
        "renter": tenure.get("renter", 0),
        "units_1_det": units_in_structure.get("1_det", 0),
        "units_2": units_in_structure.get("2_units", 0),
        "units_3_4": units_in_structure.get("3_4", 0),
        "units_5_9": units_in_structure.get("5_9", 0),
        "units_10_19": units_in_structure.get("10_19", 0),
        "units_20p": units_in_structure.get("20p", 0)
    })
    
    # Add costs data
    costs = metrics_data.get("costs", {})
    rent_burden = costs.get("rent_burden_pct", {})
    
    csv_data.update({
        "median_gross_rent": costs.get("median_gross_rent", 0),
        "median_home_value": costs.get("median_home_value", 0),
        "rent_burden_gt30_pct": rent_burden.get("gt_30", 0.0),
        "rent_burden_gt50_pct": rent_burden.get("gt_50", 0.0)
    })
    
    # Add jobs data
    jobs = metrics_data.get("jobs_workplace", {})
    earnings_bands = jobs.get("earnings_bands", {})
    
    csv_data.update({
        "jobs_total": jobs.get("total_jobs", 0),
        "E1_lt_1250": earnings_bands.get("E1", 0),
        "E2_1251_3333": earnings_bands.get("E2", 0),
        "E3_gt_3333": earnings_bands.get("E3", 0)
    })
    
    return csv_data

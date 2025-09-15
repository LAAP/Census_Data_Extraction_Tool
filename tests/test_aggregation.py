"""Tests for aggregation utilities."""

import pytest
from app.aggregation import (
    area_weighted_sum, area_weighted_median, calculate_age_distribution,
    calculate_income_brackets, quantile_from_brackets, aggregate_metrics
)


class TestAreaWeightedAggregation:
    """Test area-weighted aggregation functions."""
    
    def test_area_weighted_sum(self):
        """Test area-weighted sum calculation."""
        block_group_data = [
            {"data": {"population": 100}, "area_weight": 0.5},
            {"data": {"population": 200}, "area_weight": 0.3},
            {"data": {"population": 300}, "area_weight": 0.2}
        ]
        
        result = area_weighted_sum(block_group_data, "population")
        expected = 100 * 0.5 + 200 * 0.3 + 300 * 0.2
        assert result == expected
    
    def test_area_weighted_sum_empty(self):
        """Test area-weighted sum with empty data."""
        result = area_weighted_sum([], "population")
        assert result == 0.0
    
    def test_area_weighted_median(self):
        """Test area-weighted median calculation."""
        block_group_data = [
            {"data": {"income": 50000}, "area_weight": 0.5},
            {"data": {"income": 60000}, "area_weight": 0.3},
            {"data": {"income": 70000}, "area_weight": 0.2}
        ]
        
        result = area_weighted_median(block_group_data, "income")
        expected = (50000 * 0.5 + 60000 * 0.3 + 70000 * 0.2) / (0.5 + 0.3 + 0.2)
        assert result == expected


class TestAgeDistribution:
    """Test age distribution calculation."""
    
    def test_calculate_age_distribution(self):
        """Test age distribution calculation."""
        block_group_data = [
            {
                "data": {
                    "male_0_4": 10, "female_0_4": 8,
                    "male_5_9": 12, "female_5_9": 10,
                    "male_15_17": 5, "female_15_17": 4,
                    "male_25_29": 15, "female_25_29": 12
                },
                "area_weight": 1.0
            }
        ]
        
        result = calculate_age_distribution(block_group_data)
        
        assert result["0_4"] == 18  # 10 + 8
        assert result["5_14"] == 22  # 12 + 10
        assert result["15_24"] == 9  # 5 + 4
        assert result["25_44"] == 27  # 15 + 12


class TestIncomeBrackets:
    """Test income bracket calculation."""
    
    def test_calculate_income_brackets(self):
        """Test income bracket calculation."""
        block_group_data = [
            {
                "data": {
                    "income_lt_10k": 5,
                    "income_10_15k": 10,
                    "income_15_20k": 15,
                    "income_20_25k": 20
                },
                "area_weight": 1.0
            }
        ]
        
        result = calculate_income_brackets(block_group_data)
        
        assert result["lt_10k"] == 5
        assert result["10_15k"] == 10
        assert result["15_25k"] == 35  # 15 + 20
        assert result["25_35k"] == 0


class TestQuantileCalculation:
    """Test quantile calculation from brackets."""
    
    def test_quantile_from_brackets(self):
        """Test quantile calculation from bracket distribution."""
        brackets = {
            "lt_10k": 10,
            "10_15k": 20,
            "15_25k": 30,
            "25_35k": 40
        }
        
        bracket_ranges = {
            "lt_10k": (0, 10),
            "10_15k": (10, 15),
            "15_25k": (15, 25),
            "25_35k": (25, 35)
        }
        
        # Test median (50th percentile)
        median = quantile_from_brackets(brackets, bracket_ranges, 0.5)
        assert 15 <= median <= 25  # Should be in the 15-25k bracket
        
        # Test 25th percentile
        q25 = quantile_from_brackets(brackets, bracket_ranges, 0.25)
        assert 10 <= q25 <= 15  # Should be in the 10-15k bracket
    
    def test_quantile_empty_brackets(self):
        """Test quantile calculation with empty brackets."""
        brackets = {}
        bracket_ranges = {}
        
        result = quantile_from_brackets(brackets, bracket_ranges, 0.5)
        assert result == 0.0


class TestAggregateMetrics:
    """Test full metrics aggregation."""
    
    def test_aggregate_metrics_empty(self):
        """Test aggregation with empty data."""
        result = aggregate_metrics([])
        
        assert result["population"]["total"] == 0
        assert result["households"]["total"] == 0
        assert result["income"]["median"] == 0
    
    def test_aggregate_metrics_basic(self):
        """Test aggregation with basic data."""
        block_group_data = [
            {
                "data": {
                    "total_population": 1000,
                    "total_households": 400,
                    "aggregate_household_income": 50000000,
                    "civilian_labor_force": 600,
                    "employed": 550,
                    "unemployed": 50,
                    "total_housing_units": 450,
                    "occupied_housing_units": 400,
                    "vacant_housing_units": 50,
                    "owner_occupied": 250,
                    "renter_occupied": 150,
                    "median_gross_rent": 1500,
                    "median_home_value": 500000,
                    "male_0_4": 50, "female_0_4": 45,
                    "male_5_9": 60, "female_5_9": 55,
                    "bachelors_degree": 200, "masters_degree": 100,
                    "total_education_population": 800
                },
                "area_weight": 1.0
            }
        ]
        
        result = aggregate_metrics(block_group_data)
        
        assert result["population"]["total"] == 1000
        assert result["households"]["total"] == 400
        assert result["employment"]["labor_force"] == 600
        assert result["housing"]["units_total"] == 450

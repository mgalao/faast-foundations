"""Tests for the cleaning module"""
import pytest
import pandas as pd
from life_expectancy.cleaning import (
    clean_data
    , split_metadata_columns
    , melt_years
    , clean_types
    , filter_country
)

@pytest.mark.parametrize(
    "country, expected_fixture",
    [
        ("PT", "pt_life_expectancy_expected"),
        ("ES", "es_life_expectancy_expected"),
        ("FR", "fr_life_expectancy_expected"),
    ],
)
def test_clean_data(
    country,
    expected_fixture,
    eu_life_expectancy_sample,
    request,
):
    """
    Run the cleaning pipeline for different countries using the sample fixture.
    Compare actual output with expected fixture.
    """
    # Clean the sample data for the given country
    df_cleaned = clean_data(eu_life_expectancy_sample, country=country)

    # Get the expected DataFrame from the fixture dynamically
    df_expected = request.getfixturevalue(expected_fixture)

    # Compare actual vs expected
    pd.testing.assert_frame_equal(df_cleaned, df_expected)


def test_split_metadata_columns(eu_life_expectancy_sample):
    """
    Test that split_metadata_columns correctly separates the first column
    into metadata fields.
    """
    df_split = split_metadata_columns(eu_life_expectancy_sample)

    # Check that the new columns were created
    assert "unit" in df_split.columns
    assert "sex" in df_split.columns
    assert "age" in df_split.columns
    assert "region" in df_split.columns

    # Check that we have the expected number of columns
    # (original + 4 new metadata columns)
    assert len(df_split.columns) == len(eu_life_expectancy_sample.columns) + 4


def test_melt_years():
    """Test that melt_years correctly transforms from wide to long format."""
    # Create a simple test DataFrame
    df_wide = pd.DataFrame({
        "composed": ["a,b,c,PT"],
        "2020": ["10.5"],
        "2021": ["11.0"],
        "unit": ["YR"],
        "sex": ["M"],
        "age": ["Y10"],
        "region": ["PT"]
    })

    df_long = melt_years(df_wide)

    # Check that year column was created
    assert "year" in df_long.columns
    assert "value" in df_long.columns

    # Check that we have 2 rows (one for each year)
    assert len(df_long) == 2

    # Check that metadata columns are preserved
    assert "unit" in df_long.columns
    assert "sex" in df_long.columns
    assert "age" in df_long.columns
    assert "region" in df_long.columns


def test_clean_types(eu_life_expectancy_sample):
    """
    Test that clean_types correctly converts
    'year' to int and 'value' to float.
    """
    df = split_metadata_columns(eu_life_expectancy_sample)
    df = melt_years(df)
    df = clean_types(df)
    
    # Check types
    assert df["year"].dtype == int
    assert df["value"].dtype == float
    assert not df["value"].isna().any()


def test_filter_country():
    """
    Test that filter_country correctly filters data
    for a specific country.
    """
    # Create a test DataFrame with multiple countries
    df_multi = pd.DataFrame({
        "region": ["PT", "ES", "PT", "FR"],
        "year": [2020, 2020, 2021, 2020],
        "value": [80.5, 82.0, 81.0, 83.0]
    })

    df_pt = filter_country(df_multi, "PT")

    # Check that only PT rows are returned
    assert len(df_pt) == 2
    assert all(df_pt["region"] == "PT")

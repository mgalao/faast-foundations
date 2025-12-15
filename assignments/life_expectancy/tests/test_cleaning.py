"""Tests for the cleaning module"""
import pytest
import pandas as pd
from life_expectancy.cleaning import clean_data
from . import OUTPUT_DIR

@pytest.mark.parametrize("country", ["PT", "ES", "FR"])
def test_clean_data(pt_life_expectancy_expected, country):
    """Run the `clean_data` function and compare the output to the expected output"""
    clean_data(country=country)

    output_file = OUTPUT_DIR / f"{country.lower()}_life_expectancy.csv"
    df_actual = pd.read_csv(output_file)

    if country == "PT":
        # Only compare for PT as we only have the expected data for it
        pd.testing.assert_frame_equal(df_actual, pt_life_expectancy_expected)
    else:
        # Check that the output file is not empty for other countries
        assert not df_actual.empty

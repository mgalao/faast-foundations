"""Tests for the cleaning module"""
import pytest
import pandas as pd
from pathlib import Path

from life_expectancy.cleaning import load_data, clean_data, save_data
from . import OUTPUT_DIR


@pytest.mark.parametrize("country", ["PT", "ES", "FR"])
def test_clean_data(pt_life_expectancy_expected, country):
    """
    Run the cleaning pipeline for different countries.
    For PT, compare to expected DataFrame.
    For other countries, just check that the output file is created and non-empty.
    """
    # Load, clean, and save data
    df_raw = load_data()
    df_cleaned = clean_data(df_raw, country=country)
    save_data(df_cleaned, country=country, output_dir=OUTPUT_DIR)

    # Read back the saved CSV
    output_file = Path(OUTPUT_DIR) / f"{country.lower()}_life_expectancy.csv"
    df_actual = pd.read_csv(output_file)

    # Validate the results
    if country == "PT":
        # Compare actual vs expected for PT
        pd.testing.assert_frame_equal(df_actual, pt_life_expectancy_expected)
    else:
        # For other countries, check the file is not empty
        assert not df_actual.empty
        assert "year" in df_actual.columns
        assert "value" in df_actual.columns

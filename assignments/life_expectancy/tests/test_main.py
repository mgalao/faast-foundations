"""Tests for the main orchestration function"""
from unittest.mock import patch

import pandas as pd
import pytest

from life_expectancy.cleaning import main

@pytest.mark.parametrize("country", ["PT", "ES", "FR"])
def test_main_orchestrates_pipeline(country):
    """
    Test that main function orchestrates
    load_data, clean_data, and save_data correctly.
    """
    mock_raw_df = pd.DataFrame({"col": ["data"]})
    mock_clean_df = pd.DataFrame({"cleaned": ["data"]})

    with patch("life_expectancy.cleaning.load_data") as mock_load, \
         patch("life_expectancy.cleaning.clean_data") as mock_clean, \
         patch("life_expectancy.cleaning.save_data") as mock_save:

        mock_load.return_value = mock_raw_df
        mock_clean.return_value = mock_clean_df

        main(country=country)

        mock_load.assert_called_once()
        mock_clean.assert_called_once_with(mock_raw_df, country=country)
        mock_save.assert_called_once_with(mock_clean_df, country=country)

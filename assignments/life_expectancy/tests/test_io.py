"""Tests for I/O operations (load_data and save_data)"""
from unittest.mock import patch
import pandas as pd
from life_expectancy.cleaning import save_data, load_data


def test_load_data_reads_csv():
    """Test that load_data calls pd.read_csv with the correct arguments."""
    with patch("pandas.read_csv") as mock_read:
        mock_read.return_value = pd.DataFrame()

        df = load_data("fake.tsv")

        # Assert read_csv was called once
        mock_read.assert_called_once()

        # Assert it was called with correct arguments
        call_args = mock_read.call_args
        assert call_args[0][0] == "fake.tsv"
        assert call_args[1]["sep"] == "\t"

        # Assert the return value is a DataFrame
        assert isinstance(df, pd.DataFrame)


def test_load_data_uses_default_path():
    """Test that load_data uses the default file path when none is provided."""
    with patch("pandas.read_csv") as mock_read:
        mock_read.return_value = pd.DataFrame()

        load_data()

        # Assert it was called with the default path
        call_args = mock_read.call_args
        assert "eu_life_expectancy_raw.tsv" in call_args[0][0]


def test_save_data_calls_to_csv_with_correct_args():
    """
    Test that save_data calls to_csv with
    the correct file path and arguments.
    """
    df = pd.DataFrame({"a": [1, 2]})

    with patch("pandas.DataFrame.to_csv") as mock_to_csv:
        save_data(df, country="PT", output_dir="some/dir")

        # Assert to_csv was called once
        mock_to_csv.assert_called_once()

        # Assert it was called with the correct arguments
        call_args = mock_to_csv.call_args
        assert call_args[0][0] == "some/dir/pt_life_expectancy.csv"
        assert call_args[1]["index"] is False


def test_save_data_uses_correct_filename_for_country():
    """
    Test that save_data generates the correct filename
    for different countries.
    """
    df = pd.DataFrame({"a": [1, 2]})

    with patch("pandas.DataFrame.to_csv") as mock_to_csv:
        save_data(df, country="ES", output_dir="output")

        # Check the filename is correct
        call_args = mock_to_csv.call_args
        assert call_args[0][0] == "output/es_life_expectancy.csv"

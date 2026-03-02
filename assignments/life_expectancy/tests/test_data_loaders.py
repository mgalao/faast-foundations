"""Tests for the data_loaders module"""
import pytest
import pandas as pd
from pathlib import Path
from life_expectancy.data_loaders import (
    DataLoader,
    TSVDataLoader,
    JSONDataLoader,
    get_data_loader
)
from . import OUTPUT_DIR


def test_tsv_data_loader():
    """Test TSVDataLoader can load TSV files."""
    loader = TSVDataLoader()
    file_path = str(OUTPUT_DIR / "eu_life_expectancy_raw.tsv")
    
    df = loader.load(file_path)
    
    # Check that data was loaded
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # TSV format has the composed first column
    assert df.columns[0].startswith("unit")


def test_json_data_loader():
    """Test JSONDataLoader can load and standardize JSON files."""
    loader = JSONDataLoader()
    file_path = str(OUTPUT_DIR / "eurostat_life_expect.json")
    
    df = loader.load(file_path)
    
    # Check that data was loaded
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    
    # Check that columns were standardized
    expected_columns = {'unit', 'sex', 'age', 'region', 'year', 'value'}
    assert set(df.columns) == expected_columns
    
    # Check that 'country' was renamed to 'region'
    assert 'country' not in df.columns
    assert 'region' in df.columns
    
    # Check that 'life_expectancy' was renamed to 'value'
    assert 'life_expectancy' not in df.columns
    assert 'value' in df.columns


def test_get_data_loader_tsv():
    """Test factory function returns TSVDataLoader for .tsv files."""
    loader = get_data_loader("data/file.tsv")
    assert isinstance(loader, TSVDataLoader)


def test_get_data_loader_json():
    """Test factory function returns JSONDataLoader for .json files."""
    loader = get_data_loader("data/file.json")
    assert isinstance(loader, JSONDataLoader)


def test_get_data_loader_unsupported_format():
    """Test factory function raises ValueError for unsupported formats."""
    with pytest.raises(ValueError, match="Unsupported file format"):
        get_data_loader("data/file.csv")


def test_get_data_loader_case_insensitive():
    """Test factory function handles uppercase extensions."""
    loader = get_data_loader("data/file.JSON")
    assert isinstance(loader, JSONDataLoader)
    
    loader = get_data_loader("data/file.TSV")
    assert isinstance(loader, TSVDataLoader)

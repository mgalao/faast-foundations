"""Pytest configuration file"""
import pandas as pd
import pytest

from . import FIXTURES_DIR

@pytest.fixture(scope="session")
def eu_life_expectancy_sample() -> pd.DataFrame:
    """Return the sample input for tests."""
    return pd.read_csv(
        FIXTURES_DIR / "eu_life_expectancy_raw_sample.tsv",
        sep="\t"
    )

@pytest.fixture(scope="session")
def pt_life_expectancy_expected() -> pd.DataFrame:
    """Expected output for Portugal (PT)."""
    return pd.read_csv(FIXTURES_DIR / "pt_life_expectancy_expected.csv")

@pytest.fixture(scope="session")
def es_life_expectancy_expected() -> pd.DataFrame:
    """Expected output for Spain (ES)."""
    return pd.read_csv(FIXTURES_DIR / "es_life_expectancy_expected.csv")

@pytest.fixture(scope="session")
def fr_life_expectancy_expected() -> pd.DataFrame:
    """Expected output for France (FR)."""
    return pd.read_csv(FIXTURES_DIR / "fr_life_expectancy_expected.csv")

"""
Data loader strategies for different file formats.

This module implements the Strategy pattern to allow loading
life expectancy data from different file formats (TSV, JSON, etc.).
"""
import logging
from abc import ABC, abstractmethod
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


class DataLoader(ABC):  # pylint: disable=too-few-public-methods
    """Abstract base class for data loading strategies."""

    @abstractmethod
    def load(self, file_path: str) -> pd.DataFrame:
        """
        Load data from a file and return a DataFrame.

        Args:
            file_path: Path to the data file.

        Returns:
            DataFrame with the loaded data.
        """


class TSVDataLoader(DataLoader):  # pylint: disable=too-few-public-methods
    """Strategy for loading TSV files in the original Eurostat format."""

    def load(self, file_path: str) -> pd.DataFrame:
        """
        Load TSV file with life expectancy data.

        The TSV format has a composed first column with metadata
        and separate columns for each year.

        Args:
            file_path: Path to the TSV file.

        Returns:
            Raw DataFrame from TSV.
        """
        df = pd.read_csv(file_path, sep="\t")
        logger.debug("Loaded TSV data with shape: %s", df.shape)
        return df


class JSONDataLoader(DataLoader):  # pylint: disable=too-few-public-methods
    """Strategy for loading JSON files in the new Eurostat format."""

    def load(self, file_path: str) -> pd.DataFrame:
        """
        Load JSON file with life expectancy data.

        The JSON format is already in long format with columns:
        unit, sex, age, country, year, life_expectancy, flag, flag_detail.

        We need to standardize column names to match the TSV pipeline:
        - 'country' -> 'region'
        - 'life_expectancy' -> 'value'

        Args:
            file_path: Path to the JSON file.

        Returns:
            DataFrame with standardized column names.
        """
        df = pd.read_json(file_path)
        logger.debug("Loaded JSON data with shape: %s", df.shape)

        # Standardize column names to match TSV processing pipeline
        df = df.rename(columns={
            'country': 'region',
            'life_expectancy': 'value'
        })

        # Select only the columns we need (matching TSV pipeline output)
        expected_columns = ['unit', 'sex', 'age', 'region', 'year', 'value']
        df = df[expected_columns]

        logger.debug("Standardized JSON data to match TSV format")
        return df


def get_data_loader(file_path: str) -> DataLoader:
    """
    Factory function to get the appropriate data loader based on file extension.

    Args:
        file_path: Path to the data file.

    Returns:
        Instance of the appropriate DataLoader.

    Raises:
        ValueError: If the file format is not supported.
    """
    file_extension = Path(file_path).suffix.lower()

    loaders = {
        '.tsv': TSVDataLoader(),
        '.json': JSONDataLoader(),
    }

    loader = loaders.get(file_extension)
    if loader is None:
        raise ValueError(
            f"Unsupported file format: {file_extension}. "
            f"Supported formats: {list(loaders.keys())}"
        )

    logger.debug("Selected %s loader for %s", type(loader).__name__, file_path)
    return loader

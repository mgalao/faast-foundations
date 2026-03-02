import logging
import argparse
import pandas as pd

from life_expectancy.regions import Region

# Set up logging
logging.basicConfig(
    level=logging.INFO, # INFO for normal runs; DEBUG for more verbosity
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_data(
    raw_file: str = "life_expectancy/data/eu_life_expectancy_raw.tsv"
) -> pd.DataFrame:
    """
    Load the raw EU life expectancy dataset from a TSV file.

    Args:
        raw_file: Path to the raw TSV file.

    Returns:
        DataFrame containing the raw dataset.
    """
    df = pd.read_csv(raw_file, sep="\t")
    logger.debug("Loaded raw data with shape: %s", df.shape)
    return df


def split_metadata_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split the first composed column into separate metadata columns:
    unit, sex, age, region.

    Args:
        df: Raw DataFrame with the first column
        containing comma-separated metadata.

    Returns:
        DataFrame with separate metadata columns.
    """
    df = df.copy()
    df[['unit', 'sex', 'age', 'region']] = (
        df.iloc[:, 0].str.split(",", expand=True)
    )
    logger.debug(
        "Split first column into metadata columns: unit, sex, age, region"
    )
    return df


def melt_years(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape the DataFrame from wide to long format, keeping only year columns
    and the metadata columns (unit, sex, age, region).

    Columns layout after split:
    [composed_col, 2021, 2020, ..., 1960, unit, sex, age, region]
    Year columns are all except the first composed column and
    the last four metadata columns.

    Args:
        df: DataFrame with separated metadata columns.

    Returns:
        DataFrame in long format with columns:
            unit, sex, age, region, year, value.
    """
    year_columns = df.columns[1:-4]
    df_long = pd.melt(
        df,
        id_vars=['unit', 'sex', 'age', 'region'],
        value_vars=year_columns,
        var_name='year',
        value_name='value'
    )
    logger.debug(
        "Melted year columns into long format with shape: %s", df_long.shape
    )
    return df_long


def clean_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert 'year' and 'value' columns to numeric types
    and clean the 'value' column.

    - 'year' is stripped and converted to int.
    - 'value' has non-numeric characters removed (except the dot)
        and is converted to float.
    - Rows with NaN in 'value' are dropped.

    Example conversions:
        '21.7 e' -> 21.7
        '18.5*'  -> 18.5
        '20,5'   -> 205

    Args:
        df: DataFrame with 'year' and 'value' columns as strings.

    Returns:
        DataFrame with numeric 'year' and 'value'.
    """
    df = df.copy()
    df['year'] = df['year'].str.strip().astype(int)
    df['value'] = df['value'].astype(str).str.strip().str.replace(
        r'[^0-9.]', '', regex=True
    )
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])
    logger.debug("Converted 'year' to int, cleaned 'value', dropped NaNs")
    return df


def filter_country(df: pd.DataFrame, country: Region) -> pd.DataFrame:
    """
    Filter the DataFrame to only include rows for the specified country.

    Args:
        df: Cleaned DataFrame.
        country: Country region (e.g., Region.PT).

    Returns:
        DataFrame containing only rows for the specified country.
    """
    df_country = df[df['region'] == country.value].copy()
    logger.debug(
        "Filtered data for country '%s' with shape: %s",
        country, df_country.shape
    )
    return df_country


def clean_data(df: pd.DataFrame, country: Region = Region.PT) -> pd.DataFrame:
    """
    Orchestrate the cleaning pipeline:
    - Split metadata columns
    - Melt year columns
    - Convert types and clean values
    - Filter for a specific country

    Args:
        df: Raw DataFrame.
        country: Country region to filter by (default Region.PT).

    Returns:
        Cleaned DataFrame for the specified country.
    """
    df = split_metadata_columns(df)
    df = melt_years(df)
    df = clean_types(df)
    df = filter_country(df, country)
    df = df.reset_index(drop=True)
    logger.info("Completed cleaning for country: %s", country)
    return df


def save_data(
    df: pd.DataFrame,
    country: Region = Region.PT,
    output_dir: str = "life_expectancy/data"
) -> None:
    """
    Save the cleaned life expectancy data for a country to a CSV file.

    Args:
        df: Cleaned DataFrame.
        country: Country region (e.g., Region.PT).
        output_dir: Directory where the CSV will be saved.
    """
    output_file = f"{output_dir}/{country.lower()}_life_expectancy.csv"
    df.to_csv(output_file, index=False)
    logger.info("Saved cleaned data to %s", output_file)


def main(country: Region = Region.PT) -> None:
    """
    Load, clean, and save life expectancy data for a given country.

    Args:
        country: Country region to filter by (default Region.PT).
    """
    df_raw = load_data()
    df_clean = clean_data(df_raw, country=country)
    save_data(df_clean, country=country)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Clean EU life expectancy data."
    )
    parser.add_argument(
        "--country",
        type=lambda c: Region[c],
        choices=list(Region),
        default=Region.PT,
        help="Country code to filter the data (default: PT)"
    )
    args = parser.parse_args()
    main(country=args.country)

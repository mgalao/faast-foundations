import logging
import argparse
import pandas as pd

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def clean_data(country="PT"):
    """
    Cleans the raw life expectancy data for a specified country
    and saves it to a CSV file.
    """
    raw_file = "life_expectancy/data/eu_life_expectancy_raw.tsv"
    output_file = f"life_expectancy/data/{country.lower()}_life_expectancy.csv"

    # Load data
    df = pd.read_csv(raw_file, sep="\t")
    logger.debug("\nInitial dataframe:\n%s\n", df.head())

    # Split the composed first column
    df[['unit', 'sex', 'age', 'region']] = df.iloc[:, 0].str.split(",", expand=True)
    logger.debug("\nSplitted first column into multiple columns:\n%s\n", df.head())

    # Unpivot
    year_columns = df.columns[1:-4] # Exclude composed column and new columns
    df_long = pd.melt(
        df,
        id_vars=['unit', 'sex', 'age', 'region'],
        value_vars=year_columns,
        var_name='year',
        value_name='value'
    )
    logger.debug("\nUnpivoted date to long format:\n%s\n", df_long.head())

    # Inspect rows for year 2021 before converting value
    logging.debug("\nRows for 2021 before conversion:\n%s\n", df_long[df_long['year'] == '2021'])

    # Convert types
    df_long['year'] = df_long['year'].str.strip().astype(int)
    # Remove any non-numeric characters except the dot (.)
        # Example:
        #   '21.7 e' -> '21.7'
        #   '18.5*'  -> '18.5'
        #   '20,5'   -> '205'
    df_long['value'] = df_long['value'].str.strip().str.replace(r'[^0-9.]', '', regex=True)
    df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
    df_long = df_long.dropna(subset=['value'])
    logger.debug("\nConverted year to int, value to float, dropped NaNs:\n%s\n", df_long.head())
    logger.debug("\nData types of the dataframe:\n%s\n", df_long.dtypes)

    # Filter by country
    df_country = df_long[df_long['region'] == country].copy()
    logger.debug("\nFiltered data for %s:\n%s\n", country, df_country.head())

    # Save cleaned data
    df_country.to_csv(output_file, index=False)
    logger.info("\nSaved cleaned data to %s\n", output_file)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Clean EU life expectancy data.")
    parser.add_argument(
        "--country",
        type=str,
        default="PT",
        help="Country code to filter the data (default: PT)"
    )
    args = parser.parse_args()
    clean_data(country=args.country)

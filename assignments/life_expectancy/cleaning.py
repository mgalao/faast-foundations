import logging
import pandas as pd

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def clean_data():
    logging.info("Loading raw data")
    df = pd.read_csv("life_expectancy/data/eu_life_expectancy_raw.tsv", sep="\t")
    logging.debug("Initial dataframe:\n%s", df.head())

    logging.info("Unpivoting the date to long format")
    

if __name__ == "__main__":  # pragma: no cover
    clean_data()
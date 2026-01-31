from pathlib import Path

import pandas as pd

from life_expectancy.cleaning import clean_data

# Directories
PACKAGE_DIR = Path(__file__).parent
DATA_DIR = PACKAGE_DIR / "data"
FIXTURES_DIR = PACKAGE_DIR / "tests" / "fixtures"
FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

# Load full raw dataset
raw_file = DATA_DIR / "eu_life_expectancy_raw.tsv"
df_full = pd.read_csv(raw_file, sep="\t")

# Create sample with at least some rows from each country
df_pt = df_full[df_full.iloc[:, 0].str.contains("PT")].head(2)
df_es = df_full[df_full.iloc[:, 0].str.contains("ES")].head(2)
df_fr = df_full[df_full.iloc[:, 0].str.contains("FR")].head(2)
df_sample = pd.concat([df_pt, df_es, df_fr], ignore_index=True)

# Save sample TSV
sample_file = FIXTURES_DIR / "eu_life_expectancy_raw_sample.tsv"
df_sample.to_csv(sample_file, sep="\t", index=False)

# Generate expected cleaned CSV for each country
for country in ["PT", "ES", "FR"]:
    df_expected = clean_data(df_sample, country=country)
    expected_file = (
        FIXTURES_DIR / f"{country.lower()}_life_expectancy_expected.csv"
    )
    df_expected.to_csv(expected_file, index=False)
    
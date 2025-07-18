import pandas as pd
from logger.logger_config import setup_logger

logger = setup_logger(__name__)


def extract_csv(
        file_path: str | None
) -> pd.DataFrame:
    """
    Reads a CSV file and returns its contents as a pandas DataFrame.
    """
    try:

        logger.info(f"Reading CSV from: {file_path}")
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"Successfully read CSV: {df.shape[0]} rows")

        return df
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except Exception:
        logger.exception("Unexpected error occurred while reading CSV")
    

def normalization_data_from_csv(
        df: pd.DataFrame | None
) -> pd.DataFrame:
    """
    Normalize the data from a CSV by:
    - Dropping the 6th column (index 5).
    - Dropping columns from index 6 to 17 (inclusive).
    - Renaming the column '(kW)' to 'kW'.

    :param df: The input pandas DataFrame.
    :return: Normalized DataFrame.
    """
    try:

        df = df.drop(df.columns[5], axis=1)
        df = df.drop(df.columns[6:18], axis=1)
        df = df.rename(columns={'(kW)': 'kW'})

        return df
    
    except Exception as e:
        logger.exception("Unexpected error occurred while reading DataFrame")

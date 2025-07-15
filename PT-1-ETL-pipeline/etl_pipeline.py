import pandas as pd

def extract_csv(file_path) -> pd.DataFrame:
    """
    Reads a CSV file and returns its contents as a pandas DataFrame.

    :param file_path: Path to the CSV file.
    :return: DataFrame with the CSV contents.
    """
    df = pd.read_csv(file_path, encoding='utf-8')

    return df


def normalization_data_from_csv(df):
    """
    Normalize the data from a CSV by:
    - Dropping the 6th column (index 5).
    - Dropping columns from index 6 to 17 (inclusive).
    - Renaming the column '(kW)' to 'kW'.

    :param df: The input pandas DataFrame.
    :return: Normalized DataFrame.
    """
    df = df.drop(df.columns[5], axis=1)
    df = df.drop(df.columns[6:18], axis=1)
    df = df.rename(columns={'(kW)': 'kW'})

    return df

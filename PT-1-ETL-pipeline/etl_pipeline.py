import numpy as np
import pandas as pd

def extract_csv(file_path):

    with open(file_path, mode='r', encoding='utf-8') as f:
        df = pd.read_csv(file_path)
    return df


def normalization_data_from_csv(df):

    df = df.drop(df.columns[5], axis=1)
    df = df.drop(df.columns[6:18], axis=1)
    df = df.rename(columns={'(kW)': 'kW'})

    return df




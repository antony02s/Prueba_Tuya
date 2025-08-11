# extract/read_files.py
import pandas as pd
from pathlib import Path

def read_csv(path, **kwargs):
    return pd.read_csv(path, **kwargs)

def read_parquet(path, **kwargs):
    return pd.read_parquet(path, **kwargs)

def list_files(folder, pattern="*.csv"):
    return sorted(Path(folder).glob(pattern))
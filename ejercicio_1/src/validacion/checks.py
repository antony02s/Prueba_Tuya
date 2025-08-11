
import pandas as pd

class ValidationError(Exception): pass

def assert_columns(df: pd.DataFrame, required: list[str]):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValidationError(f"Missing columns: {missing}")

def assert_not_null(df: pd.DataFrame, cols: list[str]):
    for c in cols:
        if df[c].isna().any():
            raise ValidationError(f"Nulls found in {c}")

def assert_unique(df: pd.DataFrame, keys: list[str]):
    if df.duplicated(keys).any():
        raise ValidationError(f"Duplicates for keys {keys}")
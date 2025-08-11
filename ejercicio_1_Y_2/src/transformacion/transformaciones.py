import pandas as pd

def normalize_columns(df: pd.DataFrame):
    df = df.rename(columns=str.lower)
    return df

def cast_types(df: pd.DataFrame, schema: dict[str, str]):
    for col, dtype in schema.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype, errors="ignore")
    return df

def business_rules(df: pd.DataFrame):
    # ejemplo: filtrar registros inválidos
    return df[df["status"].isin(["active","pending"])]
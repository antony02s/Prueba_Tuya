# load/to_postgres.py
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine(user, pwd, host, db, port=5432):
    url = f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url)

def upsert_dataframe(df: pd.DataFrame, engine, table: str, unique_key: str):
    # 1) Cargar a staging temporal
    tmp_table = f"{table}_tmp"
    with engine.begin() as conn:
        df.to_sql(tmp_table, conn, if_exists="replace", index=False)
        # 2) UPSERT (asume columnas simples)
        cols = ", ".join(df.columns)
        updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in df.columns if c != unique_key])
        sql = f"""
            INSERT INTO {table} ({cols})
            SELECT {cols} FROM {tmp_table}
            ON CONFLICT ({unique_key}) DO UPDATE SET {updates};
            DROP TABLE {tmp_table};
        """
        conn.execute(text(sql))
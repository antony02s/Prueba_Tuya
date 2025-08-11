# src/load/to_postgres.py
import uuid
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine(user, pwd, host, db, port=5432):
    url = f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url)

def _split_table(table: str):
    if "." in table:
        schema, name = table.split(".", 1)
    else:
        schema, name = "public", table
    return schema, name

def ensure_unique_index(engine, table: str, unique_keys: list[str]):
    """Crea un índice único si no existe; requerido por ON CONFLICT."""
    schema, name = _split_table(table)
    idx_name = f"ux_{name}_" + "_".join(unique_keys)
    cols = ", ".join(unique_keys)
    sql = f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" ON "{schema}"."{name}" ({cols});'
    with engine.begin() as con:
        con.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        con.execute(text(sql))

def upsert_dataframe(
    df: pd.DataFrame,
    engine,
    table: str,
    unique_keys: list[str],
    chunk_size: int = 100000,
):
    """
    UPSERT idempotente usando tabla temporal + ON CONFLICT.
    - unique_keys: lista de columnas que identifican un registro (compuestas OK).
    - Si df está vacío, no hace nada.
    """
    if df is None or df.empty:
        return 0

    # Asegura índice único para ON CONFLICT
    ensure_unique_index(engine, table, unique_keys)

    schema, name = _split_table(table)
    tmp = f"tmp_{name}_{uuid.uuid4().hex[:8]}"

    cols = list(df.columns)
    col_list = ", ".join([f'"{c}"' for c in cols])
    conflict_cols = ", ".join([f'"{c}"' for c in unique_keys])

    # columnas a actualizar = todas menos las de la clave
    to_update = [c for c in cols if c not in unique_keys]
    if not to_update:
        # Si todo es clave, no hay nada que actualizar (solo evita duplicados)
        set_clause = ""
    else:
        set_clause = ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in to_update])

    with engine.begin() as con:
        # Garantiza schema
        con.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

        # Crea tabla temporal y vuelca datos
        df.to_sql(tmp, con, schema=schema, if_exists="replace", index=False, chunksize=chunk_size)

        # Inserta + upsert
        upsert_sql = f'''
        INSERT INTO "{schema}"."{name}" ({col_list})
        SELECT {col_list} FROM "{schema}"."{tmp}"
        ON CONFLICT ({conflict_cols}) DO
        {'NOTHING' if set_clause == "" else f'UPDATE SET {set_clause}'};
        DROP TABLE "{schema}"."{tmp}";
        '''
        res = con.execute(text(upsert_sql))
        # res.rowcount no siempre es confiable en INSERT ... SELECT, pero dejamos el flujo claro
    return len(df)
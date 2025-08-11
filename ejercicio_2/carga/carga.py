from sqlalchemy import create_engine, text
import pandas as pd

# ----- Parámetros fijos (este ejercicio) -----
XLSX_PATH = "data/customers.xlsx"   # coloca aquí tu archivo
SCHEMA = "public"
TABLE = "customers"

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB   = "app_db"
PG_USER = "postgres"
PG_PWD  = "postgres"
# ---------------------------------------------

engine = create_engine(f"postgresql+psycopg://{PG_USER}:{PG_PWD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

print(f"📥 Leyendo Excel: {XLSX_PATH}")
df = pd.read_excel(XLSX_PATH)  # si necesitas otra hoja: sheet_name="Hoja1"
df.columns = [str(c).strip().lower() for c in df.columns]

# Normalizaciones mínimas de ejemplo
if "id" not in df.columns:
    raise RuntimeError("Falta columna obligatoria 'id' en el Excel.")
if "name" not in df.columns and "nombre" in df.columns:
    df = df.rename(columns={"nombre": "name"})
if "phone" not in df.columns and "telefono" in df.columns:
    df = df.rename(columns={"telefono": "phone"})

with engine.begin() as con:
    # Crear esquema/tabla si no existen
    con.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
    con.execute(text(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE}" (
          id         BIGINT PRIMARY KEY,
          name       TEXT,
          phone      TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ
        );
    """))

    # Carga idempotente: volcamos a tabla temporal y hacemos UPSERT por id
    tmp = f"tmp_{TABLE}"
    df.to_sql(tmp, con, schema=SCHEMA, if_exists="replace", index=False)

    cols = [f'"{c}"' for c in df.columns]
    col_list = ", ".join(cols)
    updates = ", ".join([f'{c}=EXCLUDED.{c}' for c in cols if c != '"id"'])

    upsert_sql = f'''
      INSERT INTO "{SCHEMA}"."{TABLE}" ({col_list})
      SELECT {col_list} FROM "{SCHEMA}"."{tmp}"
      ON CONFLICT ("id") DO UPDATE SET {updates};
      DROP TABLE "{SCHEMA}"."{tmp}";
    '''
    con.execute(text(upsert_sql))

print("✅ Carga desde XLSX completa (idempotente por id).")
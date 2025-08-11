from utils.io import load_yaml, env, new_run_id, logging
from extract.api_client import fetch_api
from extract.read_files import read_csv
from validate.checks import assert_columns, assert_not_null, assert_unique
from transform.transforms import normalize_columns, cast_types, business_rules
from load.to_postgres import get_engine, upsert_dataframe
from utils.storage import save_staging

def main():
    run_id = new_run_id()
    logging.info(f"Run start: {run_id}")

    cfg = load_yaml("configs/sources.yaml")
    schema_cfg = load_yaml("configs/schema.yaml")

    # --- Extract (ejemplo: API + CSV) ---
    df_api = fetch_api(cfg["api"]["url"], params=cfg["api"].get("params"))
    path_data, path_meta = save_staging(df_api, base="resources/staging/api", run_id=run_id, source_name="api_main")
    df_csv = read_csv(cfg["files"]["path"])

    # --- Merge crudo ---
    df = df_api.merge(df_csv, how="outer", on=cfg["join_key"])

    # --- Validate ---
    assert_columns(df, schema_cfg["required_columns"])
    assert_not_null(df, schema_cfg["not_null"])
    assert_unique(df, [schema_cfg["unique_key"]])

    # --- Transform ---
    df = normalize_columns(df)
    df = cast_types(df, schema_cfg["dtypes"])
    df = business_rules(df)
    df["run_id"] = run_id

    # --- Load ---
    engine = get_engine(env("PG_USER"), env("PG_PWD"), env("PG_HOST"), env("PG_DB"))
    upsert_dataframe(df, engine, schema_cfg["target_table"], schema_cfg["unique_key"])

    logging.info(f"Run success: {run_id} rows={len(df)}")

if __name__ == "__main__":
    main()
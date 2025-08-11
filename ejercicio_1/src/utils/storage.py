from pathlib import Path
import pandas as pd, json, hashlib, time

def _md5_of_df(df: pd.DataFrame) -> str:
    # hash rápido sobre CSV en memoria (para detectar cambios)
    buf = df.to_csv(index=False).encode("utf-8")
    return hashlib.md5(buf).hexdigest()

def save_staging(df: pd.DataFrame, base="resources/staging/api", run_id="local",
                 ymd=None, source_name="api_main", fmt="parquet"):
    ymd = ymd or time.strftime("%Y-%m-%d")
    yyyy, mm, dd = ymd.split("-")
    folder = Path(base) / f"YYYY={yyyy}" / f"MM={mm}" / f"DD={dd}"
    folder.mkdir(parents=True, exist_ok=True)

    fname = f"data__{source_name}__{run_id}"
    data_path = folder / f"{fname}.{fmt}"
    meta_path = Path("resources/staging/meta") / f"{fname}.meta.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "parquet":
        df.to_parquet(data_path, index=False)
    else:
        df.to_csv(data_path, index=False)

    meta = {
        "run_id": run_id,
        "source": source_name,
        "rows": int(len(df)),
        "columns": list(df.columns),
        "checksum_md5": _md5_of_df(df),
        "stored_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data_path": str(data_path),
        "partition": {"YYYY": yyyy, "MM": mm, "DD": dd}
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return str(data_path), str(meta_path)
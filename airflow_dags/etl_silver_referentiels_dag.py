# pyright: reportMissingImports=false
"""
etl_silver_referentiels_dag.py  (v3 - Bronze Exact Match Version)
-------------------------------------------------------------------
Silver layer: Load referential data (regions, provinces, communes)
from OBS_RAW.raw.RAW_ROWS where dataset='referentiels'.

Key improvements:
- Reads only exact rel_paths for each CSV
- Unified helper for reading multiple rel_paths (accent-insensitive)
- Consistent cleaning, normalization, and logging across all levels
"""

import json
import logging
import re
import unicodedata
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from airflow import DAG
from airflow.operators.python import PythonOperator

import config
import db_utils

logger = logging.getLogger(__name__)

# =====================================================================
# HELPERS
# =====================================================================
def normalize_header(s: str) -> str:
    """Normalize header: ASCII, lowercase, underscores, no punctuation."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9a-z_]+", "", s)
    return s


def normalize_code(code_val) -> str:
    """Normalize administrative codes: strip, convert floats, uppercase."""
    if code_val is None or (isinstance(code_val, float) and pd.isna(code_val)):
        return ""
    s = str(code_val).strip()
    try:
        if "." in s:
            f = float(s)
            if f.is_integer():
                s = str(int(f))
    except Exception:
        pass
    return s.upper()


def has_letters(s: str) -> bool:
    """Check if string contains letters (Latin or accented)."""
    if s is None:
        return False
    s2 = unicodedata.normalize("NFKD", str(s))
    s2 = "".join(ch for ch in s2 if not unicodedata.combining(ch))
    return bool(re.search(r"[A-Za-z]", s2))


def first_col(df: pd.DataFrame, *candidates) -> str | None:
    """Find first column whose normalized name contains any candidate token."""
    cols = list(df.columns)
    for pat in candidates:
        pat_n = normalize_header(pat)
        for c in cols:
            if pat_n and pat_n in c:
                return c
    return None


def pick_name_column(df: pd.DataFrame, candidates: list[str], code_series: pd.Series) -> str | None:
    """Pick name column (has letters, not identical to code)."""
    for cand in candidates:
        c = first_col(df, cand)
        if not c:
            continue
        vals = df[c].astype(str)
        pct_letters = vals.apply(has_letters).mean()
        pct_equal_code = (
            vals.fillna("").str.strip().values ==
            code_series.fillna("").astype(str).str.strip().values
        ).mean()
        if pct_letters >= 0.5 and pct_equal_code < 0.5:
            return c
    for c in df.columns:
        vals = df[c].astype(str)
        if vals.apply(has_letters).mean() >= 0.5:
            return c
    return None


def read_bronze_by_relpaths(run_id: str, rel_paths: list[str]) -> pd.DataFrame:
    """
    Read rows from raw.RAW_ROWS for a set of exact rel_path candidates.
    Accent-insensitive comparison via COLLATE Latin1_General_CI_AI.
    Returns a normalized pandas DF from row_json.
    """
    if not rel_paths:
        return pd.DataFrame()

    engine = db_utils.make_sqlalchemy_engine(schema=config.RAW_DB)
    params = {"run_id": run_id}
    placeholders = []
    for i, p in enumerate(rel_paths):
        key = f"p{i}"
        placeholders.append(f":{key}")
        params[key] = p

    sql = text(f"""
        SELECT rel_path, row_index, row_json
        FROM raw.RAW_ROWS
        WHERE run_id = :run_id
          AND dataset = 'referentiels'
          AND rel_path COLLATE Latin1_General_CI_AI IN ({",".join(placeholders)})
        ORDER BY rel_path, row_index
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    if not rows:
        return pd.DataFrame()

    df_rows = pd.DataFrame(rows, columns=["rel_path", "row_index", "row_json"])
    payload = df_rows["row_json"].apply(lambda s: json.loads(s or "{}"))
    df = pd.json_normalize(payload).fillna("")
    df.columns = [normalize_header(c) for c in df.columns]
    return df


def get_latest_run_id() -> str:
    """Get latest run_id from raw.RAW_FILES."""
    engine = db_utils.make_sqlalchemy_engine(schema=config.RAW_DB)
    sql = "SELECT TOP 1 run_id FROM raw.RAW_FILES ORDER BY load_ts DESC"
    with engine.connect() as conn:
        row = conn.execute(text(sql)).first()
    if not row or not row[0]:
        raise ValueError("No run_id found in raw.RAW_FILES")
    return row[0]

# =====================================================================
# TASKS
# =====================================================================
def task_create_tables(**ctx):
    ddl = f"""
    USE [{config.STAGING_DB}];
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg');
    IF OBJECT_ID('stg.REGION_REF','U') IS NULL
        CREATE TABLE stg.REGION_REF(region_code NVARCHAR(50) PRIMARY KEY, region_name NVARCHAR(200), load_ts DATETIME2 DEFAULT SYSUTCDATETIME());
    IF OBJECT_ID('stg.PROVINCE_REF','U') IS NULL
        CREATE TABLE stg.PROVINCE_REF(province_code NVARCHAR(50) PRIMARY KEY, province_name NVARCHAR(200), region_code NVARCHAR(50), load_ts DATETIME2 DEFAULT SYSUTCDATETIME());
    IF OBJECT_ID('stg.COMMUNE_REF','U') IS NULL
        CREATE TABLE stg.COMMUNE_REF(commune_code NVARCHAR(50) PRIMARY KEY, commune_name NVARCHAR(200), province_code NVARCHAR(50), load_ts DATETIME2 DEFAULT SYSUTCDATETIME());
    """
    db_utils.execute_sql_with_transaction(ddl, schema=config.STAGING_DB)
    logger.info("✓ Created referential staging tables")


def task_load_regions(**ctx):
    run_id = get_latest_run_id()
    logger.info(f"Using run_id: {run_id}")

    candidates = [
        "referentiels/region.csv",
        "referentiels/Region.csv",
        "referentiels/région.csv",
        "referentiels/Région.csv",
    ]
    df = read_bronze_by_relpaths(run_id, candidates)
    if df.empty:
        logger.warning("⚠️ No rows found for region.csv in RAW_ROWS")
        ctx["ti"].xcom_push(key="regions", value=0)
        return

    logger.info(f"Columns detected: {list(df.columns)}")
    logger.info(f"First rows:\n{df.head(3).to_string()}")

    c_code = first_col(df, "code_region","cod_reg","codregion","region_code","code","id")
    if not c_code:
        raise ValueError(f"Region code column not found. Columns: {df.columns.tolist()}")

    code_series = df[c_code].apply(normalize_code)
    c_name = pick_name_column(df, ["noms_reg","nom_region","region","name","nom"], code_series)
    if not c_name:
        raise ValueError(f"Region name column not found. Columns: {df.columns.tolist()}")

    out = pd.DataFrame({
        "region_code": code_series,
        "region_name": df[c_name].astype(str).str.strip().str.upper(),
    }).drop_duplicates()
    out = out[(out["region_code"] != "") & (out["region_name"] != "")]

    logger.info(f"Sample regions cleaned:\n{out.head(12).to_string()}")

    engine = db_utils.make_sqlalchemy_engine(schema=config.STAGING_DB)
    db_utils.truncate_table(config.SILVER_REGION_REF, schema=config.STAGING_DB)
    out.to_sql(config.SILVER_REGION_REF.split(".")[-1], engine, schema="stg",
               if_exists="append", index=False, chunksize=config.BATCH_SIZE)

    ctx["ti"].xcom_push(key="regions", value=len(out))
    logger.info(f"✓ Loaded {len(out)} regions")


def task_load_provinces(**ctx):
    run_id = get_latest_run_id()
    logger.info(f"Using run_id: {run_id}")

    candidates = ["referentiels/province.csv", "referentiels/Province.csv"]
    df = read_bronze_by_relpaths(run_id, candidates)
    if df.empty:
        logger.warning("⚠️ No rows found for province.csv in RAW_ROWS")
        ctx["ti"].xcom_push(key="provinces", value=0)
        return

    logger.info(f"Columns detected: {list(df.columns)}")
    logger.info(f"First rows:\n{df.head(3).to_string()}")

    c_code = first_col(df, "code_province","cod_prov","province_code","code","id")
    if not c_code:
        raise ValueError(f"Province code column not found. Columns: {df.columns.tolist()}")
    prov_code = df[c_code].apply(normalize_code)

    c_name = pick_name_column(df, ["nom_province","province","lib_province","name","nom"], prov_code)
    if not c_name:
        raise ValueError(f"Province name column not found. Columns: {df.columns.tolist()}")

    c_reg = first_col(df, "code_region","cod_reg","region_code")
    region_code = df[c_reg].apply(normalize_code) if c_reg else pd.Series([""] * len(df))

    out = pd.DataFrame({
        "province_code": prov_code,
        "province_name": df[c_name].astype(str).str.strip().str.upper(),
        "region_code": region_code,
    }).drop_duplicates()
    out = out[(out["province_code"] != "") & (out["province_name"] != "")]

    logger.info(f"Sample provinces cleaned:\n{out.head(12).to_string()}")

    engine = db_utils.make_sqlalchemy_engine(schema=config.STAGING_DB)
    db_utils.truncate_table(config.SILVER_PROVINCE_REF, schema=config.STAGING_DB)
    out.to_sql(config.SILVER_PROVINCE_REF.split(".")[-1], engine, schema="stg",
               if_exists="append", index=False, chunksize=config.BATCH_SIZE)

    ctx["ti"].xcom_push(key="provinces", value=len(out))
    logger.info(f"✓ Loaded {len(out)} provinces")


def task_load_communes(**ctx):
    run_id = get_latest_run_id()
    logger.info(f"Using run_id: {run_id}")

    candidates = [
        "referentiels/commune.csv",
        "referentiels/Commune.csv",
        "referentiels/Commune (1).csv",
    ]
    df = read_bronze_by_relpaths(run_id, candidates)
    if df.empty:
        logger.warning("⚠️ No rows found for commune.csv in RAW_ROWS")
        ctx["ti"].xcom_push(key="communes", value=0)
        return

    logger.info(f"Columns detected: {list(df.columns)}")
    logger.info(f"First rows:\n{df.head(3).to_string()}")

    c_code = first_col(df, "code_commune","cod_commune","code_com","commune_code","code","id")
    if not c_code:
        raise ValueError(f"Commune code column not found. Columns: {df.columns.tolist()}")
    com_code = df[c_code].apply(normalize_code)

    c_name = pick_name_column(df, ["nom_commune","commune","lib_commune","name","nom"], com_code)
    if not c_name:
        raise ValueError(f"Commune name column not found. Columns: {df.columns.tolist()}")

    c_prov = first_col(df, "code_province","cod_prov","province_code")
    prov_code = df[c_prov].apply(normalize_code) if c_prov else pd.Series([""] * len(df))

    out = pd.DataFrame({
        "commune_code": com_code,
        "commune_name": df[c_name].astype(str).str.strip().str.upper(),
        "province_code": prov_code,
    })

    missing = out["province_code"].astype(str).str.strip().eq("") | out["province_code"].isna()
    if missing.any():
        out.loc[missing, "province_code"] = out.loc[missing, "commune_code"].str[:5]
        still = out["province_code"].astype(str).str.strip().eq("") | out["province_code"].isna()
        out.loc[still, "province_code"] = out.loc[still, "commune_code"].str[:4]

    out = out[(out["commune_code"] != "") & (out["commune_name"] != "")].drop_duplicates()

    logger.info(f"Sample communes cleaned:\n{out.head(12).to_string()}")

    engine = db_utils.make_sqlalchemy_engine(schema=config.STAGING_DB)
    db_utils.truncate_table(config.SILVER_COMMUNE_REF, schema=config.STAGING_DB)
    out.to_sql(config.SILVER_COMMUNE_REF.split(".")[-1], engine, schema="stg",
               if_exists="append", index=False, chunksize=config.BATCH_SIZE)

    ctx["ti"].xcom_push(key="communes", value=len(out))
    logger.info(f"✓ Loaded {len(out)} communes")


def task_patch_fk_codes(**ctx):
    sql = f"""
    USE [{config.STAGING_DB}];
    UPDATE p
       SET p.region_code = LEFT(p.province_code, 2)
      FROM stg.PROVINCE_REF p
     WHERE (p.region_code IS NULL OR p.region_code = '')
       AND p.province_code IS NOT NULL;

    UPDATE c
       SET c.province_code = LEFT(c.commune_code, 5)
      FROM stg.COMMUNE_REF c
     WHERE (c.province_code IS NULL OR c.province_code = '')
       AND c.commune_code IS NOT NULL AND LEN(c.commune_code) >= 5;
    """
    db_utils.execute_sql_with_transaction(sql, schema=config.STAGING_DB)
    logger.info("✓ Patched FK hierarchy codes")


def task_report(**ctx):
    r = ctx["ti"].xcom_pull(key="regions", task_ids="task_load_regions") or 0
    p = ctx["ti"].xcom_pull(key="provinces", task_ids="task_load_provinces") or 0
    c = ctx["ti"].xcom_pull(key="communes", task_ids="task_load_communes") or 0
    logger.info(f"Final counts -> Regions={r}, Provinces={p}, Communes={c}")
    if r == 0 or p == 0 or c == 0:
        raise ValueError("Some referentials not loaded. Check RAW_FILES and column mapping.")

# =====================================================================
# DAG
# =====================================================================
with DAG(
    dag_id="etl_silver_referentiels",
    description="Silver layer: Load referential data (regions, provinces, communes) from Bronze",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1},
    tags=config.DAG_TAGS["silver"],
) as dag:

    t_create = PythonOperator(task_id="task_create_tables", python_callable=task_create_tables)
    t_regions = PythonOperator(task_id="task_load_regions", python_callable=task_load_regions)
    t_provinces = PythonOperator(task_id="task_load_provinces", python_callable=task_load_provinces)
    t_communes = PythonOperator(task_id="task_load_communes", python_callable=task_load_communes)
    t_patch = PythonOperator(task_id="task_patch_fk_codes", python_callable=task_patch_fk_codes)
    t_report = PythonOperator(task_id="task_report", python_callable=task_report)

    t_create >> [t_regions, t_provinces, t_communes] >> t_patch >> t_report

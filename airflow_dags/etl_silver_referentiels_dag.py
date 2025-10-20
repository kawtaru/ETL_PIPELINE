# pyright: reportMissingImports=false
from datetime import datetime
import json, re, os, unicodedata
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Compat: SqlHook may not exist depending on provider versions. Fallback to MsSqlHook or DbApiHook.
try:
    from airflow.providers.common.sql.hooks.sql import SqlHook  # Airflow >= 2.5 common SQL provider
except Exception:  # pragma: no cover - environment dependent
    try:
        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook as SqlHook  # MSSQL provider
    except Exception:
        try:
            from airflow.providers.common.sql.hooks.sql import DbApiHook as SqlHook  # older common provider
        except Exception:
            from airflow.hooks.dbapi import DbApiHook as SqlHook  # core fallback

STAGING_DB = os.environ.get("ETL_STAGING_DB", "OBS_STAGING")

DDL = r"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg');

IF OBJECT_ID('stg.REGION_REF','U') IS NULL
CREATE TABLE stg.REGION_REF(
  region_code NVARCHAR(50) NOT NULL,
  region_name NVARCHAR(200) NOT NULL,
  load_ts     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_REGION_REF PRIMARY KEY(region_code)
);

IF OBJECT_ID('stg.PROVINCE_REF','U') IS NULL
CREATE TABLE stg.PROVINCE_REF(
  province_code NVARCHAR(50) NOT NULL,
  province_name NVARCHAR(200) NOT NULL,
  region_code   NVARCHAR(50) NULL,
  load_ts       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_PROVINCE_REF PRIMARY KEY(province_code)
);

IF OBJECT_ID('stg.COMMUNE_REF','U') IS NULL
CREATE TABLE stg.COMMUNE_REF(
  commune_code  NVARCHAR(50) NOT NULL,
  commune_name  NVARCHAR(200) NOT NULL,
  province_code NVARCHAR(50) NULL,
  load_ts       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_COMMUNE_REF PRIMARY KEY(commune_code)
);
"""

# SQL backfill to derive missing foreign keys from code prefixes
BACKFILL_CODES = r"""
-- Provinces: derive region_code from province_code prefix
UPDATE p
SET region_code = CASE
    WHEN LEFT(p.province_code,2) IN ('10','11','12') THEN LEFT(p.province_code,2)
    ELSE LEFT(p.province_code,1)
  END
FROM stg.PROVINCE_REF p
WHERE (p.region_code IS NULL OR LTRIM(RTRIM(p.region_code))='')
  AND NULLIF(LTRIM(RTRIM(p.province_code)),'') IS NOT NULL;

-- Communes: prefer 5-digit match then 4-digit match to a known province
UPDATE c
SET province_code = LEFT(c.commune_code,5)
FROM stg.COMMUNE_REF c
JOIN stg.PROVINCE_REF p ON p.province_code = LEFT(c.commune_code,5)
WHERE (c.province_code IS NULL OR LTRIM(RTRIM(c.province_code))='')
  AND NULLIF(LTRIM(RTRIM(c.commune_code)),'') IS NOT NULL;

UPDATE c
SET province_code = LEFT(c.commune_code,4)
FROM stg.COMMUNE_REF c
JOIN stg.PROVINCE_REF p ON p.province_code = LEFT(c.commune_code,4)
WHERE (c.province_code IS NULL OR LTRIM(RTRIM(c.province_code))='')
  AND NULLIF(LTRIM(RTRIM(c.commune_code)),'') IS NOT NULL;
"""

# Ensure nullability on existing tables created before the NULL change
DDL_ALTER_NULLS = r"""
IF OBJECT_ID('stg.PROVINCE_REF','U') IS NOT NULL AND EXISTS(
  SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('stg.PROVINCE_REF') AND name='region_code' AND is_nullable=0)
  ALTER TABLE stg.PROVINCE_REF ALTER COLUMN region_code NVARCHAR(50) NULL;

IF OBJECT_ID('stg.COMMUNE_REF','U') IS NOT NULL AND EXISTS(
  SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('stg.COMMUNE_REF') AND name='province_code' AND is_nullable=0)
  ALTER TABLE stg.COMMUNE_REF ALTER COLUMN province_code NVARCHAR(50) NULL;
"""

def _latest_run_id(hook: SqlHook) -> str:
    rid = hook.get_first("SELECT TOP 1 run_id FROM raw.RAW_FILES ORDER BY load_ts DESC;")[0]
    if not rid:
        raise ValueError("No run_id found")
    return rid

def _read_dataset(hook: SqlHook, run_id: str, filename_pattern: str) -> pd.DataFrame:
    sql = """
    SELECT rel_path, row_index, row_json
    FROM raw.RAW_ROWS
    WHERE run_id = %(r)s AND dataset='referentiels' AND rel_path LIKE %(pat)s
    ORDER BY rel_path, row_index
    """
    rows = hook.get_pandas_df(sql, parameters={"r": run_id, "pat": filename_pattern})
    if rows.empty:
        return rows
    payload = rows["row_json"].apply(lambda s: json.loads(s or "{}"))
    df = pd.json_normalize(payload)

    def norm2(x: str) -> str:
        s = unicodedata.normalize('NFKD', str(x))
        s = ''.join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower().strip()
        s = re.sub(r"\s+", "_", s)
        return re.sub(r"[^0-9a-z_]+", "", s)

    df.columns = [norm2(str(c)) for c in df.columns]
    return df

def _first_col(df: pd.DataFrame, *cands):
    for c in df.columns:
        for pat in cands:
            if pat in c:
                return c
    return None

def create_tables(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    hook.run(DDL)
    hook.run(DDL_ALTER_NULLS)

def load_regions(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    run_id = _latest_run_id(hook)
    df = _read_dataset(hook, run_id, "%region%.csv%")
    if df.empty:
        ctx["ti"].xcom_push(key="regions", value=0); return
    c_code = _first_col(df, "code", "id")
    c_name = _first_col(df, "nom", "name", "region")
    if not (c_code and c_name):
        raise ValueError("Region columns not found")

    out = pd.DataFrame({
        "region_code": df[c_code].astype(str).str.strip().str.upper(),
        "region_name": df[c_name].astype(str).str.strip().str.upper(),
    }).dropna().drop_duplicates()

    out = out[(out["region_code"]!="") & (out["region_name"]!="")]
    engine = hook.get_sqlalchemy_engine()
    hook.run("TRUNCATE TABLE stg.REGION_REF;")
    out.to_sql("REGION_REF", engine, schema="stg", if_exists="append", index=False, method="multi", chunksize=1000)
    ctx["ti"].xcom_push(key="regions", value=int(len(out)))

def load_provinces(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    run_id = _latest_run_id(hook)
    df = _read_dataset(hook, run_id, "%provinc%.csv%")
    if df.empty:
        ctx["ti"].xcom_push(key="provinces", value=0); return

    c_code = _first_col(df, "code_prov", "code", "id")
    c_name = _first_col(df, "nom_prov", "nom", "name", "province")
    c_reg  = _first_col(df, "code_reg", "region", "reg")
    if not (c_code and c_name):
        raise ValueError("Province columns not found: code/name")

    region_series = df[c_reg].astype(str).str.strip().str.upper() if c_reg else pd.Series([None] * len(df))
    out = pd.DataFrame({
        "province_code": df[c_code].astype(str).str.strip().str.upper(),
        "province_name": df[c_name].astype(str).str.strip().str.upper(),
        "region_code":   region_series,
    }).dropna(subset=["province_code","province_name"]).drop_duplicates()

    out = out[(out["province_code"]!="") & (out["province_name"]!="")]
    engine = hook.get_sqlalchemy_engine()
    hook.run("TRUNCATE TABLE stg.PROVINCE_REF;")
    out.to_sql("PROVINCE_REF", engine, schema="stg", if_exists="append", index=False, method="multi", chunksize=1000)
    ctx["ti"].xcom_push(key="provinces", value=int(len(out)))

def load_communes(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    run_id = _latest_run_id(hook)
    df = _read_dataset(hook, run_id, "%commune%.csv%")
    if df.empty:
        ctx["ti"].xcom_push(key="communes", value=0); return

    c_code = _first_col(df, "code_com", "code", "id")
    c_name = _first_col(df, "nom_com", "nom", "name", "commune")
    c_prov = _first_col(df, "code_prov", "province", "prov")
    if not (c_code and c_name):
        raise ValueError("Commune columns not found: code/name")

    province_series = df[c_prov].astype(str).str.strip().str.upper() if c_prov else pd.Series([None] * len(df))
    out = pd.DataFrame({
        "commune_code":  df[c_code].astype(str).str.strip().str.upper(),
        "commune_name":  df[c_name].astype(str).str.strip().str.upper(),
        "province_code": province_series,
    }).dropna(subset=["commune_code","commune_name"]).drop_duplicates()

    out = out[(out["commune_code"]!="") & (out["commune_name"]!="")]
    engine = hook.get_sqlalchemy_engine()
    hook.run("TRUNCATE TABLE stg.COMMUNE_REF;")
    out.to_sql("COMMUNE_REF", engine, schema="stg", if_exists="append", index=False, method="multi", chunksize=1000)
    ctx["ti"].xcom_push(key="communes", value=int(len(out)))

# === Algorithmic FK derivation ===
def patch_fk_with_hierarchical_codes(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)

    # Derive region_code from province_code (1- or 2-digit prefix rule)
    hook.run("""
      UPDATE p
      SET p.region_code = CASE
          WHEN LEFT(p.province_code,2) IN ('10','11','12') THEN LEFT(p.province_code,2)
          ELSE LEFT(p.province_code,1)
      END
      FROM stg.PROVINCE_REF p
      WHERE (p.region_code IS NULL OR p.region_code='')
        AND p.province_code IS NOT NULL AND p.province_code<>'';
    """)

    # Commune → Province via 5-digit prefix
    hook.run("""
      UPDATE c
      SET c.province_code = p.province_code
      FROM stg.COMMUNE_REF c
      JOIN stg.PROVINCE_REF p ON p.province_code = LEFT(c.commune_code,5)
      WHERE (c.province_code IS NULL OR c.province_code='')
        AND c.commune_code IS NOT NULL AND c.commune_code<>'';
    """)

    # Commune → Province via 4-digit prefix fallback
    hook.run("""
      UPDATE c
      SET c.province_code = p.province_code
      FROM stg.COMMUNE_REF c
      JOIN stg.PROVINCE_REF p ON p.province_code = LEFT(c.commune_code,4)
      WHERE (c.province_code IS NULL OR c.province_code='')
        AND c.commune_code IS NOT NULL AND c.commune_code<>'';
    """)

def report(**ctx):
    r = ctx["ti"].xcom_pull(key="regions",   task_ids="load_regions")   or 0
    p = ctx["ti"].xcom_pull(key="provinces", task_ids="load_provinces") or 0
    c = ctx["ti"].xcom_pull(key="communes",  task_ids="load_communes")  or 0
    print(f"[SILVER/REF] rows -> regions={r}, provinces={p}, communes={c}")
    if r == p == c == 0:
        raise ValueError("No referential rows produced. Check column mapping or file names.")

with DAG(
    "etl_silver_referentiels",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    default_args={"owner":"data-eng"},
    tags=["observatoire","silver","referentiels"],
) as dag:
    t_ddl = PythonOperator(task_id="create_tables", python_callable=create_tables)
    t_reg = PythonOperator(task_id="load_regions",  python_callable=load_regions)
    t_pro = PythonOperator(task_id="load_provinces",python_callable=load_provinces)
    t_com = PythonOperator(task_id="load_communes", python_callable=load_communes)
    t_fix = SQLExecuteQueryOperator(
        task_id="backfill_missing_codes",
        conn_id="mssql_default",
        hook_params={"schema": STAGING_DB},
        autocommit=True,
        sql=BACKFILL_CODES,
    )
    t_rep = PythonOperator(task_id="report", python_callable=report)

    t_ddl >> [t_reg, t_pro, t_com] >> t_fix >> t_rep

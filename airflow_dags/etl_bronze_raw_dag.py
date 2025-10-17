# pyright: reportMissingImports=false
# airflow_dags/etl_bronze_raw_dag.py
from datetime import datetime
import os, glob, zipfile, hashlib, json, shutil
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Compat: SqlHook may not exist on some provider versions. Build a portable factory.
try:
    from airflow.providers.common.sql.hooks.sql import SqlHook as _SqlHookBase
    _HAS_COMMON_SQLHOOK = True
except Exception:
    _SqlHookBase = None
    _HAS_COMMON_SQLHOOK = False
try:
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook as _MsSqlHook
    _HAS_MSSQLHOOK = True
except Exception:
    _MsSqlHook = None
    _HAS_MSSQLHOOK = False

from airflow.hooks.base import BaseHook
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.exc import InterfaceError

def copy_referentiels_to_landing(**ctx):
    run_dir = ctx["ti"].xcom_pull(key="landing_dir", task_ids="unzip_to_landing")
    # Use module-level imports; avoid shadowing with local imports
    if not os.path.isdir(REF_DIR):
        return
    dst = os.path.join(run_dir, "referentiels")
    os.makedirs(dst, exist_ok=True)
    for p in glob.glob(os.path.join(REF_DIR, "*.csv")):
        shutil.copy2(p, os.path.join(dst, os.path.basename(p)))
        
class _EngineWrapper:
    def __init__(self, engine):
        self._engine = engine
    def get_sqlalchemy_engine(self):
        return self._engine
    def run(self, sql, parameters=None):
        with self._engine.begin() as conn:
            if isinstance(sql, str):
                conn.execute(text(sql), parameters or {})
            else:
                for stmt in sql:
                    conn.execute(text(stmt), parameters or {})

def _make_engine_from_conn(conn_id: str, schema: str | None = None):
    conn = BaseHook.get_connection(conn_id)
    host = conn.host
    port = conn.port or 1433
    user = conn.login or ""
    pwd  = conn.get_password() or ""
    db   = schema or conn.schema or ""
    extras = {}
    try:
        extras = conn.extra_dejson or {}
    except Exception:
        extras = {}
    driver_pref = extras.get("odbc_driver") or extras.get("driver")
    encrypt = str(extras.get("Encrypt", "yes")).lower()
    trust   = str(extras.get("TrustServerCertificate", "yes")).lower()
    candidates = [d for d in [driver_pref, "ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"] if d]
    for drv in candidates:
        try:
            params = f"driver={quote_plus(drv)}&Encrypt={encrypt}&TrustServerCertificate={trust}"
            url = f"mssql+pyodbc://{quote_plus(user)}:{quote_plus(pwd)}@{host}:{port}/{quote_plus(db)}?{params}"
            eng = create_engine(url, fast_executemany=True)
            with eng.connect() as _:
                pass
            return eng
        except Exception:
            continue
    # fallback to pymssql
    url = f"mssql+pymssql://{quote_plus(user)}:{quote_plus(pwd)}@{host}:{port}/{quote_plus(db)}"
    return create_engine(url)

def make_sql_hook(conn_id: str, schema: str | None = None):
    if _HAS_COMMON_SQLHOOK and _SqlHookBase is not None:
        return _SqlHookBase(conn_id=conn_id, schema=schema)
    if _HAS_MSSQLHOOK and _MsSqlHook is not None:
        return _MsSqlHook(mssql_conn_id=conn_id, schema=schema)
    # Final fallback: minimal wrapper around a SQLAlchemy engine
    return _EngineWrapper(_make_engine_from_conn(conn_id, schema))

# ---------- Config ----------
BASE_DIR = os.environ.get("ETL_BASE_DIR", "/opt/etl/data")
RAW_DIR  = os.path.join(BASE_DIR, "raw", "prix_indices")     # preferred drop location for the ZIP
LAND_DIR = os.path.join(BASE_DIR, "landing", "bronze_raw")   # unzip target per run
ARCH_DIR = os.path.join(BASE_DIR, "archive", "bronze_raw")   # archives

STAGING_DB = "OBS_STAGING"   # <-- bronze/staging database (separate from OBSERVATOIRE)

# Optional referentials folder alongside raw data
REF_DIR = os.path.join(BASE_DIR, "referentiels")  # where your region/province/commune live

# ---------- Helpers ----------
def _md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _dataset_from_path(path: str) -> str:
    p = path.lower()
    if "ponderation" in p or "pond" in p:
        return "ponderations"
    if "region" in p or "province" in p or "commune" in p or "referent" in p:
        return "referentiels"
    return "prix_indices"

# ---------- Python tasks ----------
def detect_zip(**ctx):
    # Be tolerant to singular folder name used locally: raw/prix_indice
    search_dirs = [
        RAW_DIR,
        os.path.join(BASE_DIR, "raw", "prix_indice"),
    ]
    zips = []
    checked = []
    for d in search_dirs:
        checked.append(d)
        zips.extend(sorted(glob.glob(os.path.join(d, "*.zip"))))
    if not zips:
        raise FileNotFoundError(f"No ZIP found in any of: {', '.join(checked)}")
    # pick the latest by name sort (common for dated filenames)
    zips = sorted(zips)
    src_zip = zips[-1]
    ctx["ti"].xcom_push(key="src_zip", value=src_zip)

def unzip_to_landing(**ctx):
    src_zip = ctx["ti"].xcom_pull(key="src_zip", task_ids="detect_zip")
    run_id = ctx["run_id"].replace(":", "_").lower()
    run_dir = os.path.join(LAND_DIR, run_id)
    os.makedirs(run_dir, exist_ok=True)
    with zipfile.ZipFile(src_zip, "r") as zf:
        zf.extractall(run_dir)
    ctx["ti"].xcom_push(key="landing_dir", value=run_dir)

def stage_clear_run(**ctx):
    """Idempotent: clears only the current run_id from raw tables."""
    run_id = ctx["run_id"].replace(":", "_").lower()
    hook = make_sql_hook(conn_id="mssql_default", schema=STAGING_DB)
    hook.run("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='raw') EXEC('CREATE SCHEMA raw');
    IF OBJECT_ID('raw.RAW_FILES','U') IS NULL
    CREATE TABLE raw.RAW_FILES(
      run_id     NVARCHAR(200) NOT NULL,
      rel_path   NVARCHAR(400) NOT NULL,
      dataset    NVARCHAR(100) NOT NULL,
      size_bytes BIGINT        NOT NULL,
      md5        NVARCHAR(64)  NOT NULL,
      n_rows     INT           NOT NULL,
      load_ts    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
      CONSTRAINT PK_RAW_FILES PRIMARY KEY (run_id, rel_path)
    );
    IF OBJECT_ID('raw.RAW_ROWS','U') IS NULL
    CREATE TABLE raw.RAW_ROWS(
      run_id     NVARCHAR(200) NOT NULL,
      rel_path   NVARCHAR(400) NOT NULL,
      dataset    NVARCHAR(100) NOT NULL,
      row_index  INT           NOT NULL,
      row_json   NVARCHAR(MAX) NOT NULL,
      load_ts    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
      CONSTRAINT PK_RAW_ROWS PRIMARY KEY (run_id, rel_path, row_index)
    );
    DELETE FROM raw.RAW_ROWS  WHERE run_id = %(r)s;
    DELETE FROM raw.RAW_FILES WHERE run_id = %(r)s;
    """, parameters={"r": run_id})

def stage_load_raw(**ctx):
    """Read all CSVs → raw.RAW_FILES + raw.RAW_ROWS (1 JSON row per CSV line)."""
    run_id = ctx["run_id"].replace(":", "_").lower()
    landing_dir = ctx["ti"].xcom_pull(key="landing_dir", task_ids="unzip_to_landing")
    hook = make_sql_hook(conn_id="mssql_default", schema=STAGING_DB)
    engine = hook.get_sqlalchemy_engine()

    files_meta = []
    row_frames = []

    csv_paths = sorted(Path(landing_dir).rglob("*.csv"))
    print(f"Found {len(csv_paths)} CSV files under {landing_dir}")
    for p in csv_paths:
        p = str(p)
        rel = os.path.relpath(p, landing_dir).replace("\\", "/")

        # robust CSV read (utf-8 → cp1252 fallback)
        try:
            df = pd.read_csv(p, sep=";", dtype=str, encoding="utf-8", keep_default_na=False, na_values=[])
        except Exception:
            df = pd.read_csv(p, sep=";", dtype=str, encoding="cp1252", keep_default_na=False, na_values=[])

        files_meta.append({
            "run_id": run_id,
            "rel_path": rel,
            "dataset": _dataset_from_path(rel),
            "size_bytes": os.path.getsize(p),
            "md5": _md5_file(p),
            "n_rows": int(df.shape[0]),
        })
        if df.empty:
            continue

        # 1 JSON by row (keys/values as strings)
        js = df.fillna("").apply(
            lambda r: json.dumps({str(k): str(v) for k, v in r.items()}, ensure_ascii=False), axis=1
        )
        row_frames.append(pd.DataFrame({
            "run_id": run_id,
            "rel_path": rel,
            "dataset": _dataset_from_path(rel),
            "row_index": range(1, len(df) + 1),
            "row_json": js.astype(str),
        }))
        print(f"[{len(files_meta)}/{len(csv_paths)}] {rel}: {df.shape[0]} rows queued")

    # bulk insert (inventory)
    if files_meta:
        inv = pd.DataFrame(files_meta)
        inv.to_sql("RAW_FILES", engine, schema="raw", if_exists="append", index=False, method="multi", chunksize=1000)
        print(f"Inserted RAW_FILES: {len(inv)}")

    # bulk insert (rows)
    if row_frames:
        total_rows = sum(int(m.get("n_rows", 0)) for m in files_meta)
        print(f"Inserting RAW_ROWS: {total_rows} rows across {len(row_frames)} files")
        all_rows = pd.concat(row_frames, ignore_index=True)
        all_rows.to_sql("RAW_ROWS", engine, schema="raw", if_exists="append", index=False, method="multi", chunksize=1000)
        print("Inserted RAW_ROWS")

def archive_inputs(**ctx):
    src_zip     = ctx["ti"].xcom_pull(key="src_zip", task_ids="detect_zip")
    landing_dir = ctx["ti"].xcom_pull(key="landing_dir", task_ids="unzip_to_landing")
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    os.makedirs(ARCH_DIR, exist_ok=True)
    # move original ZIP
    shutil.move(src_zip, os.path.join(ARCH_DIR, f"{ts}__{os.path.basename(src_zip)}"))
    # zip landing content
    shutil.make_archive(os.path.join(ARCH_DIR, f"{ts}__landing"), "zip", root_dir=landing_dir)

# ---------- DAG ----------
with DAG(
    dag_id="etl_bronze_raw",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng"},
    tags=["observatoire", "bronze", "raw", "staging-db"],
) as dag:

    # Optional: ensure DB exists (harmless if it already exists)
    ddl_db = SQLExecuteQueryOperator(
        task_id="ensure_obs_staging_db",
        conn_id="mssql_default",
        hook_params={"schema": "master"},
        autocommit=True,
        sql="IF DB_ID(N'OBS_STAGING') IS NULL CREATE DATABASE [OBS_STAGING];",
    )

    t_detect = PythonOperator(task_id="detect_zip", python_callable=detect_zip)
    t_unzip  = PythonOperator(task_id="unzip_to_landing", python_callable=unzip_to_landing)
    t_clear  = PythonOperator(task_id="stage_clear_run", python_callable=stage_clear_run)
    t_load   = PythonOperator(task_id="stage_load_raw", python_callable=stage_load_raw)
    t_arch   = PythonOperator(task_id="archive_inputs", python_callable=archive_inputs)

t_copy_refs = PythonOperator(task_id="copy_referentiels_to_landing",
                             python_callable=copy_referentiels_to_landing)

ddl_db >> t_detect >> t_unzip >> t_copy_refs >> t_clear >> t_load >> t_arch

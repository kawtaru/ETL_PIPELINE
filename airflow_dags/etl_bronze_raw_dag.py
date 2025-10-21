# pyright: reportMissingImports=false
# airflow_dags/etl_bronze_raw_dag.py

from datetime import datetime
import os, glob, zipfile, hashlib, json, shutil
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Optional hooks (gracefully fallback to raw SQLAlchemy if provider differs)
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


# =========================
# Engine wrapper / Hook shim
# =========================
class _EngineWrapper:
    def __init__(self, engine):   # FIX: correct __init__
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

    # detect driver & TLS extras
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
            # quick connectivity check
            with eng.connect() as _:
                pass
            return eng
        except Exception:
            continue

    # last resort (pymssql)
    url = f"mssql+pymssql://{quote_plus(user)}:{quote_plus(pwd)}@{host}:{port}/{quote_plus(db)}"
    return create_engine(url)


def make_sql_hook(conn_id: str, schema: str | None = None):
    if _HAS_COMMON_SQLHOOK and _SqlHookBase is not None:
        return _SqlHookBase(conn_id=conn_id, schema=schema)
    if _HAS_MSSQLHOOK and _MsSqlHook is not None:
        return _MsSqlHook(mssql_conn_id=conn_id, schema=schema)
    return _EngineWrapper(_make_engine_from_conn(conn_id, schema))


# =========================
# Config
# =========================
BASE_DIR   = os.environ.get("ETL_BASE_DIR", "/opt/etl/data")
RAW_DIR    = os.path.join(BASE_DIR, "raw", "prix_indices")
ALT_RAW    = os.path.join(BASE_DIR, "raw", "prix_indice")  # sometimes spelled without 's'
LAND_DIR   = os.path.join(BASE_DIR, "landing", "bronze_raw")
ARCH_DIR   = os.path.join(BASE_DIR, "archive", "bronze_raw")
REF_DIR    = os.path.join(BASE_DIR, "referentiels")

STAGING_DB = "OBS_STAGING"


# =========================
# Helpers
# =========================
def _md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _dataset_from_path(path: str) -> str:
    """Classify dataset type from relative file path."""
    p = path.lower()
    if "ponderation" in p or "pond" in p:
        return "ponderations"
    if "referentiel" in p or "commune" in p or "province" in p or "region.csv" in p:
        return "referentiels"
    return "prix_indices"


def _parse_file_type(filename: str) -> str:
    """Determine if file is T1 (prix par variété) or T3 (indice par produit)."""
    fn = filename.lower()
    if "_t1" in fn or "t1." in fn:
        return "T1"
    if "_t3" in fn or "t3." in fn:
        return "T3"
    return "UNKNOWN"


def _extract_ville_from_path(rel_path: str) -> str | None:
    """
    Extract city (agglomération) from the folder directly before the file.
    Expected: .../region-<REGION>_CSV/<VILLE>/<VILLE>_T1.csv
    """
    if not rel_path:
        return None
    parts = [x for x in rel_path.replace("\\", "/").split("/") if x]
    return parts[-2] if len(parts) >= 2 else None


def _normalize_region_token(val: str | None) -> str:
    if not val:
        return ""
    return " ".join(str(val).lower().strip().split())


# =========================
# Python tasks
# =========================
def detect_zip(**ctx):
    search_dirs = [RAW_DIR, ALT_RAW]
    zips = []
    for d in search_dirs:
        if os.path.exists(d):
            zips.extend(sorted(glob.glob(os.path.join(d, "*.zip"))))
    if not zips:
        raise FileNotFoundError(f"No ZIP found in: {search_dirs}")
    src_zip = sorted(zips)[-1]  # most recent
    ctx["ti"].xcom_push(key="src_zip", value=src_zip)
    print(f"Detected ZIP: {src_zip}")


def unzip_to_landing(**ctx):
    src_zip = ctx["ti"].xcom_pull(key="src_zip", task_ids="detect_zip")
    run_id = ctx["run_id"].replace(":", "_").lower()
    run_dir = os.path.join(LAND_DIR, run_id)
    os.makedirs(run_dir, exist_ok=True)
    with zipfile.ZipFile(src_zip, "r") as zf:
        zf.extractall(run_dir)
    ctx["ti"].xcom_push(key="landing_dir", value=run_dir)
    print(f"Extracted to: {run_dir}")


def copy_referentiels_to_landing(**ctx):
    run_dir = ctx["ti"].xcom_pull(key="landing_dir", task_ids="unzip_to_landing")
    if not os.path.isdir(REF_DIR):
        print(f"WARN: referentials directory not found: {REF_DIR}")
        return
    dst = os.path.join(run_dir, "referentiels")
    os.makedirs(dst, exist_ok=True)
    count = 0
    for p in glob.glob(os.path.join(REF_DIR, "*.csv")):
        shutil.copy2(p, os.path.join(dst, os.path.basename(p)))
        count += 1
    print(f"Copied {count} referential files to {dst}")


def stage_clear_run(**ctx):
    """Create schemas/tables and clear existing rows for this run."""
    run_id = ctx["run_id"].replace(":", "_").lower()
    hook = make_sql_hook(conn_id="mssql_default", schema=STAGING_DB)

    ddl = """
    -- Schemas
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='raw') EXEC('CREATE SCHEMA raw');
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg');

    -- Raw inventory
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

    -- Staging table for prix/indices (multi-niveau) — NO 'mois'
    IF OBJECT_ID('stg.PRIX_INDICE_RAW','U') IS NOT NULL DROP TABLE stg.PRIX_INDICE_RAW;
    CREATE TABLE stg.PRIX_INDICE_RAW(
      -- Granularité
      niveau         NVARCHAR(16)  NULL,    -- 'VILLE' | 'REGION' | 'NATIONAL'
      region         NVARCHAR(200) NULL,    -- filled only for niveau='REGION'
      agglomeration  NVARCHAR(200) NULL,    -- filled only for niveau='VILLE'
      -- Métier
      corps          NVARCHAR(200) NULL,
      activite       NVARCHAR(200) NULL,
      produit        NVARCHAR(200) NULL,
      variete        NVARCHAR(200) NULL,
      -- Temps + mesures
      annee          INT           NOT NULL,
      prix_ttc       DECIMAL(18,6) NULL,
      indice         DECIMAL(18,6) NULL,
      -- Traçabilité
      file_type      NVARCHAR(20)  NULL,
      source_file    NVARCHAR(400) NULL
    );

    -- Clear current run inventories
    DELETE FROM raw.RAW_ROWS  WHERE run_id = %(r)s;
    DELETE FROM raw.RAW_FILES WHERE run_id = %(r)s;
    """
    hook.run(ddl, parameters={"r": run_id})
    print(f"Tables created/cleared for run_id: {run_id}")


def stage_load_raw(**ctx):
    """Load all CSV files → raw.RAW_FILES + raw.RAW_ROWS."""
    run_id = ctx["run_id"].replace(":", "_").lower()
    landing_dir = ctx["ti"].xcom_pull(key="landing_dir", task_ids="unzip_to_landing")

    hook = make_sql_hook(conn_id="mssql_default", schema=STAGING_DB)
    engine = hook.get_sqlalchemy_engine()

    files_meta = []
    row_frames = []

    csv_paths = sorted(Path(landing_dir).rglob("*.csv"))
    print(f"Found {len(csv_paths)} CSV files under {landing_dir}")

    for i, p in enumerate(csv_paths, start=1):
        p = str(p)
        rel = os.path.relpath(p, landing_dir).replace("\\", "/")

        # Read robustly (encoding variations)
        df = None
        for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
            try:
                df = pd.read_csv(p, sep=";", dtype=str, encoding=enc, keep_default_na=False, na_values=[])
                break
            except Exception:
                continue
        if df is None:
            print(f"ERROR: Could not read {rel} with supported encodings")
            continue

        files_meta.append({
            "run_id": run_id,
            "rel_path": rel,
            "dataset": _dataset_from_path(rel),
            "size_bytes": os.path.getsize(p),
            "md5": _md5_file(p),
            "n_rows": int(df.shape[0]),
        })

        if df.empty:
            print(f"[{i}/{len(csv_paths)}] {rel}: 0 rows (skipped)")
            continue

        # JSON-ify each row preserving UTF-8
        js = df.fillna("").apply(
            lambda r: json.dumps({str(k): (str(v) if v is not None else "") for k, v in r.items()},
                                 ensure_ascii=False), axis=1
        )
        row_frames.append(pd.DataFrame({
            "run_id": run_id,
            "rel_path": rel,
            "dataset": _dataset_from_path(rel),
            "row_index": range(1, len(df) + 1),
            "row_json": js.astype(str),
        }))
        print(f"[{i}/{len(csv_paths)}] {rel}: {df.shape[0]} rows")

    if files_meta:
        inv = pd.DataFrame(files_meta)
        inv.to_sql("RAW_FILES", engine, schema="raw", if_exists="append", index=False, chunksize=1000)
        print(f"Inserted {len(inv)} files into raw.RAW_FILES")

    if row_frames:
        all_rows = pd.concat(row_frames, ignore_index=True)
        all_rows.to_sql("RAW_ROWS", engine, schema="raw", if_exists="append", index=False, chunksize=1000)
        print(f"Inserted {len(all_rows)} rows into raw.RAW_ROWS")


def transform_to_prix_indice_raw(**ctx):
    """
    Transform raw.RAW_ROWS → stg.PRIX_INDICE_RAW
    with correct NATIONAL / REGION / VILLE detection
    and agglomeration = ville (only for VILLE).
    """
    run_id = ctx["run_id"].replace(":", "_").lower()
    hook = make_sql_hook(conn_id="mssql_default", schema=STAGING_DB)
    engine = hook.get_sqlalchemy_engine()

    # Quick inventory by dataset
    df_debug = pd.read_sql(
        """
        SELECT dataset, COUNT(*) AS cnt
        FROM raw.RAW_ROWS
        WHERE run_id = %(r)s
        GROUP BY dataset
        """,
        engine, params={"r": run_id}
    )
    print("=== RAW_ROWS DATASET DISTRIBUTION ===")
    print(df_debug)

    df_raw = pd.read_sql(
        """
        SELECT rel_path, row_json
        FROM raw.RAW_ROWS
        WHERE run_id = %(r)s AND dataset = 'prix_indices'
        """,
        engine, params={"r": run_id}
    )
    if df_raw.empty:
        print("WARN: No prix_indices rows found")
        return

    print(f"Processing {len(df_raw)} raw rows...")
    records, errors = [], []

    def get_value(d: dict, *possible_names):
        for name in possible_names:
            for key in d.keys():
                if name.lower() in key.lower().strip():
                    val = d[key]
                    return (val.strip() if isinstance(val, str) else str(val)) if val is not None else ""
        return ""

    for idx, row in df_raw.iterrows():
        try:
            rel_path = row["rel_path"]
            data = json.loads(row["row_json"])

            # path context
            is_national = "national" in rel_path.lower()
            ville_from_path = _extract_ville_from_path(rel_path)
            file_type = _parse_file_type(rel_path)

            # fields
            annee   = get_value(data, "année", "annee", "year")
            corps   = get_value(data, "corps de métiers", "corps de metiers", "corps", "gros oeuvre", "gros œuvre")
            activ   = get_value(data, "activité", "activite", "sous-corps", "activity")
            produit = get_value(data, "produit", "product")
            variete = get_value(data, "variété", "variete", "variety")

            prix_ttc = get_value(
                data,
                "prix moyens des matériaux de construction ttc (dh)",
                "prix moyens des materiaux de construction ttc (dh)",
                "prix ttc", "prix", "price"
            )
            indice = get_value(
                data,
                "indices des prix moyens des matériaux de construction",
                "indices des prix moyens des materiaux de construction",
                "indice", "index"
            )

            # conversions
            try:
                annee_int = int(annee) if annee else None
            except Exception:
                annee_int = None

            try:
                prix_float = float(str(prix_ttc).replace(",", ".")) if prix_ttc else None
            except Exception:
                prix_float = None

            try:
                indice_float = float(str(indice).replace(",", ".")) if indice else None
            except Exception:
                indice_float = None

            # read potential region value from the row (national file lines)
            region_in_row = None
            for k in ("region", "région", "Region", "Région"):
                if k in data and str(data[k]).strip():
                    region_in_row = str(data[k]).strip()
                    break
            reg_norm = _normalize_region_token(region_in_row)

            # final niveau logic
            if is_national:
                # total Maroc → NATIONAL; otherwise per-region
                if reg_norm in ("", "national", "maroc", "total", "total maroc", "total_maroc"):
                    niveau, region_val, aggl_val = "NATIONAL", None, None
                else:
                    niveau, region_val, aggl_val = "REGION", region_in_row, None
            else:
                niveau, region_val, aggl_val = "VILLE", None, ville_from_path

            # validation
            if not annee_int:
                errors.append(f"Missing annee in {rel_path} row {idx}")
                continue
            if niveau == "VILLE" and not aggl_val:
                errors.append(f"Missing agglomeration for VILLE in {rel_path} row {idx}")
                continue

            records.append({
                "niveau":        niveau,
                "region":        region_val,
                "agglomeration": aggl_val,
                "corps":         (corps or None),
                "activite":      (activ or None),
                "produit":       (produit or None),
                "variete":       (variete or None),
                "annee":         annee_int,
                "prix_ttc":      prix_float,
                "indice":        indice_float,
                "file_type":     file_type,
                "source_file":   rel_path,
            })

        except Exception as e:
            errors.append(f"Error processing row {idx} from {rel_path}: {e}")

    if errors:
        print(f"WARN: {len(errors)} errors (showing up to 10):")
        for e in errors[:10]:
            print("  -", e)

    if not records:
        print("ERROR: No valid records extracted")
        return

    df_out = pd.DataFrame(records)
    if "annee" in df_out:
        df_out["annee"] = df_out["annee"].astype("Int64")

    df_out.to_sql("PRIX_INDICE_RAW", engine, schema="stg", if_exists="append", index=False, chunksize=1000)
    print(f"✓ Inserted {len(df_out)} records into stg.PRIX_INDICE_RAW")

    # quick stats
    print("\n=== NIVEAU DISTRIBUTION ===")
    print(df_out["niveau"].value_counts())

    print("\n=== REGION VALUES (niveau=REGION) ===")
    print(df_out[df_out["niveau"] == "REGION"]["region"].value_counts().head(20))

    print("\n=== VILLES (niveau=VILLE) ===")
    print(df_out[df_out["niveau"] == "VILLE"]["agglomeration"].value_counts().head(20))

    print("\n=== TIME RANGE ===")
    print(f"Years: {df_out['annee'].min()} - {df_out['annee'].max()}")


def stage_load_ponderations(**ctx):
    """Extract ponderation CSV/XLSX files and store them raw."""
    run_id = ctx["run_id"].replace(":", "_").lower()
    landing_dir = ctx["ti"].xcom_pull(key="landing_dir", task_ids="unzip_to_landing")

    hook = make_sql_hook(conn_id="mssql_default", schema=STAGING_DB)
    engine = hook.get_sqlalchemy_engine()

    # Create table if not exists
    ddl = """
    IF OBJECT_ID('raw.RAW_PONDERATIONS','U') IS NULL
    CREATE TABLE raw.RAW_PONDERATIONS(
      run_id NVARCHAR(200) NOT NULL,
      rel_path NVARCHAR(400) NOT NULL,
      sheet_name NVARCHAR(200) NULL,
      row_index INT NOT NULL,
      row_json NVARCHAR(MAX) NOT NULL,
      load_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
      CONSTRAINT PK_RAW_POND PRIMARY KEY (run_id, rel_path, row_index)
    );
    """
    hook.run(ddl)

    files = sorted(Path(landing_dir).rglob("*pond*.csv")) + sorted(Path(landing_dir).rglob("*pond*.xlsx"))
    if not files:
        print("⚠️ No ponderation files found.")
        return

    print(f"Found {len(files)} ponderation files.")
    all_rows = []

    for p in files:
        rel = os.path.relpath(p, landing_dir).replace("\\", "/")
        if p.suffix.lower() in [".xlsx", ".xls"]:
            excel = pd.ExcelFile(p)
            for sheet in excel.sheet_names:
                df = excel.parse(sheet, dtype=str).fillna("")
                js = df.apply(lambda r: json.dumps(r.to_dict(), ensure_ascii=False), axis=1)
                rows = pd.DataFrame({
                    "run_id": run_id,
                    "rel_path": rel,
                    "sheet_name": sheet,
                    "row_index": range(1, len(js)+1),
                    "row_json": js
                })
                all_rows.append(rows)
                print(f"  - {rel} [{sheet}] → {len(df)} rows")
        else:
            df = pd.read_csv(p, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)
            js = df.apply(lambda r: json.dumps(r.to_dict(), ensure_ascii=False), axis=1)
            rows = pd.DataFrame({
                "run_id": run_id,
                "rel_path": rel,
                "sheet_name": None,
                "row_index": range(1, len(js)+1),
                "row_json": js
            })
            all_rows.append(rows)
            print(f"  - {rel} → {len(df)} rows")

    if all_rows:
        df_all = pd.concat(all_rows, ignore_index=True)
        df_all.to_sql("RAW_PONDERATIONS", engine, schema="raw", if_exists="append", index=False)
        print(f"✅ Inserted {len(df_all)} ponderation rows into raw.RAW_PONDERATIONS")


def archive_inputs(**ctx):
    src_zip = ctx["ti"].xcom_pull(key="src_zip", task_ids="detect_zip")
    landing_dir = ctx["ti"].xcom_pull(key="landing_dir", task_ids="unzip_to_landing")
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    os.makedirs(ARCH_DIR, exist_ok=True)

    # move ZIP
    shutil.move(src_zip, os.path.join(ARCH_DIR, f"{ts}__{os.path.basename(src_zip)}"))
    # archive landing dir
    shutil.make_archive(os.path.join(ARCH_DIR, f"{ts}__landing"), "zip", root_dir=landing_dir)
    print(f"Archived to: {ARCH_DIR}")


# =========================
# DAG
# =========================
with DAG(
    dag_id="etl_bronze_raw",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng"},
    tags=["observatoire", "bronze", "raw"],
) as dag:

    ddl_db = SQLExecuteQueryOperator(
        task_id="ensure_obs_staging_db",
        conn_id="mssql_default",
        hook_params={"schema": "master"},
        autocommit=True,
        sql="IF DB_ID(N'OBS_STAGING') IS NULL CREATE DATABASE [OBS_STAGING];",
    )

    t_detect   = PythonOperator(task_id="detect_zip", python_callable=detect_zip)
    t_unzip    = PythonOperator(task_id="unzip_to_landing", python_callable=unzip_to_landing)
    t_copyrefs = PythonOperator(task_id="copy_referentiels_to_landing", python_callable=copy_referentiels_to_landing)
    t_clear    = PythonOperator(task_id="stage_clear_run", python_callable=stage_clear_run)
    t_load     = PythonOperator(task_id="stage_load_raw", python_callable=stage_load_raw)
    t_xform    = PythonOperator(task_id="transform_to_prix_indice_raw", python_callable=transform_to_prix_indice_raw)
    t_pond     = PythonOperator(task_id="stage_load_ponderations", python_callable=stage_load_ponderations)
    t_arch     = PythonOperator(task_id="archive_inputs", python_callable=archive_inputs)

    ddl_db >> t_detect >> t_unzip >> t_copyrefs >> t_clear >> t_load >> [t_xform, t_pond] >> t_arch

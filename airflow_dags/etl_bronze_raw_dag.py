# pyright: reportMissingImports=false
"""
etl_bronze_raw_dag.py - CORRECTED VERSION (v4)
Bronze layer: Ingest raw ZIP files → normalize → stage to OBS_RAW.raw.RAW_ROWS

Key fixes (v4):
  - FIXED: _extract_ville_from_path() now properly strips whitespace and normalizes paths
  - FIXED: Classification checks VILLE FIRST (most specific), then NATIONAL, then REGION
  - FIXED: Better logging for debugging
  - Enhanced path parsing: distinguishes regional codes from villes/provinces
  - Added region code extraction from path structure
  - Changed NIVEAU_COMMUNE to NIVEAU_VILLE (correct terminology)
"""

import glob
import hashlib
import json
import logging
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Import centralized utilities
import config
import db_utils

logger = logging.getLogger(__name__)

# =====================================================================
# HELPERS
# =====================================================================
def _md5_file(path: str) -> str:
    """Compute MD5 hash of file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _dataset_from_path(path: str) -> str:
    """Classify dataset from file path - only check actual file/folder names, not structure."""
    p = (path or "").lower()
    
    # Check if it's a referential file (specific file names in referentiels folder)
    if "referentiels/" in p or "referentiels\\" in p:
        # It's in a referentiels folder
        if any(tok in p for tok in ("region.csv", "commune.csv", "province.csv")):
            return "referentiels"
    
    # Everything else is prix_indices
    return "prix_indices"


def _parse_file_type(filename: str) -> str:
    """Determine file type: T1 (prix/variété) or T3 (indice/produit)."""
    fn = (filename or "").lower()
    if "_t1" in fn or "t1." in fn:
        return config.FILE_TYPE_T1
    if "_t3" in fn or "t3." in fn:
        return config.FILE_TYPE_T3
    return config.FILE_TYPE_UNKNOWN


def _extract_ville_from_path(rel_path: str) -> str | None:
    """Extract ville/province from folder structure.
    
    Rules:
    - NATIONAL/* → None (national level)
    - MS/ms_*.csv → None (regional level, MS is a regional code)
    - MARRAKECH/marrakech_*.csv → "MARRAKECH" (ville/province level)
    - prix-et-indices.../region-marrakech-safi_CSV/ALHAOUZ/alhaouz_*.csv → "ALHAOUZ"
    """
    if not rel_path:
        return None
    
    # Normalize path separators and strip whitespace
    rel_path = rel_path.replace("\\", "/").replace("//", "/")
    parts = [x.strip() for x in rel_path.split("/") if x.strip()]
    
    if len(parts) < 2:
        return None
    
    folder_name = parts[-2].upper().strip()
    
    # Regional codes from LEVEL 1
    regional_codes = {
        "BKH", "CS", "DT", "EOD", "FM", "GON", "LSH", "MS", 
        "ORI", "RSK", "SM", "TTAH", "NATIONAL"
    }
    
    # If regional code or NATIONAL, not a ville-level file
    if folder_name in regional_codes:
        return None
    
    # Only return if it contains letters (real name, not just numbers)
    if any(c.isalpha() for c in folder_name):
        return folder_name
    
    return None


def _extract_region_code_from_path(rel_path: str) -> str | None:
    """Extract region code from folder structure.
    
    Examples:
    - MS/ms_T1.csv → "MS"
    - MARRAKECH/marrakech_T1.csv → None (it's a ville, not region)
    - NATIONAL/maroc_T1.csv → None (it's national)
    - region-marrakech-safi_CSV/ALHAOUZ/alhaouz_T1.csv → None (it's ville)
    """
    if not rel_path:
        return None
    
    rel_path = rel_path.replace("\\", "/").replace("//", "/")
    parts = [x.strip().upper() for x in rel_path.split("/") if x.strip()]
    
    if not parts:
        return None
    
    # Search for region codes in the path
    regional_codes = {
        "BKH", "CS", "DT", "EOD", "FM", "GON", "LSH", "MS", 
        "ORI", "RSK", "SM", "TTAH"
    }
    
    for part in parts:
        if part in regional_codes:
            return part
    
    return None


def _normalize_region_token(val: str | None) -> str:
    """Normalize region name."""
    if not val:
        return ""
    return " ".join(str(val).lower().strip().split())


def _read_csv_robust(path: str, encodings=None, separators=None) -> pd.DataFrame:
    """Read CSV with auto-detection of encoding/separator."""
    if encodings is None:
        encodings = config.CSV_ENCODINGS
    if separators is None:
        separators = config.CSV_SEPARATORS
    
    for sep in separators:
        for enc in encodings:
            try:
                df = pd.read_csv(
                    path,
                    sep=sep,
                    encoding=enc,
                    dtype=str,
                    keep_default_na=False,
                )
                logger.debug(f"✓ Read CSV with sep='{sep}', encoding='{enc}'")
                return df
            except Exception:
                continue
    
    raise ValueError(f"Cannot read CSV: {path} (tried all encoding/separator combinations)")


# =====================================================================
# TASKS
# =====================================================================
def task_detect_zip(**ctx):
    """Find latest ZIP file in raw directory."""
    search_dirs = [config.RAW_DIR, config.RAW_DIR.parent / "prix_indices"]
    zips = []
    
    for d in search_dirs:
        if d.exists():
            zips.extend(sorted(d.glob("*.zip")))
    
    if not zips:
        raise FileNotFoundError(f"No ZIP files found in: {[str(d) for d in search_dirs]}")
    
    src_zip = str(sorted(zips)[-1])
    logger.info(f"✓ Detected ZIP: {src_zip}")
    ctx["ti"].xcom_push(key="src_zip", value=src_zip)


def task_unzip_to_landing(**ctx):
    """Extract ZIP to landing directory."""
    src_zip = ctx["ti"].xcom_pull(key="src_zip", task_ids="task_detect_zip")
    run_id = ctx["run_id"].replace(":", "_").lower()
    run_dir = config.LANDING_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    
    with zipfile.ZipFile(src_zip, "r") as zf:
        zf.extractall(run_dir)
    
    logger.info(f"✓ Extracted to: {run_dir}")
    ctx["ti"].xcom_push(key="landing_dir", value=str(run_dir))


def task_copy_referentiels(**ctx):
    """Copy referential files to landing directory."""
    run_dir = Path(ctx["ti"].xcom_pull(key="landing_dir", task_ids="task_unzip_to_landing"))
    
    if not config.REF_DIR.exists():
        logger.warning(f"Referentials directory not found: {config.REF_DIR}")
        return
    
    dst = run_dir / "referentiels"
    dst.mkdir(parents=True, exist_ok=True)
    
    count = 0
    for p in config.REF_DIR.glob("*.csv"):
        shutil.copy2(p, dst / p.name)
        count += 1
    
    logger.info(f"✓ Copied {count} referential files to {dst}")


def task_stage_clear_run(**ctx):
    """Create raw schema/tables and clear run data."""
    run_id = ctx["run_id"].replace(":", "_").lower()
    
    ddl = """
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='raw')
        EXEC('CREATE SCHEMA raw');
    """
    db_utils.execute_sql_with_transaction(ddl, schema=config.RAW_DB)
    logger.info("✓ Ensured raw schema exists")


def task_stage_load_raw(**ctx):
    """Load raw ZIP contents into raw.RAW_FILES and raw.RAW_ROWS."""
    landing_dir = Path(ctx["ti"].xcom_pull(key="landing_dir", task_ids="task_unzip_to_landing"))
    run_id = ctx["run_id"].replace(":", "_").lower()
    
    engine = db_utils.make_sqlalchemy_engine(schema=config.RAW_DB)
    csv_paths = sorted(landing_dir.rglob("*.csv"))
    
    logger.info(f"Found {len(csv_paths)} CSV files")
    
    files_meta = []
    row_frames = []
    
    for i, path in enumerate(csv_paths, 1):
        rel = str(path.relative_to(landing_dir)).replace("\\", "/")
        
        try:
            df = _read_csv_robust(str(path))
        except Exception as e:
            logger.warning(f"Skipped {rel}: {e}")
            continue
        
        if df.empty:
            logger.debug(f"[{i}/{len(csv_paths)}] {rel}: 0 rows")
            continue
        
        files_meta.append({
            "run_id": run_id,
            "rel_path": rel,
            "dataset": _dataset_from_path(rel),
            "row_count": len(df),
            "md5_hash": _md5_file(str(path)),
        })
        
        row_json = df.fillna("").apply(
            lambda r: json.dumps(
                {str(k): (str(r[k]) if r[k] is not None else "") for k in df.columns},
                ensure_ascii=False
            ),
            axis=1
        )
        
        row_frames.append(pd.DataFrame({
            "run_id": run_id,
            "rel_path": rel,
            "dataset": _dataset_from_path(rel),
            "row_index": range(1, len(df) + 1),
            "row_json": row_json.astype(str),
        }))
        
        logger.info(f"[{i}/{len(csv_paths)}] {rel}: {len(df)} rows")
    
    if files_meta:
        pd.DataFrame(files_meta).to_sql(
            "RAW_FILES",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=config.BATCH_SIZE,
        )
        logger.info(f"✓ Inserted {len(files_meta)} file metadata rows")
    
    if row_frames:
        all_rows = pd.concat(row_frames, ignore_index=True)
        all_rows.to_sql(
            "RAW_ROWS",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=config.BATCH_SIZE,
        )
        logger.info(f"✓ Inserted {len(all_rows)} data rows")


def task_transform_to_prix_indice_raw(**ctx):
    """Transform raw.RAW_ROWS → stg.PRIX_INDICE_RAW with tolerant header matching."""
    run_id = ctx["run_id"].replace(":", "_").lower()
    
    engine_raw = db_utils.make_sqlalchemy_engine(schema=config.RAW_DB)
    
    with engine_raw.connect() as conn:
        result = conn.execute(text("""
            SELECT rel_path, row_json
            FROM raw.RAW_ROWS
            WHERE run_id = :r AND dataset = 'prix_indices'
            ORDER BY rel_path, row_index
        """), {"r": run_id})
        df_raw = pd.DataFrame(result.fetchall(), columns=["rel_path", "row_json"])
    
    if df_raw.empty:
        logger.warning("No prix_indices rows found in raw data")
        return
    
    logger.info(f"Total rows to process: {len(df_raw)}")
    
    def get_value(data: dict, *names):
        """Tolerant column value retrieval: case/accent insensitive."""
        if not data:
            return ""
        keys = list(data.keys())
        for name in names:
            needle = (name or "").lower().strip()
            for k in keys:
                if k.lower().strip() == needle:
                    v = data.get(k)
                    return v.strip() if isinstance(v, str) else (str(v) if v is not None else "")
            for k in keys:
                if needle and needle in k.lower():
                    v = data.get(k)
                    return v.strip() if isinstance(v, str) else (str(v) if v is not None else "")
        return ""
    
    def to_float(x):
        """Convert to float, handling comma decimals."""
        if x in (None, ""):
            return None
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return None
    
    records = []
    skipped_count = 0
    alhaouz_count = 0
    
    for idx, row in df_raw.iterrows():
        try:
            rel_path = row["rel_path"]
            data = json.loads(row["row_json"])
            
            # DEBUG: Log ALHAOUZ
            if "alhaouz" in rel_path.lower():
                alhaouz_count += 1
                logger.info(f"ALHAOUZ [{alhaouz_count}] rel_path={rel_path}")
                logger.info(f"  JSON keys: {list(data.keys())}")
            
            annee = get_value(data, "année", "annee", "year", "an", "Année")
            corps = get_value(data, "corps de métiers", "corps de metiers", "corps", "metiers", "Metiers", "Corps de métiers")
            activite = get_value(data, "activité", "activite", "Activité")
            produit = get_value(data, "produit", "product", "Produit")
            variete = get_value(data, "variété", "variete", "variety", "Variété")
            prix_ttc = get_value(data, "prix moyens", "prix ttc", "prix", "prix moyens des matériaux de construction ttc", "Prix moyens des matériaux de construction TTC (DH)")
            indice = get_value(data, "indices des prix", "indice", "index", "indices des prix moyens", "Indices des prix moyens des matériaux de construction")
            region_in = get_value(data, "region", "région", "Region")
            ville_in = get_value(data, "ville", "city", "agglomeration", "Ville")
            
            if "alhaouz" in rel_path.lower():
                logger.info(f"  Extracted: annee={annee}, prix_ttc={prix_ttc}, indice={indice}, ville_in={ville_in}")
            
            try:
                annee_int = int(str(annee).strip())
            except Exception as e:
                if "alhaouz" in rel_path.lower():
                    logger.warning(f"  ALHAOUZ: Failed to parse annee: {annee} ({e})")
                skipped_count += 1
                continue
            
            # Determine level from path
            p = rel_path.lower()
            parts = [x.strip() for x in p.split("/") if x.strip()]
            last_folder = parts[-2].upper().strip() if len(parts) >= 2 else ""
            filename = parts[-1].lower() if parts else ""
            
            ville_from_path = _extract_ville_from_path(rel_path)
            region_code_from_path = _extract_region_code_from_path(rel_path)
            
            if "alhaouz" in rel_path.lower():
                logger.info(f"  Path parsing: last_folder={last_folder}, ville_from_path={ville_from_path}")
            
            national_hint = (
                last_folder == "NATIONAL" or
                filename.startswith("maroc_t") or
                filename.startswith("national_t")
            )
            reg_norm = _normalize_region_token(region_in)
            
            # Classification
            if ville_from_path:
                niveau, region_val, aggl_val = "VILLE", None, ville_from_path
                if "alhaouz" in rel_path.lower():
                    logger.info(f"  ✓ Classified as VILLE: {ville_from_path}")
            elif national_hint:
                niveau, region_val, aggl_val = "NATIONAL", None, None
            elif region_code_from_path:
                niveau, aggl_val = "REGION", None
                region_val = region_code_from_path
            else:
                niveau, aggl_val = "REGION", None
                region_val = reg_norm if reg_norm else None
            
            # Validate
            if niveau == "VILLE" and not aggl_val:
                if "alhaouz" in rel_path.lower():
                    logger.warning(f"  ⚠ SKIPPED: VILLE without agglomeration")
                skipped_count += 1
                continue
            
            records.append({
                "niveau": niveau,
                "region": region_val,
                "agglomeration": aggl_val,
                "corps": corps or None,
                "activite": activite or None,
                "produit": produit or None,
                "variete": variete or None,
                "annee": annee_int,
                "prix_ttc": to_float(prix_ttc),
                "indice": to_float(indice),
                "file_type": _parse_file_type(rel_path),
                "source_file": rel_path,
            })
            
            if "alhaouz" in rel_path.lower():
                logger.info(f"  ✓ ADDED to records")
                
        except Exception as e:
            logger.error(f"EXCEPTION in {rel_path}: {str(e)}", exc_info=True)
            skipped_count += 1
    
    logger.info(f"Processing complete: {len(records)} records, {skipped_count} skipped, {alhaouz_count} ALHAOUZ rows processed")
    
    if not records:
        raise ValueError(f"No valid records extracted from raw data (skipped {skipped_count})")
    
    engine_staging = db_utils.make_sqlalchemy_engine(schema=config.STAGING_DB)
    pd.DataFrame(records).to_sql(
        "PRIX_INDICE_RAW",
        engine_staging,
        schema="stg",
        if_exists="append",
        index=False,
        chunksize=config.BATCH_SIZE,
    )
    logger.info(f"✓ Inserted {len(records)} records into stg.PRIX_INDICE_RAW")

def task_archive_inputs(**ctx):
    """Archive landing directory and ZIP file."""
    src_zip = ctx["ti"].xcom_pull(key="src_zip", task_ids="task_detect_zip")
    landing_dir = Path(ctx["ti"].xcom_pull(key="landing_dir", task_ids="task_unzip_to_landing"))
    
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    config.ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    
    archived_zip = config.ARCHIVE_DIR / f"{ts}__{Path(src_zip).name}"
    shutil.copy2(src_zip, archived_zip)
    logger.info(f"✓ Archived source ZIP: {archived_zip}")
    
    landing_snapshot = config.ARCHIVE_DIR / f"{ts}__landing"
    shutil.make_archive(str(landing_snapshot), "zip", root_dir=landing_dir)
    logger.info(f"✓ Archived landing snapshot: {landing_snapshot}.zip")


# =====================================================================
# DAG
# =====================================================================
with DAG(
    dag_id="etl_bronze_raw",
    description="Bronze layer: Ingest raw data from ZIP files",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1},
    tags=config.DAG_TAGS["bronze"],
) as dag:

    t_ensure_db = SQLExecuteQueryOperator(
        task_id="ensure_raw_db",
        conn_id=config.MSSQL_CONN_ID,
        hook_params={"schema": "master"},
        autocommit=True,
        sql=f"IF DB_ID(N'{config.RAW_DB}') IS NULL CREATE DATABASE [{config.RAW_DB}];",
    )

    t_detect = PythonOperator(
        task_id="task_detect_zip",
        python_callable=task_detect_zip,
    )

    t_unzip = PythonOperator(
        task_id="task_unzip_to_landing",
        python_callable=task_unzip_to_landing,
    )

    t_refs = PythonOperator(
        task_id="task_copy_referentiels",
        python_callable=task_copy_referentiels,
    )

    t_clear = PythonOperator(
        task_id="task_stage_clear_run",
        python_callable=task_stage_clear_run,
    )

    t_load = PythonOperator(
        task_id="task_stage_load_raw",
        python_callable=task_stage_load_raw,
    )

    t_xform = PythonOperator(
        task_id="task_transform_to_prix_indice_raw",
        python_callable=task_transform_to_prix_indice_raw,
    )

    t_arch = PythonOperator(
        task_id="task_archive_inputs",
        python_callable=task_archive_inputs,
    )

    t_ensure_db >> t_detect >> t_unzip >> t_refs >> t_clear >> t_load >> t_xform >> t_arch
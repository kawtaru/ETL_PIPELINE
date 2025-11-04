# pyright: reportMissingImports=false
"""
etl_silver_ponderations_dag.py - CORRECTED VERSION
Silver layer: Load ponderation (weight) CSV files into staging tables

Features:
  - Robust CSV reading with encoding/separator auto-detection
  - Dynamic column creation from CSV headers
  - Preserves original column names exactly as in CSV
"""

import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import types as sa_types

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import config
import db_utils

logger = logging.getLogger(__name__)

# =====================================================================
# FILE CONFIGURATION
# =====================================================================
FILE_PONDERATION_VILLES = config.PONDERATION_DIR / "ponderations_villes_regions_output.csv"
FILE_PONDERATION_MC = config.PONDERATION_DIR / "ponderation_MC_prixindice.csv"

# =====================================================================
# HELPERS
# =====================================================================
def sql_quote_ident(name: str) -> str:
    """Quote SQL Server identifier, preserving accents/spaces."""
    return f"[{str(name).replace(']', ']]')}]"


def read_csv_robust(path: str) -> pd.DataFrame:
    """Read CSV with auto-detection of encoding and separator."""
    for sep in config.CSV_SEPARATORS:
        for enc in config.CSV_ENCODINGS:
            try:
                df = pd.read_csv(
                    path,
                    sep=sep,
                    encoding=enc,
                    dtype=str,
                    keep_default_na=False,
                )
                logger.info(f"✓ Read CSV with sep='{sep}', encoding='{enc}'")
                return df
            except Exception:
                continue
    
    raise ValueError(f"Cannot read CSV with any encoding/separator: {path}")


def create_table_from_csv(table_full: str, df: pd.DataFrame) -> None:
    """
    Create SQL table with exact CSV headers as NVARCHAR(MAX).
    Drops and truncates if exists.
    """
    schema, table = table_full.split(".", 1)
    
    # Build column list
    cols = ",\n        ".join(
        f"{sql_quote_ident(c)} NVARCHAR(MAX) NULL" for c in df.columns
    )
    
    ddl = f"""
    USE [{config.STAGING_DB}];
    
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{schema}')
        EXEC('CREATE SCHEMA {schema}');
    
    IF OBJECT_ID('{table_full}', 'U') IS NOT NULL
        DROP TABLE {table_full};
    
    CREATE TABLE {table_full} (
        {cols}
    );
    
    TRUNCATE TABLE {table_full};
    """
    
    db_utils.execute_sql_with_transaction(ddl, schema=config.STAGING_DB)
    logger.info(f"✓ Created table {table_full} with {len(df.columns)} columns")


def insert_csv_data(table_full: str, df: pd.DataFrame) -> None:
    """Insert CSV data with NVARCHAR(MAX) for all columns."""
    dtype_map = {c: sa_types.UnicodeText() for c in df.columns}
    engine = db_utils.make_sqlalchemy_engine(schema=config.STAGING_DB)
    
    schema, table = table_full.split(".", 1)
    
    df.to_sql(
        table,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=config.BATCH_SIZE,
        dtype=dtype_map,
    )
    logger.info(f"✓ Inserted {len(df)} rows into {table_full}")


# =====================================================================
# TASKS
# =====================================================================
def task_ensure_db_schema(**ctx):
    """Ensure database and schema exist."""
    ddl = f"""
    IF DB_ID(N'{config.STAGING_DB}') IS NULL
        CREATE DATABASE [{config.STAGING_DB}];
    
    USE [{config.STAGING_DB}];
    
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg')
        EXEC('CREATE SCHEMA stg');
    """
    db_utils.execute_sql_with_autocommit(ddl, config.MSSQL_CONN_ID)
    logger.info("✓ Ensured database and schema exist")


def task_load_ponderation_villes(**ctx):
    """Load ponderation villes CSV."""
    if not FILE_PONDERATION_VILLES.exists():
        logger.warning(f"File not found: {FILE_PONDERATION_VILLES} (skipping)")
        ctx["ti"].xcom_push(key="ponderation_villes_rows", value=0)
        return
    
    logger.info(f"Reading: {FILE_PONDERATION_VILLES}")
    df = read_csv_robust(str(FILE_PONDERATION_VILLES))
    
    if df.empty:
        logger.warning("CSV is empty")
        ctx["ti"].xcom_push(key="ponderation_villes_rows", value=0)
        return
    
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from CSV")
    logger.info(f"Sample columns: {df.columns.tolist()[:5]}")
    logger.info(f"Sample data:\n{df.head(2).to_string()}")
    
    # Create table and insert
    create_table_from_csv(config.SILVER_PONDERATION_VILLE, df)
    insert_csv_data(config.SILVER_PONDERATION_VILLE, df)
    
    ctx["ti"].xcom_push(key="ponderation_villes_rows", value=len(df))


def task_load_ponderation_mc(**ctx):
    """Load ponderation MC (Marche de Constructive) CSV."""
    if not FILE_PONDERATION_MC.exists():
        logger.warning(f"File not found: {FILE_PONDERATION_MC} (skipping)")
        ctx["ti"].xcom_push(key="ponderation_mc_rows", value=0)
        return
    
    logger.info(f"Reading: {FILE_PONDERATION_MC}")
    df = read_csv_robust(str(FILE_PONDERATION_MC))
    
    if df.empty:
        logger.warning("CSV is empty")
        ctx["ti"].xcom_push(key="ponderation_mc_rows", value=0)
        return
    
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from CSV")
    logger.info(f"Sample columns: {df.columns.tolist()[:5]}")
    logger.info(f"Sample data:\n{df.head(2).to_string()}")
    
    # Create table and insert
    create_table_from_csv(config.SILVER_PONDERATION_MC, df)
    insert_csv_data(config.SILVER_PONDERATION_MC, df)
    
    ctx["ti"].xcom_push(key="ponderation_mc_rows", value=len(df))


def task_report(**ctx):
    """Report summary."""
    villes_rows = ctx["ti"].xcom_pull(key="ponderation_villes_rows", task_ids="task_load_ponderation_villes") or 0
    mc_rows = ctx["ti"].xcom_pull(key="ponderation_mc_rows", task_ids="task_load_ponderation_mc") or 0
    
    logger.info(f"✓ SILVER PONDERATIONS: villes={villes_rows}, mc={mc_rows}")
    
    if villes_rows == 0 and mc_rows == 0:
        logger.warning("⚠️ No ponderation data loaded")


# =====================================================================
# DAG
# =====================================================================
with DAG(
    dag_id="etl_silver_ponderations",
    description="Silver layer: Load ponderation (weight) CSV files",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1},
    tags=config.DAG_TAGS["silver"],
) as dag:

    t_ensure = PythonOperator(
        task_id="task_ensure_db_schema",
        python_callable=task_ensure_db_schema,
    )

    t_villes = PythonOperator(
        task_id="task_load_ponderation_villes",
        python_callable=task_load_ponderation_villes,
    )

    t_mc = PythonOperator(
        task_id="task_load_ponderation_mc",
        python_callable=task_load_ponderation_mc,
    )

    t_report = PythonOperator(
        task_id="task_report",
        python_callable=task_report,
    )

    # Task dependencies
    t_ensure >> [t_villes, t_mc] >> t_report
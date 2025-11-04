# pyright: reportMissingImports=false
"""
obs_create_schema_dag.py - CORRECTED VERSION
Bootstrap DAG: Creates databases and schemas for the Observatoire project.
"""

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)

# =====================================================================
# DDL SCRIPTS (inline for bootstrap)
# =====================================================================
DDL_CREATE_DATABASES = """
-- Create databases if missing
IF DB_ID(N'OBSERVATOIRE') IS NULL CREATE DATABASE [OBSERVATOIRE];
IF DB_ID(N'OBS_STAGING') IS NULL CREATE DATABASE [OBS_STAGING];
IF DB_ID(N'OBS_RAW') IS NULL CREATE DATABASE [OBS_RAW];
"""

DDL_OBSERVATOIRE_SCHEMA = """
USE [OBSERVATOIRE];

-- Create dbo schema (usually exists by default, but ensure it)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='dbo')
    EXEC('CREATE SCHEMA dbo');

-- Geographic dimensions
IF OBJECT_ID('dbo.REGION', 'U') IS NULL
    CREATE TABLE dbo.REGION (
        id_region INT IDENTITY(1,1) PRIMARY KEY,
        nom_region NVARCHAR(150) NOT NULL,
        code_region NVARCHAR(50) NOT NULL UNIQUE
    );

IF OBJECT_ID('dbo.PROVINCE_PREFECTURE', 'U') IS NULL
    CREATE TABLE dbo.PROVINCE_PREFECTURE (
        id_province_prefecture INT IDENTITY(1,1) PRIMARY KEY,
        nom_province_prefecture NVARCHAR(200) NOT NULL,
        code_province_prefecture NVARCHAR(50) NOT NULL UNIQUE,
        id_region INT NOT NULL FOREIGN KEY REFERENCES dbo.REGION(id_region)
    );

-- CORRECTED: Replaced VILLE with COMMUNE
IF OBJECT_ID('dbo.COMMUNE', 'U') IS NULL
    CREATE TABLE dbo.COMMUNE (
        id_commune INT IDENTITY(1,1) PRIMARY KEY,
        nom_commune NVARCHAR(200) NOT NULL,
        code_commune NVARCHAR(80) NULL UNIQUE,
        id_province_prefecture INT NOT NULL
            FOREIGN KEY REFERENCES dbo.PROVINCE_PREFECTURE(id_province_prefecture)
    );

CREATE UNIQUE INDEX UX_COMMUNE_code ON dbo.COMMUNE(code_commune) WHERE code_commune IS NOT NULL;

-- Business dimensions
IF OBJECT_ID('dbo.METIER', 'U') IS NULL
    CREATE TABLE dbo.METIER (
        id_metier INT IDENTITY(1,1) PRIMARY KEY,
        nom_metier NVARCHAR(120) NOT NULL UNIQUE
    );

IF OBJECT_ID('dbo.CORPS_METIER', 'U') IS NULL
    CREATE TABLE dbo.CORPS_METIER (
        id_corps INT IDENTITY(1,1) PRIMARY KEY,
        nom_corps NVARCHAR(200) NOT NULL UNIQUE,
        id_metier INT NOT NULL FOREIGN KEY REFERENCES dbo.METIER(id_metier)
    );

IF OBJECT_ID('dbo.ACTIVITE', 'U') IS NULL
    CREATE TABLE dbo.ACTIVITE (
        id_activite INT IDENTITY(1,1) PRIMARY KEY,
        nom_activite NVARCHAR(200) NOT NULL UNIQUE,
        id_corps INT NOT NULL FOREIGN KEY REFERENCES dbo.CORPS_METIER(id_corps)
    );

IF OBJECT_ID('dbo.PRODUIT', 'U') IS NULL
    CREATE TABLE dbo.PRODUIT (
        id_produit INT IDENTITY(1,1) PRIMARY KEY,
        nom_produit NVARCHAR(200) NOT NULL UNIQUE,
        id_activite INT NOT NULL FOREIGN KEY REFERENCES dbo.ACTIVITE(id_activite)
    );

IF OBJECT_ID('dbo.VARIETE', 'U') IS NULL
    CREATE TABLE dbo.VARIETE (
        id_variete INT IDENTITY(1,1) PRIMARY KEY,
        nom_variete NVARCHAR(200) NOT NULL UNIQUE,
        id_produit INT NOT NULL FOREIGN KEY REFERENCES dbo.PRODUIT(id_produit)
    );

-- Fact tables (with corrected FK to COMMUNE instead of VILLE)
IF OBJECT_ID('dbo.INDICE', 'U') IS NULL
    CREATE TABLE dbo.INDICE (
        id_indice INT IDENTITY(1,1) PRIMARY KEY,
        annee INT NOT NULL,
        valeur_indice DECIMAL(18, 6) NULL,
        prix_moyen DECIMAL(18, 6) NULL,
        type_indice NVARCHAR(20) NULL,
        id_reference INT NULL,
        id_commune INT NULL FOREIGN KEY REFERENCES dbo.COMMUNE(id_commune),
        id_region INT NULL FOREIGN KEY REFERENCES dbo.REGION(id_region)
    );

CREATE UNIQUE INDEX UX_INDICE_nk ON dbo.INDICE(annee, type_indice, id_reference, id_commune, id_region);

IF OBJECT_ID('dbo.PONDERATION', 'U') IS NULL
    CREATE TABLE dbo.PONDERATION (
        id_ponderation INT IDENTITY(1,1) PRIMARY KEY,
        annee INT NOT NULL,
        poids DECIMAL(18, 6) NULL,
        niveau NVARCHAR(20) NOT NULL,
        id_reference INT NULL,
        id_commune INT NULL FOREIGN KEY REFERENCES dbo.COMMUNE(id_commune),
        id_region INT NULL FOREIGN KEY REFERENCES dbo.REGION(id_region)
    );

CREATE UNIQUE INDEX UX_POND_nk ON dbo.PONDERATION(annee, niveau, id_reference, id_commune, id_region);
"""

DDL_STAGING_SCHEMA = """
USE [OBS_STAGING];

-- Create stg schema for staging tables
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg')
    EXEC('CREATE SCHEMA stg');

-- Referential tables (geographic)
IF OBJECT_ID('stg.REGION_REF', 'U') IS NULL
    CREATE TABLE stg.REGION_REF (
        region_code NVARCHAR(50) NOT NULL,
        region_name NVARCHAR(200) NOT NULL,
        load_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_REGION_REF PRIMARY KEY(region_code)
    );

IF OBJECT_ID('stg.PROVINCE_REF', 'U') IS NULL
    CREATE TABLE stg.PROVINCE_REF (
        province_code NVARCHAR(50) NOT NULL,
        province_name NVARCHAR(200) NOT NULL,
        region_code NVARCHAR(50) NULL,
        load_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_PROVINCE_REF PRIMARY KEY(province_code)
    );

IF OBJECT_ID('stg.COMMUNE_REF', 'U') IS NULL
    CREATE TABLE stg.COMMUNE_REF (
        commune_code NVARCHAR(50) NOT NULL,
        commune_name NVARCHAR(200) NOT NULL,
        province_code NVARCHAR(50) NULL,
        load_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_COMMUNE_REF PRIMARY KEY(commune_code)
    );

-- Price and index raw data
IF OBJECT_ID('stg.PRIX_INDICE_RAW', 'U') IS NULL
    CREATE TABLE stg.PRIX_INDICE_RAW (
        id INT IDENTITY(1,1) PRIMARY KEY,
        niveau NVARCHAR(20) NOT NULL,
        region NVARCHAR(200) NULL,
        agglomeration NVARCHAR(200) NULL,
        corps NVARCHAR(200) NULL,
        activite NVARCHAR(200) NULL,
        produit NVARCHAR(200) NULL,
        variete NVARCHAR(200) NULL,
        annee INT NOT NULL,
        prix_ttc DECIMAL(18, 6) NULL,
        indice DECIMAL(18, 6) NULL,
        file_type NVARCHAR(10) NULL,
        source_file NVARCHAR(500) NULL,
        load_ts DATETIME2 DEFAULT SYSUTCDATETIME()
    );

-- Ponderation (weights) files
IF OBJECT_ID('stg.PONDERATION_VILLE', 'U') IS NULL
    CREATE TABLE stg.PONDERATION_VILLE (
        id INT IDENTITY(1,1),
        -- columns will be populated dynamically from CSV headers
        load_ts DATETIME2 DEFAULT SYSUTCDATETIME()
    );

IF OBJECT_ID('stg.PONDERATION_MC', 'U') IS NULL
    CREATE TABLE stg.PONDERATION_MC (
        id INT IDENTITY(1,1),
        -- columns will be populated dynamically from CSV headers
        load_ts DATETIME2 DEFAULT SYSUTCDATETIME()
    );
"""

DDL_RAW_SCHEMA = """
USE [OBS_RAW];

-- Create raw schema for raw data ingestion
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='raw')
    EXEC('CREATE SCHEMA raw');

-- Raw file inventory
IF OBJECT_ID('raw.RAW_FILES', 'U') IS NULL
    CREATE TABLE raw.RAW_FILES (
        id_file INT IDENTITY(1,1) PRIMARY KEY,
        run_id NVARCHAR(100) NOT NULL,
        rel_path NVARCHAR(500) NOT NULL,
        dataset NVARCHAR(50) NOT NULL,
        row_count INT NULL,
        md5_hash NVARCHAR(32) NULL,
        load_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

-- Raw row data (JSON-based for flexibility)
IF OBJECT_ID('raw.RAW_ROWS', 'U') IS NULL
    CREATE TABLE raw.RAW_ROWS (
        id_row BIGINT IDENTITY(1,1) PRIMARY KEY,
        run_id NVARCHAR(100) NOT NULL,
        rel_path NVARCHAR(500) NOT NULL,
        dataset NVARCHAR(50) NOT NULL,
        row_index INT NOT NULL,
        row_json NVARCHAR(MAX) NOT NULL,
        load_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

CREATE INDEX IX_RAW_ROWS_run_id ON raw.RAW_ROWS(run_id);
CREATE INDEX IX_RAW_ROWS_dataset ON raw.RAW_ROWS(dataset);
"""

QC_VALIDATION = """
-- Validation checks
SELECT 'SCHEMA VALIDATION' AS check_name;

-- Check all required databases exist
SELECT 
    'OBSERVATOIRE' AS database_name,
    CASE WHEN DB_ID(N'OBSERVATOIRE') IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS status;

SELECT 
    'OBS_STAGING' AS database_name,
    CASE WHEN DB_ID(N'OBS_STAGING') IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS status;

SELECT 
    'OBS_RAW' AS database_name,
    CASE WHEN DB_ID(N'OBS_RAW') IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS status;

-- Check key tables exist in OBSERVATOIRE
USE [OBSERVATOIRE];
SELECT
    'OBSERVATOIRE.dbo.REGION' AS table_name,
    CASE WHEN OBJECT_ID('dbo.REGION', 'U') IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS status;

SELECT
    'OBSERVATOIRE.dbo.COMMUNE' AS table_name,
    CASE WHEN OBJECT_ID('dbo.COMMUNE', 'U') IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS status;

-- Check key tables exist in OBS_STAGING
USE [OBS_STAGING];
SELECT
    'OBS_STAGING.stg.REGION_REF' AS table_name,
    CASE WHEN OBJECT_ID('stg.REGION_REF', 'U') IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS status;

SELECT
    'OBS_STAGING.stg.COMMUNE_REF' AS table_name,
    CASE WHEN OBJECT_ID('stg.COMMUNE_REF', 'U') IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS status;
"""

# =====================================================================
# DAG Tasks
# =====================================================================
def log_bootstrap(**ctx):
    """Log bootstrap start."""
    logger.info("🚀 Starting Observatoire ETL bootstrap...")
    logger.info("Creating databases and schemas...")


def log_completion(**ctx):
    """Log bootstrap completion."""
    logger.info("✅ Bootstrap completed successfully!")
    logger.info("Ready to run BRONZE → SILVER → GOLD pipeline")


# =====================================================================
# DAG Definition
# =====================================================================
with DAG(
    dag_id="obs_create_schema",
    description="Bootstrap: Create databases and schemas for Observatoire ETL",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 1,
    },
    tags=["observatoire", "bootstrap", "ddl"],
) as dag:

    # 1. Log start
    t_log_start = PythonOperator(
        task_id="log_bootstrap_start",
        python_callable=log_bootstrap,
    )

    # 2. Create databases
    t_create_dbs = SQLExecuteQueryOperator(
        task_id="create_databases",
        conn_id="mssql_default",
        hook_params={"schema": "master"},
        autocommit=True,
        sql=DDL_CREATE_DATABASES,
    )

    # 3. Create OBSERVATOIRE schema
    t_obs_schema = SQLExecuteQueryOperator(
        task_id="create_observatoire_schema",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql=DDL_OBSERVATOIRE_SCHEMA,
    )

    # 4. Create OBS_STAGING schema
    t_staging_schema = SQLExecuteQueryOperator(
        task_id="create_staging_schema",
        conn_id="mssql_default",
        hook_params={"schema": "OBS_STAGING"},
        autocommit=True,
        sql=DDL_STAGING_SCHEMA,
    )

    # 5. Create OBS_RAW schema
    t_raw_schema = SQLExecuteQueryOperator(
        task_id="create_raw_schema",
        conn_id="mssql_default",
        hook_params={"schema": "OBS_RAW"},
        autocommit=True,
        sql=DDL_RAW_SCHEMA,
    )

    # 6. Validation
    t_validate = SQLExecuteQueryOperator(
        task_id="validate_schema",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        sql=QC_VALIDATION,
    )

    # 7. Log completion
    t_log_end = PythonOperator(
        task_id="log_bootstrap_complete",
        python_callable=log_completion,
    )

    # Task dependencies
    t_log_start >> t_create_dbs >> [t_obs_schema, t_staging_schema, t_raw_schema] >> t_validate >> t_log_end
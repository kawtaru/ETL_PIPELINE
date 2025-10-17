# pyright: reportMissingImports=false
from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    "obs_create_staging",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng"},
    tags=["observatoire","staging","ddl"],
) as dag:

    # 1) Créer la DB OBS_STAGING (depuis master)
    ddl_001 = SQLExecuteQueryOperator(
        task_id="ddl_001_create_db",
        conn_id="mssql_default",
        hook_params={"schema":"master"},
        autocommit=True,
        sql="""
IF DB_ID(N'OBS_STAGING') IS NULL
  CREATE DATABASE [OBS_STAGING];
""",
    )

    # 2) Schémas + tables bronze (raw) et staging typé (stg)
    ddl_010 = SQLExecuteQueryOperator(
        task_id="ddl_010_schema_bronze",
        conn_id="mssql_default",
        hook_params={"schema":"OBS_STAGING"},
        autocommit=True,
        sql="""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='raw') EXEC('CREATE SCHEMA raw');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg');

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

-- staging typé minimal pour la suite
IF OBJECT_ID('stg.PRIX_INDICE_RAW','U') IS NULL
CREATE TABLE stg.PRIX_INDICE_RAW(
  source_file NVARCHAR(400) NULL,
  agglomeration NVARCHAR(200) NULL,
  produit NVARCHAR(200) NULL,
  variete NVARCHAR(200) NULL,
  annee INT NULL,
  type_indice NVARCHAR(50) NULL,
  valeur_indice DECIMAL(18,6) NULL,
  prix_moyen DECIMAL(18,6) NULL
);

IF OBJECT_ID('stg.PONDERATION_VILLE_RAW','U') IS NULL
CREATE TABLE stg.PONDERATION_VILLE_RAW(
  source_file NVARCHAR(400) NULL,
  region NVARCHAR(200) NULL,
  ville  NVARCHAR(200) NULL,
  variete NVARCHAR(200) NULL,
  annee INT NULL,
  poids DECIMAL(18,6) NULL
);

IF OBJECT_ID('stg.PONDERATION_REGION_RAW','U') IS NULL
CREATE TABLE stg.PONDERATION_REGION_RAW(
  source_file NVARCHAR(400) NULL,
  region NVARCHAR(200) NULL,
  variete NVARCHAR(200) NULL,
  annee INT NULL,
  poids DECIMAL(18,6) NULL
);
""",
    )

    # 3) Sanity check
    ddl_check = SQLExecuteQueryOperator(
        task_id="ddl_check",
        conn_id="mssql_default",
        hook_params={"schema":"OBS_STAGING"},
        autocommit=True,
        sql="""
SELECT s.name AS schema_name, t.name AS table_name
FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id
ORDER BY 1,2;
""",
    )

    ddl_001 >> ddl_010 >> ddl_check

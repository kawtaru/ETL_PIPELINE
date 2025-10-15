from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import json

TARGET_DB = "ConstructionDW"
SCHEMA = "dw"   # on crée un schéma logique pour nos tables

def _run_sql(hook: MsSqlHook, sql: str, autocommit: bool = True):
    # MsSqlHook.run accepte soit une chaîne soit une liste de requêtes
    hook.run(sql, autocommit=autocommit)

@task
def create_database_if_missing():
    """
    Se connecte à master et crée la base si elle n'existe pas.
    Idempotent.
    """
    master = MsSqlHook(mssql_conn_id="mssql_default", schema="master")
    sql = f"""
    IF DB_ID(N'{TARGET_DB}') IS NULL
    BEGIN
        PRINT 'Creating database [{TARGET_DB}]...';
        CREATE DATABASE [{TARGET_DB}];
    END
    ELSE
    BEGIN
        PRINT 'Database [{TARGET_DB}] already exists.';
    END
    """
    _run_sql(master, sql)
    return {"database": TARGET_DB, "created": True}

@task
def create_schema_and_tables(ctx: dict):
    """
    Se connecte à la base cible et crée un schéma + 2 tables (idempotent).
    Tables: dw.Projects, dw.Tasks (FK Tasks -> Projects)
    """
    db = ctx["database"]
    hook = MsSqlHook(mssql_conn_id="mssql_default", schema=db)

    sql_statements = f"""
    -- 1) Créer le schéma s'il n'existe pas
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{SCHEMA}')
        EXEC('CREATE SCHEMA {SCHEMA}');

    -- 2) Créer dw.Projects si absent
    IF OBJECT_ID(N'{SCHEMA}.Projects', N'U') IS NULL
    BEGIN
        CREATE TABLE {SCHEMA}.Projects
        (
            ProjectID       INT             IDENTITY(1,1) PRIMARY KEY,
            ProjectCode     VARCHAR(50)     NOT NULL UNIQUE,
            ProjectName     VARCHAR(200)    NOT NULL,
            StartDate       DATE            NULL,
            EndDate         DATE            NULL,
            CreatedAt       DATETIME2(0)    NOT NULL DEFAULT SYSUTCDATETIME(),
            UpdatedAt       DATETIME2(0)    NULL
        );

        CREATE INDEX IX_{SCHEMA}_Projects_ProjectName ON {SCHEMA}.Projects(ProjectName);
    END

    -- 3) Créer dw.Tasks si absent
    IF OBJECT_ID(N'{SCHEMA}.Tasks', N'U') IS NULL
    BEGIN
        CREATE TABLE {SCHEMA}.Tasks
        (
            TaskID          INT             IDENTITY(1,1) PRIMARY KEY,
            ProjectID       INT             NOT NULL,
            TaskName        VARCHAR(200)    NOT NULL,
            Status          VARCHAR(30)     NOT NULL DEFAULT 'Pending',
            DueDate         DATE            NULL,
            CreatedAt       DATETIME2(0)    NOT NULL DEFAULT SYSUTCDATETIME(),
            UpdatedAt       DATETIME2(0)    NULL,

            CONSTRAINT FK_{SCHEMA}_Tasks_Projects
                FOREIGN KEY (ProjectID) REFERENCES {SCHEMA}.Projects(ProjectID)
                ON DELETE CASCADE
        );

        CREATE INDEX IX_{SCHEMA}_Tasks_Project_Status ON {SCHEMA}.Tasks(ProjectID, Status);
    END
    """
    _run_sql(hook, sql_statements)

    # (facultatif) une petite insertion de test, seulement si tables vides
    seed_sql = f"""
    IF NOT EXISTS (SELECT 1 FROM {SCHEMA}.Projects)
    BEGIN
        INSERT INTO {SCHEMA}.Projects (ProjectCode, ProjectName, StartDate)
        VALUES ('PRJ-001','Demo Project','2025-01-01');

        INSERT INTO {SCHEMA}.Tasks (ProjectID, TaskName, Status, DueDate)
        SELECT p.ProjectID, 'Initial task','Pending','2025-12-31'
        FROM {SCHEMA}.Projects p WHERE p.ProjectCode='PRJ-001';
    END
    """
    _run_sql(hook, seed_sql)

    return {"database": db, "schema": SCHEMA, "status": "OK"}

@task
def show_summary(info: dict):
    print("Creation summary:\n" + json.dumps(info, indent=2))

with DAG(
    dag_id="create_mssql_db_with_two_tables",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # Airflow 2.10: utilise `schedule` (remplace schedule_interval)
    catchup=False,
    tags=["setup","mssql"],
) as dag:
    db_ctx = create_database_if_missing()
    info = create_schema_and_tables(db_ctx)
    show_summary(info)

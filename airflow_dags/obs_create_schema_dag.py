# pyright: reportMissingImports=false
from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    "obs_create_schema",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng"},
    tags=["observatoire", "bootstrap", "ddl"],
) as dag:

    # 1️⃣ Créer la base si elle n'existe pas (connexion à master)
    ddl_001 = SQLExecuteQueryOperator(
        task_id="ddl_001_create_db",
        conn_id="mssql_default",
        autocommit=True,
        hook_params={"schema": "master"},   # ← important when DB was deleted
        sql="sql/observatoire/001_create_db.sql",
)

    # 2️⃣ Schéma géographique
    ddl_010 = SQLExecuteQueryOperator(
        task_id="ddl_010_schema_geo",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql="sql/observatoire/010_schema_geo.sql",
    )

    # 3️⃣ Schéma métier
    ddl_020 = SQLExecuteQueryOperator(
        task_id="ddl_020_schema_metier",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql="sql/observatoire/020_schema_metier.sql",
    )

    # 4️⃣ Schéma des faits
    ddl_030 = SQLExecuteQueryOperator(
        task_id="ddl_030_schema_facts",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql="sql/observatoire/030_schema_facts.sql",
    )

    # 5️⃣ Indexes
    ddl_090 = SQLExecuteQueryOperator(
        task_id="ddl_090_indexes",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql="sql/observatoire/090_indexes.sql",
    )

    # 6️⃣ Vérification finale de cohérence / santé
    ddl_999 = SQLExecuteQueryOperator(
        task_id="ddl_999_checks",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql="sql/observatoire/999_checks.sql",
    )

    # 7️⃣ Nouvelle tâche : vérifie les tables créées et les log dans Airflow
    ddl_check_tables = SQLExecuteQueryOperator(
        task_id="ddl_check_tables",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql="""
        SELECT 
            t.name AS table_name, 
            s.name AS schema_name, 
            p.rows AS row_count
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
        GROUP BY t.name, s.name, p.rows
        ORDER BY schema_name, table_name;
        """,
    )

    # Orchestration complète
    ddl_001 >> ddl_010 >> ddl_020 >> ddl_030 >> ddl_090 >> ddl_999 >> ddl_check_tables

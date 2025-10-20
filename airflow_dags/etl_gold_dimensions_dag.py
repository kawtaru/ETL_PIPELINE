# pyright: reportMissingImports=false
from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# This DAG promotes referentials from SILVER staging (stg.*_REF) into GOLD dims
# and links villes when possible. It is safe to re-run (idempotent MERGE/UPSERT).

DDL_REGION = """
-- Upsert REGION from stg.REGION_REF
;WITH src AS (
  SELECT 
    -- Coerce codes like '1.0' -> '1'
    LTRIM(RTRIM(REPLACE(REPLACE(region_code, '.0',''), ' ', ''))) AS code_region,
    LTRIM(RTRIM(region_name)) AS nom_region
  FROM stg.REGION_REF
  WHERE NULLIF(LTRIM(RTRIM(region_name)),'') IS NOT NULL
)
MERGE dbo.REGION AS t
USING src AS s
  ON t.code_region = s.code_region
WHEN MATCHED THEN UPDATE SET t.nom_region = s.nom_region
WHEN NOT MATCHED THEN INSERT(code_region, nom_region) VALUES (s.code_region, s.nom_region);
"""

DDL_PROVINCE = """
-- Upsert PROVINCE_PREFECTURE from stg.PROVINCE_REF; region is optional
;WITH src AS (
  SELECT 
    LTRIM(RTRIM(REPLACE(REPLACE(province_code, '.0',''), ' ', ''))) AS code_province_prefecture,
    LTRIM(RTRIM(province_name)) AS nom_province_prefecture,
    LTRIM(RTRIM(REPLACE(REPLACE(region_code, '.0',''), ' ', ''))) AS code_region
  FROM stg.PROVINCE_REF
  WHERE NULLIF(LTRIM(RTRIM(province_name)),'') IS NOT NULL
)
MERGE dbo.PROVINCE_PREFECTURE AS t
USING (
  SELECT s.code_province_prefecture, s.nom_province_prefecture, r.id_region
  FROM src s
  LEFT JOIN dbo.REGION r ON r.code_region = s.code_region
) m
  ON t.code_province_prefecture = m.code_province_prefecture
WHEN MATCHED THEN 
  UPDATE SET t.nom_province_prefecture = m.nom_province_prefecture,
             t.id_region = ISNULL(m.id_region, t.id_region)
WHEN NOT MATCHED THEN 
  INSERT(code_province_prefecture, nom_province_prefecture, id_region)
  VALUES (m.code_province_prefecture, m.nom_province_prefecture, m.id_region);
"""

DDL_COMMUNE = """
-- Upsert COMMUNE from stg.COMMUNE_REF when a province mapping exists
;WITH src AS (
  SELECT 
    LTRIM(RTRIM(REPLACE(REPLACE(commune_code, '.0',''), ' ', ''))) AS code_commune,
    LTRIM(RTRIM(commune_name)) AS nom_commune,
    LTRIM(RTRIM(REPLACE(REPLACE(province_code, '.0',''), ' ', ''))) AS code_province_prefecture
  FROM stg.COMMUNE_REF
  WHERE NULLIF(LTRIM(RTRIM(commune_name)),'') IS NOT NULL
)
MERGE dbo.COMMUNE AS t
USING (
  SELECT s.code_commune, s.nom_commune, p.id_province_prefecture
  FROM src s
  JOIN dbo.PROVINCE_PREFECTURE p ON p.code_province_prefecture = s.code_province_prefecture
) m
  ON t.code_commune = m.code_commune
WHEN MATCHED THEN 
  UPDATE SET t.nom_commune = m.nom_commune,
             t.id_province_prefecture = m.id_province_prefecture
WHEN NOT MATCHED THEN 
  INSERT(code_commune, nom_commune, id_province_prefecture)
  VALUES (m.code_commune, m.nom_commune, m.id_province_prefecture);
"""

DDL_LINK_VILLE = """
-- Heuristic link: if a VILLE has same name as a COMMUNE, attach province
UPDATE v
SET v.id_province_prefecture = c.id_province_prefecture
FROM dbo.VILLE v
JOIN dbo.COMMUNE c ON c.nom_commune = v.nom_ville
WHERE v.id_province_prefecture IS NULL;
"""

with DAG(
    dag_id="etl_gold_dimensions",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng"},
    tags=["observatoire","gold","dims"],
) as dag:

    up_region = SQLExecuteQueryOperator(
        task_id="upsert_region",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql=DDL_REGION,
    )

    up_province = SQLExecuteQueryOperator(
        task_id="upsert_province",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql=DDL_PROVINCE,
    )

    up_commune = SQLExecuteQueryOperator(
        task_id="upsert_commune",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql=DDL_COMMUNE,
    )

    link_ville = SQLExecuteQueryOperator(
        task_id="link_ville_province",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql=DDL_LINK_VILLE,
    )

    up_region >> up_province >> [up_commune, link_ville]


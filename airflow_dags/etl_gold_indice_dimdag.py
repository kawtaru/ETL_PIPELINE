# pyright: reportMissingImports=false
"""
etl_gold_dimensions_dag.py - CORRECTED WITH ACTUAL COLUMN NAMES
Gold layer: Load dimension tables from Bronze/Silver layers
"""

import logging
from datetime import datetime

from sqlalchemy import text
from airflow import DAG
from airflow.operators.python import PythonOperator

import config
import db_utils

logger = logging.getLogger(__name__)


# =====================================================================
# TASKS
# =====================================================================
def task_load_regions(**ctx):
    """Load REGION dimension from staging"""
    logger.info("Loading REGION dimension...")
    
    sql = """
    USE [OBSERVATOIRE];
    
    INSERT INTO dbo.REGION (code_region, nom_region)
    SELECT DISTINCT 
        UPPER(LTRIM(RTRIM(region_code))) as code_region,
        UPPER(LTRIM(RTRIM(region_name))) as nom_region
    FROM OBS_STAGING.stg.REGION_REF
    WHERE region_code IS NOT NULL AND region_code != ''
      AND region_name IS NOT NULL AND region_name != ''
    AND NOT EXISTS (
        SELECT 1 FROM dbo.REGION r 
        WHERE UPPER(r.code_region) = UPPER(LTRIM(RTRIM(OBS_STAGING.stg.REGION_REF.region_code)))
    );
    """
    
    db_utils.execute_sql_with_transaction(sql, schema=config.STAGING_DB)
    
    engine = db_utils.make_sqlalchemy_engine(schema="OBSERVATOIRE")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) as cnt FROM dbo.REGION"))
        count = result.scalar()
    
    logger.info(f"✓ Loaded {count} regions")
    ctx["ti"].xcom_push(key="regions_loaded", value=count)


def task_load_provinces(**ctx):
    """Load PROVINCE_PREFECTURE dimension from staging"""
    logger.info("Loading PROVINCE_PREFECTURE dimension...")
    
    sql = """
    USE [OBSERVATOIRE];
    
    INSERT INTO dbo.PROVINCE_PREFECTURE (code_province_prefecture, nom_province_prefecture, id_region)
    SELECT DISTINCT
        UPPER(LTRIM(RTRIM(prov.province_code))) as code_province_prefecture,
        UPPER(LTRIM(RTRIM(prov.province_name))) as nom_province_prefecture,
        ISNULL(reg.id_region, (SELECT TOP 1 id_region FROM dbo.REGION ORDER BY id_region)) as id_region
    FROM OBS_STAGING.stg.PROVINCE_REF prov
    LEFT JOIN dbo.REGION reg 
        ON UPPER(LTRIM(RTRIM(reg.code_region))) = UPPER(LTRIM(RTRIM(prov.region_code)))
    WHERE prov.province_code IS NOT NULL AND prov.province_code != ''
      AND prov.province_name IS NOT NULL AND prov.province_name != ''
    AND NOT EXISTS (
        SELECT 1 FROM dbo.PROVINCE_PREFECTURE p 
        WHERE UPPER(p.code_province_prefecture) = UPPER(LTRIM(RTRIM(prov.province_code)))
    );
    """
    
    db_utils.execute_sql_with_transaction(sql, schema=config.STAGING_DB)
    
    engine = db_utils.make_sqlalchemy_engine(schema="OBSERVATOIRE")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) as cnt FROM dbo.PROVINCE_PREFECTURE"))
        count = result.scalar()
    
    logger.info(f"✓ Loaded {count} provinces/prefectures")
    ctx["ti"].xcom_push(key="provinces_loaded", value=count)


def task_load_communes(**ctx):
    """Load COMMUNE dimension from staging"""
    logger.info("Loading COMMUNE dimension...")
    
    sql = """
    USE [OBSERVATOIRE];
    
    INSERT INTO dbo.COMMUNE (code_commune, nom_commune, id_province_prefecture)
    SELECT DISTINCT
        UPPER(LTRIM(RTRIM(comm.commune_code))) as code_commune,
        UPPER(LTRIM(RTRIM(comm.commune_name))) as nom_commune,
        ISNULL(prov.id_province_prefecture, (SELECT TOP 1 id_province_prefecture FROM dbo.PROVINCE_PREFECTURE ORDER BY id_province_prefecture)) as id_province_prefecture
    FROM OBS_STAGING.stg.COMMUNE_REF comm
    LEFT JOIN dbo.PROVINCE_PREFECTURE prov 
        ON UPPER(LTRIM(RTRIM(prov.code_province_prefecture))) = UPPER(LTRIM(RTRIM(comm.province_code)))
    WHERE comm.commune_code IS NOT NULL AND comm.commune_code != ''
      AND comm.commune_name IS NOT NULL AND comm.commune_name != ''
    AND NOT EXISTS (
        SELECT 1 FROM dbo.COMMUNE c 
        WHERE UPPER(c.code_commune) = UPPER(LTRIM(RTRIM(comm.commune_code)))
    );
    """
    
    db_utils.execute_sql_with_transaction(sql, schema=config.STAGING_DB)
    
    engine = db_utils.make_sqlalchemy_engine(schema="OBSERVATOIRE")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) as cnt FROM dbo.COMMUNE"))
        count = result.scalar()
    
    logger.info(f"✓ Loaded {count} communes")
    ctx["ti"].xcom_push(key="communes_loaded", value=count)


def task_load_activites(**ctx):
    """Load ACTIVITE dimension from PRIX_INDICE_RAW"""
    logger.info("Loading ACTIVITE dimension...")
    
    sql = """
    USE [OBSERVATOIRE];
    
    INSERT INTO dbo.ACTIVITE (nom_activite, id_corps)
    SELECT 
        nom_activite,
        MIN(id_corps) as id_corps
    FROM (
        SELECT DISTINCT
            UPPER(LTRIM(RTRIM(pir.activite))) as nom_activite,
            cm.id_corps
        FROM OBS_STAGING.stg.PRIX_INDICE_RAW pir
        LEFT JOIN dbo.CORPS_METIER cm 
            ON UPPER(cm.nom_corps) = UPPER(LTRIM(RTRIM(pir.corps)))
        WHERE pir.activite IS NOT NULL AND pir.activite != ''
          AND cm.id_corps IS NOT NULL
    ) sub
    GROUP BY nom_activite
    ;
    """
    
    db_utils.execute_sql_with_transaction(sql, schema=config.STAGING_DB)
    
    engine = db_utils.make_sqlalchemy_engine(schema="OBSERVATOIRE")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) as cnt FROM dbo.ACTIVITE"))
        count = result.scalar()
    
    logger.info(f"✓ Loaded {count} activites")
    ctx["ti"].xcom_push(key="activites_loaded", value=count)


def task_load_corps_metier(**ctx):
    """Load CORPS_METIER dimension from PRIX_INDICE_RAW"""
    logger.info("Loading CORPS_METIER dimension...")
    
    sql = """
    USE [OBSERVATOIRE];
    
    INSERT INTO dbo.CORPS_METIER (nom_corps, id_metier)
    SELECT DISTINCT 
        UPPER(LTRIM(RTRIM(pir.corps))) as nom_corps,
        met.id_metier
    FROM OBS_STAGING.stg.PRIX_INDICE_RAW pir
    LEFT JOIN dbo.METIER met 
        ON UPPER(met.nom_metier) = UPPER(LTRIM(RTRIM(pir.corps)))
    WHERE pir.corps IS NOT NULL AND pir.corps != ''
      AND met.id_metier IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM dbo.CORPS_METIER cm 
        WHERE UPPER(cm.nom_corps) = UPPER(LTRIM(RTRIM(pir.corps)))
    );
    """
    
    db_utils.execute_sql_with_transaction(sql, schema=config.STAGING_DB)
    
    engine = db_utils.make_sqlalchemy_engine(schema="OBSERVATOIRE")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) as cnt FROM dbo.CORPS_METIER"))
        count = result.scalar()
    
    logger.info(f"✓ Loaded {count} corps de metier")
    ctx["ti"].xcom_push(key="corps_metier_loaded", value=count)


def task_load_metier(**ctx):
    """Load METIER dimension from PRIX_INDICE_RAW (MUST BE FIRST - no FK)"""
    logger.info("Loading METIER dimension...")
    
    sql = """
    USE [OBSERVATOIRE];
    
    INSERT INTO dbo.METIER (nom_metier)
    SELECT DISTINCT 
        UPPER(LTRIM(RTRIM(corps))) as nom_metier
    FROM OBS_STAGING.stg.PRIX_INDICE_RAW
    WHERE corps IS NOT NULL AND corps != ''
    AND NOT EXISTS (
        SELECT 1 FROM dbo.METIER m 
        WHERE UPPER(m.nom_metier) = UPPER(LTRIM(RTRIM(OBS_STAGING.stg.PRIX_INDICE_RAW.corps)))
    );
    """
    
    db_utils.execute_sql_with_transaction(sql, schema=config.STAGING_DB)
    
    engine = db_utils.make_sqlalchemy_engine(schema="OBSERVATOIRE")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) as cnt FROM dbo.METIER"))
        count = result.scalar()
    
    logger.info(f"✓ Loaded {count} metiers")
    ctx["ti"].xcom_push(key="metier_loaded", value=count)


def task_load_produit(**ctx):
    """Load PRODUIT dimension from PRIX_INDICE_RAW"""
    logger.info("Loading PRODUIT dimension...")
    
    sql = """
    USE [OBSERVATOIRE];
    
    INSERT INTO dbo.PRODUIT (nom_produit, id_activite)
    SELECT DISTINCT 
        UPPER(LTRIM(RTRIM(pir.produit))) as nom_produit,
        act.id_activite
    FROM OBS_STAGING.stg.PRIX_INDICE_RAW pir
    LEFT JOIN dbo.ACTIVITE act 
        ON UPPER(act.nom_activite) = UPPER(LTRIM(RTRIM(pir.activite)))
    WHERE pir.produit IS NOT NULL AND pir.produit != ''
      AND act.id_activite IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM dbo.PRODUIT p 
        WHERE UPPER(p.nom_produit) = UPPER(LTRIM(RTRIM(pir.produit)))
    );
    """
    
    db_utils.execute_sql_with_transaction(sql, schema=config.STAGING_DB)
    
    engine = db_utils.make_sqlalchemy_engine(schema="OBSERVATOIRE")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) as cnt FROM dbo.PRODUIT"))
        count = result.scalar()
    
    logger.info(f"✓ Loaded {count} produits")
    ctx["ti"].xcom_push(key="produit_loaded", value=count)


def task_load_variete(**ctx):
    """Load VARIETE dimension from PRIX_INDICE_RAW"""
    logger.info("Loading VARIETE dimension...")
    
    sql = """
    USE [OBSERVATOIRE];
    
    INSERT INTO dbo.VARIETE (nom_variete, id_produit)
    SELECT 
        nom_variete,
        MIN(id_produit) as id_produit
    FROM (
        SELECT DISTINCT
            UPPER(LTRIM(RTRIM(var.variete))) as nom_variete,
            prod.id_produit
        FROM OBS_STAGING.stg.PRIX_INDICE_RAW var
        LEFT JOIN dbo.PRODUIT prod 
            ON UPPER(prod.nom_produit) = UPPER(LTRIM(RTRIM(var.produit)))
        WHERE var.variete IS NOT NULL AND var.variete != ''
          AND prod.id_produit IS NOT NULL
    ) sub
    GROUP BY nom_variete
    ;
    """
    
    db_utils.execute_sql_with_transaction(sql, schema=config.STAGING_DB)
    
    engine = db_utils.make_sqlalchemy_engine(schema="OBSERVATOIRE")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) as cnt FROM dbo.VARIETE"))
        count = result.scalar()
    
    logger.info(f"✓ Loaded {count} varietes")
    ctx["ti"].xcom_push(key="variete_loaded", value=count)


def task_validate_dimensions(**ctx):
    """Validate dimension tables"""
    logger.info("Validating dimensions...")
    
    tables = [
        'REGION', 'PROVINCE_PREFECTURE', 'COMMUNE', 'ACTIVITE',
        'CORPS_METIER', 'METIER', 'PRODUIT', 'VARIETE'
    ]
    
    engine = db_utils.make_sqlalchemy_engine(schema="OBSERVATOIRE")
    with engine.connect() as conn:
        for table in tables:
            sql = f"SELECT COUNT(*) as cnt FROM dbo.{table}"
            result = conn.execute(text(sql))
            count = result.scalar()
            logger.info(f"  {table}: {count} rows")


def task_report(**ctx):
    """Final report"""
    regions = ctx["ti"].xcom_pull(key="regions_loaded", task_ids="task_load_regions") or 0
    provinces = ctx["ti"].xcom_pull(key="provinces_loaded", task_ids="task_load_provinces") or 0
    communes = ctx["ti"].xcom_pull(key="communes_loaded", task_ids="task_load_communes") or 0
    activites = ctx["ti"].xcom_pull(key="activites_loaded", task_ids="task_load_activites") or 0
    corps_metier = ctx["ti"].xcom_pull(key="corps_metier_loaded", task_ids="task_load_corps_metier") or 0
    metier = ctx["ti"].xcom_pull(key="metier_loaded", task_ids="task_load_metier") or 0
    produit = ctx["ti"].xcom_pull(key="produit_loaded", task_ids="task_load_produit") or 0
    variete = ctx["ti"].xcom_pull(key="variete_loaded", task_ids="task_load_variete") or 0
    
    logger.info(f"""
    ✓ DIMENSIONS LOADED:
      - Regions: {regions}
      - Provinces/Prefectures: {provinces}
      - Communes: {communes}
      - Activites: {activites}
      - Corps de Metier: {corps_metier}
      - Metier: {metier}
      - Produits: {produit}
      - Varietes: {variete}
    """)


# =====================================================================
# DAG
# =====================================================================
with DAG(
    dag_id="etl_gold_dimensions",
    description="Gold layer: Load dimension tables from Bronze/Silver layers",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["gold", "dimensions"],
) as dag:

    t_regions = PythonOperator(task_id="task_load_regions", python_callable=task_load_regions)
    t_provinces = PythonOperator(task_id="task_load_provinces", python_callable=task_load_provinces)
    t_communes = PythonOperator(task_id="task_load_communes", python_callable=task_load_communes)
    t_activites = PythonOperator(task_id="task_load_activites", python_callable=task_load_activites)
    t_corps_metier = PythonOperator(task_id="task_load_corps_metier", python_callable=task_load_corps_metier)
    t_metier = PythonOperator(task_id="task_load_metier", python_callable=task_load_metier)
    t_produit = PythonOperator(task_id="task_load_produit", python_callable=task_load_produit)
    t_variete = PythonOperator(task_id="task_load_variete", python_callable=task_load_variete)
    t_validate = PythonOperator(task_id="task_validate_dimensions", python_callable=task_validate_dimensions)
    t_report = PythonOperator(task_id="task_report", python_callable=task_report)

    # Geography chain: REGION >> PROVINCE >> COMMUNE
    t_regions >> t_provinces >> t_communes
    
    # Métiers chain: METIER >> CORPS_METIER >> ACTIVITE >> PRODUIT >> VARIETE
    t_metier >> t_corps_metier >> t_activites >> t_produit >> t_variete
    
    # All dimension loads complete, then validate and report
    [t_communes, t_variete] >> t_validate >> t_report
# pyright: reportMissingImports=false
"""
config.py - Centralized configuration for ETL pipeline
Database, paths, logging, and all constants in one place.
"""

import os
from pathlib import Path
from typing import Optional

# =====================================================================
# ENVIRONMENT & PATHS
# =====================================================================
BASE_DIR = Path(os.environ.get("ETL_BASE_DIR", "/opt/etl/data"))
RAW_DIR = BASE_DIR / "raw" / "prix_indice"
LANDING_DIR = BASE_DIR / "landing" / "bronze_raw"
ARCHIVE_DIR = BASE_DIR / "archive" / "bronze_raw"
REF_DIR = BASE_DIR / "referentiels"
PONDERATION_DIR = BASE_DIR / "ponderations"

# Create directories if missing
for d in [RAW_DIR, LANDING_DIR, ARCHIVE_DIR, REF_DIR, PONDERATION_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# =====================================================================
# DATABASE CONFIGURATION
# =====================================================================
MSSQL_CONN_ID = "mssql_default"

# Database names
GOLD_DB = "OBSERVATOIRE"
STAGING_DB = "OBS_STAGING"
RAW_DB = "OBS_RAW"

# Schemas
SCHEMA_BRONZE = "raw"
SCHEMA_SILVER = "stg"
SCHEMA_GOLD_GEO = "dbo"
SCHEMA_GOLD_METIER = "dbo"
SCHEMA_GOLD_FACTS = "dbo"

# =====================================================================
# TABLE NAMES
# =====================================================================
# Bronze (Raw data ingestion)
BRONZE_FILES = f"{SCHEMA_BRONZE}.RAW_FILES"
BRONZE_ROWS = f"{SCHEMA_BRONZE}.RAW_ROWS"

# Silver (Staging / Transformation)
SILVER_PRIX_INDICE_RAW = f"{SCHEMA_SILVER}.PRIX_INDICE_RAW"
SILVER_REGION_REF = f"{SCHEMA_SILVER}.REGION_REF"
SILVER_PROVINCE_REF = f"{SCHEMA_SILVER}.PROVINCE_REF"
SILVER_COMMUNE_REF = f"{SCHEMA_SILVER}.COMMUNE_REF"
SILVER_AGGLOMERATION_MAPPING = f"{SCHEMA_SILVER}.AGGLOMERATION_MAPPING"
SILVER_PONDERATION_VILLE = f"{SCHEMA_SILVER}.PONDERATION_VILLE"
SILVER_PONDERATION_MC = f"{SCHEMA_SILVER}.PONDERATION_MC"

# Gold (Production dimensions & facts)
GOLD_REGION = f"{SCHEMA_GOLD_GEO}.REGION"
GOLD_PROVINCE_PREFECTURE = f"{SCHEMA_GOLD_GEO}.PROVINCE_PREFECTURE"
GOLD_COMMUNE = f"{SCHEMA_GOLD_GEO}.COMMUNE"
GOLD_METIER = f"{SCHEMA_GOLD_GEO}.METIER"
GOLD_CORPS_METIER = f"{SCHEMA_GOLD_GEO}.CORPS_METIER"
GOLD_ACTIVITE = f"{SCHEMA_GOLD_GEO}.ACTIVITE"
GOLD_PRODUIT = f"{SCHEMA_GOLD_GEO}.PRODUIT"
GOLD_VARIETE = f"{SCHEMA_GOLD_GEO}.VARIETE"
GOLD_INDICE = f"{SCHEMA_GOLD_GEO}.INDICE"
GOLD_PONDERATION = f"{SCHEMA_GOLD_GEO}.PONDERATION"

# =====================================================================
# FILE PATTERNS & RULES
# =====================================================================
# CSV Encodings to try (in order)
CSV_ENCODINGS = ["utf-8", "utf-8-sig", "cp1252", "latin1", "iso-8859-1"]

# CSV Separators to try (in order)
CSV_SEPARATORS = [";", ",", "\t", "|"]

# File type detection
FILE_TYPE_T1 = "T1"  # Prix par variété
FILE_TYPE_T3 = "T3"  # Indice par produit
FILE_TYPE_UNKNOWN = "UNKNOWN"

# Level detection
NIVEAU_NATIONAL = "NATIONAL"
NIVEAU_REGION = "REGION"
NIVEAU_COMMUNE = "COMMUNE"

# =====================================================================
# DATA QUALITY & VALIDATION
# =====================================================================
# Minimum expected row counts after load (for QC)
MIN_REGIONS = 10
MIN_PROVINCES = 50
MIN_COMMUNES = 500
MIN_INDICE = 1000
MIN_PONDERATION = 10

# =====================================================================
# AIRFLOW DAG DEFAULTS
# =====================================================================
DAG_DEFAULTS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay_sec": 60,
}

DAG_TAGS = {
    "bronze": ["observatoire", "bronze", "raw"],
    "silver": ["observatoire", "silver"],
    "gold": ["observatoire", "gold"],
}

# =====================================================================
# LOGGING
# =====================================================================
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# =====================================================================
# PERFORMANCE
# =====================================================================
BATCH_SIZE = 1000
FAST_EXECUTEMANY = True
USE_SETINPUTSIZES = False

# =====================================================================
# ODBC DRIVER CANDIDATES
# =====================================================================
ODBC_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13.1 for SQL Server",
    "ODBC Driver 11 for SQL Server",
]

# =====================================================================
# HIERARCHY CODES (Morocco administrative)
# =====================================================================
# Region codes are 1-2 digits: 1, 2, ..., 10, 11, 12
REGION_CODES_2DIGIT = {"10", "11", "12"}

# Province codes are typically 4-5 digits
# Commune codes are typically 5-6 digits (all have province prefix)
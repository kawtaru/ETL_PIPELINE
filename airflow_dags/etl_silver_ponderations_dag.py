# pyright: reportMissingImports=false
from datetime import datetime
import os, re, unicodedata
from pathlib import Path
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

# Compat SqlHook
try:
    from airflow.providers.common.sql.hooks.sql import SqlHook
except Exception:
    try:
        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook as SqlHook
    except Exception:
        from airflow.providers.common.sql.hooks.sql import DbApiHook as SqlHook

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
STAGING_DB = os.environ.get("ETL_STAGING_DB", "OBS_STAGING")
BASE_DIR   = os.environ.get("ETL_BASE_DIR", "/opt/etl/data")
POND_DIR   = os.path.join(BASE_DIR, "ponderations")

# ---------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------
DDL = r"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg');

IF OBJECT_ID('stg.POND_VILLE_BASE','U') IS NULL
CREATE TABLE stg.POND_VILLE_BASE(
  ville_raw NVARCHAR(200) NOT NULL,
  region_raw NVARCHAR(200) NULL,
  variete_raw NVARCHAR(200) NULL,
  poids DECIMAL(18,6) NOT NULL,
  source_file NVARCHAR(256) NULL,
  load_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('stg.POND_VARIETE_BASE','U') IS NULL
CREATE TABLE stg.POND_VARIETE_BASE(
  variete_raw NVARCHAR(200) NOT NULL,
  poids DECIMAL(18,6) NOT NULL,
  source_file NVARCHAR(256) NULL,
  load_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('stg.PONDERATION_VILLE_RAW','U') IS NULL
CREATE TABLE stg.PONDERATION_VILLE_RAW(
  ville NVARCHAR(200) NOT NULL,
  variete NVARCHAR(200) NOT NULL,
  annee INT NOT NULL,
  poids DECIMAL(18,6) NOT NULL,
  source_file NVARCHAR(256) NULL,
  load_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_POND_VILLE_RAW PRIMARY KEY (ville, variete, annee)
);

IF OBJECT_ID('stg.PONDERATION_REGION_RAW','U') IS NULL
CREATE TABLE stg.PONDERATION_REGION_RAW(
  region NVARCHAR(200) NOT NULL,
  variete NVARCHAR(200) NOT NULL,
  annee INT NOT NULL,
  poids DECIMAL(18,6) NOT NULL,
  source_file NVARCHAR(256) NULL,
  load_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_POND_REGION_RAW PRIMARY KEY (region, variete, annee)
);
"""

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------
def _norm(s):
    """Remove accents, compress spaces, uppercase."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s).strip()
    return s.upper()

def _find_one(patterns):
    for pat in patterns:
        files = sorted(Path(POND_DIR).glob(pat))
        if files:
            return str(files[0])
    return None

def _read_any_table(path):
    if path is None:
        return None
    if path.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(path, dtype=str).fillna("")
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(path, sep=None, engine="python", dtype=str, encoding=enc).fillna("")
        except Exception:
            continue
    raise ValueError(f"Cannot read file: {path}")

# ---------------------------------------------------------------------
# TASKS
# ---------------------------------------------------------------------
def ddl(**_):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    hook.run(DDL)
    print("✅ DDL ensured for ponderation tables.")

def load_city_base(**ctx):
    """Load ponderations_villes_regions_output.(csv|xlsx)"""
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    city_path = _find_one([
        "ponderations_villes_regions_output.*",
        "ponderations*ville*region*.*",
        "*villes*regions*pond*.*"
    ])
    if not city_path:
        print("⚠️ No city ponderation file found in", POND_DIR)
        ctx["ti"].xcom_push(key="city_rows", value=0)
        return

    df = _read_any_table(city_path)

    # Normalize headers (remove accents + spaces)
    def _norm_header(h):
        s = unicodedata.normalize("NFKD", str(h))
        s = "".join(c for c in s if not unicodedata.combining(c))
        s = s.strip().lower()
        s = re.sub(r"\s+", " ", s)
        return s

    norm2orig = {_norm_header(c): c for c in df.columns}

    def pick(*names):
        for n in names:
            key = _norm_header(n)
            if key in norm2orig:
                return norm2orig[key]
        for key in norm2orig:
            if any(n in key for n in names):
                return norm2orig[key]
        return None

    c_region = pick("region", "région")
    c_ville  = pick("agglomeration", "agglomération", "ville")
    c_poids  = pick("poids", "pond", "weight")

    if not (c_ville and c_poids):
        raise ValueError(f"[Ponderations/Ville] Missing required columns in {city_path}. Got={df.columns.tolist()}")

    out = pd.DataFrame({
        "ville_raw":   df[c_ville].astype(str).str.strip(),
        "region_raw":  (df[c_region].astype(str).str.strip() if c_region else ""),
        "variete_raw": "",
        "poids":       pd.to_numeric(df[c_poids].astype(str).str.replace(",", "."), errors="coerce"),
        "source_file": city_path
    })
    out = out[(out["ville_raw"]!="") & (out["poids"].notna())]
    out["poids"] = out["poids"].astype(float)

    hook.run("TRUNCATE TABLE stg.POND_VILLE_BASE;")
    engine = hook.get_sqlalchemy_engine()
    out.to_sql("POND_VILLE_BASE", engine, schema="stg", if_exists="append", index=False)
    ctx["ti"].xcom_push(key="city_rows", value=len(out))
    print(f"✅ Loaded {len(out)} rows from {city_path}")

def load_variete_base(**ctx):
    """Load ponderation_MC_prixindice.(xlsx|csv)"""
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    var_path = _find_one([
        "ponderation_MC_prixindice.*",
        "ponderation*prixind*.*",
        "*variet*pond*.*"
    ])
    if not var_path:
        print("⚠️ No varieté ponderation file found in", POND_DIR)
        ctx["ti"].xcom_push(key="var_rows", value=0)
        return

    df = _read_any_table(var_path)
    def _norm_header(h):
        s = unicodedata.normalize("NFKD", str(h))
        s = "".join(c for c in s if not unicodedata.combining(c))
        return re.sub(r"\s+", " ", s.strip().lower())

    norm2orig = {_norm_header(c): c for c in df.columns}
    c_variete = next((norm2orig[k] for k in norm2orig if "variet" in k or "variété" in k), None)
    c_poids   = next((norm2orig[k] for k in norm2orig if "poids" in k or "pond" in k or "weight" in k), None)

    if not (c_variete and c_poids):
        raise ValueError(f"[Ponderations/Variete] Columns not found in {var_path}: {df.columns.tolist()}")

    out = pd.DataFrame({
        "variete_raw": df[c_variete].astype(str).str.strip(),
        "poids": pd.to_numeric(df[c_poids].astype(str).str.replace(",", "."), errors="coerce"),
        "source_file": var_path
    }).dropna(subset=["variete_raw","poids"])

    hook.run("TRUNCATE TABLE stg.POND_VARIETE_BASE;")
    engine = hook.get_sqlalchemy_engine()
    out.to_sql("POND_VARIETE_BASE", engine, schema="stg", if_exists="append", index=False)
    ctx["ti"].xcom_push(key="var_rows", value=len(out))
    print(f"✅ Loaded {len(out)} varieté rows from {var_path}")

def build_ville_region_raw(**ctx):
    """Cross city × varieté × year, normalize, build both raw tables"""
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    engine = hook.get_sqlalchemy_engine()
    df_city = pd.read_sql("SELECT * FROM stg.POND_VILLE_BASE", engine)
    df_var  = pd.read_sql("SELECT * FROM stg.POND_VARIETE_BASE", engine)

    if df_city.empty:
        print("⚠️ No city ponderations loaded.")
        return

    if df_var.empty:
        raise ValueError("Variety base empty — cannot build ponderations.")

    # Cross-join (static weights)
    df_city["__key"] = 1
    df_var["__key"] = 1
    df = df_city.merge(df_var, on="__key", suffixes=("_city","_var")).drop(columns="__key")
    df["poids"] = df["poids_city"] * df["poids_var"]

    for col in ["ville_raw","region_raw","variete_raw","variete_raw_var"]:
        if col in df.columns:
            df[col] = df[col].apply(_norm)

    # Expand to all years in raw prices
    years = pd.read_sql("SELECT DISTINCT annee FROM stg.PRIX_INDICE_RAW ORDER BY annee", engine)["annee"].dropna().astype(int).tolist()
    df_years = pd.DataFrame({"annee": years})
    df_years["__key"] = 1
    df["__key"] = 1
    df = df.merge(df_years, on="__key").drop(columns="__key")

    # Normalize per (ville, annee)
    df["sum_p"] = df.groupby(["ville_raw","annee"])["poids"].transform("sum")
    df["poids"] = df["poids"] / df["sum_p"]
    # Use whichever varieté column exists
    var_col = "variete_raw_var" if "variete_raw_var" in df.columns else "variete_raw"
    out_ville = df[["ville_raw", var_col, "annee", "poids", "source_file_city"]]\
        .rename(columns={"ville_raw":"ville", var_col:"variete", "source_file_city":"source_file"})

    # Aggregate by region
    has_region = (df["region_raw"].fillna("").str.strip() != "").any()
    if has_region:
        df_r = df[df["region_raw"]!=""].copy()
        df_r["sum_r"] = df_r.groupby(["region_raw","annee"])["poids"].transform("sum")
        df_r["poids"] = df_r["poids"] / df_r["sum_r"]
        out_region = df_r[["region_raw", var_col, "annee", "poids", "source_file_city"]]\
            .rename(columns={"region_raw":"region", var_col:"variete", "source_file_city":"source_file"})
    else:
        out_region = pd.DataFrame(columns=["region","variete","annee","poids","source_file"])

    # --- Deduplicate before insert to avoid PK violations ---
    out_ville = (
    out_ville.groupby(["ville", "variete", "annee"], as_index=False)
    .agg({"poids": "mean", "source_file": "first"})
    )

    hook.run("TRUNCATE TABLE stg.PONDERATION_VILLE_RAW;")
    out_ville.to_sql("PONDERATION_VILLE_RAW", engine, schema="stg", if_exists="append", index=False)

    # Deduplicate region as well
    out_region = (
        out_region.groupby(["region", "variete", "annee"], as_index=False)
        .agg({"poids": "mean", "source_file": "first"})
    )

    hook.run("TRUNCATE TABLE stg.PONDERATION_REGION_RAW;")
    if not out_region.empty:
        out_region.to_sql("PONDERATION_REGION_RAW", engine, schema="stg", if_exists="append", index=False)

    print(f"✅ VILLE_RAW={len(out_ville)}, REGION_RAW={len(out_region)}")

def report(**ctx):
    print("✅ Ponderation DAG completed successfully.")
# ---------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------
with DAG(
    "etl_silver_ponderations",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    default_args={"owner":"data-eng"},
    tags=["observatoire","silver","ponderations"]
) as dag:
    tddl   = PythonOperator(task_id="ddl", python_callable=ddl)
    tcity  = PythonOperator(task_id="load_city_base", python_callable=load_city_base)
    tvar   = PythonOperator(task_id="load_variete_base", python_callable=load_variete_base)
    tbuild = PythonOperator(task_id="build_ville_region_raw", python_callable=build_ville_region_raw)
    trep   = PythonOperator(task_id="report", python_callable=report)

    tddl >> [tcity, tvar] >> tbuild >> trep

# pyright: reportMissingImports=false
from datetime import datetime
import json, re, os, unicodedata
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Compat: SqlHook may differ by provider; fall back gracefully.
try:
    from airflow.providers.common.sql.hooks.sql import SqlHook
except Exception:
    try:
        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook as SqlHook
    except Exception:
        try:
            from airflow.providers.common.sql.hooks.sql import DbApiHook as SqlHook
        except Exception:
            from airflow.hooks.dbapi import DbApiHook as SqlHook

STAGING_DB = os.environ.get("ETL_STAGING_DB", "OBS_STAGING")

# -----------------------------------------------------------------------------
# DDL (idempotent)
# -----------------------------------------------------------------------------
DDL = r"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg');

IF OBJECT_ID('stg.REGION_REF','U') IS NULL
CREATE TABLE stg.REGION_REF(
  region_code NVARCHAR(50) NOT NULL,
  region_name NVARCHAR(200) NOT NULL,
  load_ts     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_REGION_REF PRIMARY KEY(region_code)
);

IF OBJECT_ID('stg.PROVINCE_REF','U') IS NULL
CREATE TABLE stg.PROVINCE_REF(
  province_code NVARCHAR(50) NOT NULL,
  province_name NVARCHAR(200) NOT NULL,
  region_code   NVARCHAR(50) NULL,
  load_ts       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_PROVINCE_REF PRIMARY KEY(province_code)
);

IF OBJECT_ID('stg.COMMUNE_REF','U') IS NULL
CREATE TABLE stg.COMMUNE_REF(
  commune_code  NVARCHAR(50) NOT NULL,
  commune_name  NVARCHAR(200) NOT NULL,
  province_code NVARCHAR(50) NULL,
  load_ts       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_COMMUNE_REF PRIMARY KEY(commune_code)
);
"""

# Ensure nullability if older runs created NOT NULL FKs
DDL_ALTER_NULLS = r"""
IF OBJECT_ID('stg.PROVINCE_REF','U') IS NOT NULL AND EXISTS(
  SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('stg.PROVINCE_REF') AND name='region_code' AND is_nullable=0)
  ALTER TABLE stg.PROVINCE_REF ALTER COLUMN region_code NVARCHAR(50) NULL;

IF OBJECT_ID('stg.COMMUNE_REF','U') IS NOT NULL AND EXISTS(
  SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('stg.COMMUNE_REF') AND name='province_code' AND is_nullable=0)
  ALTER TABLE stg.COMMUNE_REF ALTER COLUMN province_code NVARCHAR(50) NULL;
"""

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _latest_run_id(hook: SqlHook) -> str:
    rid = hook.get_first("SELECT TOP 1 run_id FROM raw.RAW_FILES ORDER BY load_ts DESC;")[0]
    if not rid:
        raise ValueError("No run_id found in raw.RAW_FILES")
    return rid

def _normalize_header(s: str) -> str:
    """ascii, lower, underscores, strip punctuation → for resilient column matching."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9a-z_]+", "", s)
    return s

def _read_dataset(hook: SqlHook, run_id: str, like_pattern: str) -> pd.DataFrame:
    """
    Read referential CSV rows Bronze ingested into raw.RAW_ROWS (dataset='referentiels').
    We normalize headers aggressively to cope with e.g. 'Code commune', 'Noms_Comm', etc.
    """
    sql = """
    SELECT rel_path, row_index, row_json
    FROM raw.RAW_ROWS
    WHERE run_id = %(r)s AND dataset='referentiels' AND rel_path LIKE %(pat)s
    ORDER BY rel_path, row_index;
    """
    df_rows = hook.get_pandas_df(sql, parameters={"r": run_id, "pat": like_pattern})
    if df_rows.empty:
        return df_rows

    payload = df_rows["row_json"].apply(lambda s: json.loads(s or "{}"))
    df = pd.json_normalize(payload).fillna("")
    df.columns = [_normalize_header(c) for c in df.columns]
    return df

def _first_col(df: pd.DataFrame, *cands) -> str | None:
    """
    Choose the first column whose normalized name contains any candidate token.
    We iterate candidates in priority order, then columns, to reduce false matches.
    """
    cols = list(df.columns)
    for pat in cands:
        pat_n = _normalize_header(pat)
        for c in cols:
            if pat_n and pat_n in c:
                return c
    return None

def _has_letters(s: str) -> bool:
    if s is None:
        return False
    # normaliser et vérifier s'il y a au moins une lettre (latin ou accentué)
    s2 = unicodedata.normalize("NFKD", str(s))
    s2 = "".join(ch for ch in s2 if not unicodedata.combining(ch))
    return bool(re.search(r"[A-Za-z]", s2))

def _pick_name_column(df: pd.DataFrame, candidates: list[str], code_series: pd.Series) -> str | None:
    """
    Retourne la colonne 'nom' qui:
      - existe dans le DF (via _first_col),
      - contient des lettres pour >50% des lignes,
      - n'est pas (majoritairement) identique au code.
    Essaie chaque candidat dans l'ordre, puis tente un fallback: 1ère colonne
    qui contient des lettres.
    """
    # 1) Essayer les candidats connus
    for cand in candidates:
        c = _first_col(df, cand)
        if not c:
            continue
        vals = df[c].astype(str)
        # % lignes avec au moins une lettre
        pct_letters = (vals.apply(_has_letters)).mean()
        # % lignes identiques au code
        pct_equal_code = (vals.fillna("").str.strip().values == code_series.fillna("").astype(str).str.strip().values).mean()
        if pct_letters >= 0.5 and pct_equal_code < 0.5:
            return c

    # 2) Fallback: choisir la première colonne qui a des lettres pour >50%
    for c in df.columns:
        vals = df[c].astype(str)
        if (vals.apply(_has_letters)).mean() >= 0.5:
            return c

    return None

def _normalize_code(code_val) -> str:
    """
    Normalize admin codes (region/province/commune):
      - Keep as string, strip spaces
      - Convert '1.0' -> '1'
      - Upper-case (just in case)
    """
    if code_val is None or (isinstance(code_val, float) and pd.isna(code_val)):
        return ""
    s = str(code_val).strip()
    # Convert floats that are whole numbers
    try:
        if "." in s:
            f = float(s)
            if f.is_integer():
                s = str(int(f))
    except Exception:
        pass
    return s.upper()

# -----------------------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------------------
def create_tables(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    hook.run(DDL)
    hook.run(DDL_ALTER_NULLS)
    print("DDL ensured for stg.* referential tables.")

def load_regions(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    run_id = _latest_run_id(hook)

    df = _read_dataset(hook, run_id, "%region%.csv%")
    if df.empty:
        ctx["ti"].xcom_push(key="regions", value=0)
        print("No region CSV found in RAW_ROWS (dataset=referentiels).")
        return

    c_code = _first_col(df, "code_region", "code région", "code_reg", "id", "code")
    if not c_code:
        raise ValueError(f"[REGION] Code column not found. Columns={df.columns.tolist()}")

    code_series = df[c_code].apply(_normalize_code)

    c_name = _pick_name_column(
        df,
        candidates=["nom_region","nom région","region","région","name","nom"],
        code_series=code_series
    )
    if not c_name:
        raise ValueError(f"[REGION] Name column not found (non-numeric). Columns={df.columns.tolist()}")

    out = pd.DataFrame({
        "region_code": code_series,
        "region_name": df[c_name].astype(str).str.strip(),
    })
    out = out[(out["region_code"]!="") & (out["region_name"]!="")].drop_duplicates()

    print("Sample regions:\n", out.head(10))
    hook.run("TRUNCATE TABLE stg.REGION_REF;")
    engine = hook.get_sqlalchemy_engine()
    out.to_sql("REGION_REF", engine, schema="stg", if_exists="append", index=False, method="multi", chunksize=1000)
    ctx["ti"].xcom_push(key="regions", value=int(len(out)))
    print(f"✅ Loaded {len(out)} rows into stg.REGION_REF")
    return

    # Pick best columns
    c_code = _first_col(df, "code_region", "code région", "code_reg", "id", "code")
    c_name = _first_col(df, "nom_region", "nom région", "region", "région", "name", "nom")
    if not (c_code and c_name):
        raise ValueError(f"[REGION] Could not find code/name columns. Columns={df.columns.tolist()}")

    out = pd.DataFrame({
        "region_code": df[c_code].apply(_normalize_code),
        "region_name": df[c_name].astype(str).str.strip().str.upper(),
    })
    out = out[(out["region_code"]!="") & (out["region_name"]!="")].drop_duplicates()

    print("Sample regions:\n", out.head(10))
    hook.run("TRUNCATE TABLE stg.REGION_REF;")
    engine = hook.get_sqlalchemy_engine()
    out.to_sql("REGION_REF", engine, schema="stg", if_exists="append", index=False, method="multi", chunksize=1000)
    ctx["ti"].xcom_push(key="regions", value=int(len(out)))
    print(f"✅ Loaded {len(out)} rows into stg.REGION_REF")

def load_provinces(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    run_id = _latest_run_id(hook)

    df = _read_dataset(hook, run_id, "%provinc%.csv%")
    if df.empty:
        ctx["ti"].xcom_push(key="provinces", value=0)
        print("No province CSV found in RAW_ROWS (dataset=referentiels).")
        return

    c_code = _first_col(df, "code_province", "code prov", "code", "id")
    if not c_code:
        raise ValueError(f"[PROVINCE] Code column not found. Columns={df.columns.tolist()}")

    code_series = df[c_code].apply(_normalize_code)

    c_name = _pick_name_column(
        df,
        candidates=["nom_province","province","name","nom"],
        code_series=code_series
    )
    if not c_name:
        raise ValueError(f"[PROVINCE] Name column not found (non-numeric). Columns={df.columns.tolist()}")

    c_reg  = _first_col(df, "code_region", "region", "reg")

    out = pd.DataFrame({
        "province_code": code_series,
        "province_name": df[c_name].astype(str).str.strip(),
        "region_code":   (df[c_reg].apply(_normalize_code) if c_reg else pd.Series([None]*len(df))),
    })
    out = out[(out["province_code"]!="") & (out["province_name"]!="")].drop_duplicates()

    print("Sample provinces:\n", out.head(10))
    hook.run("TRUNCATE TABLE stg.PROVINCE_REF;")
    engine = hook.get_sqlalchemy_engine()
    out.to_sql("PROVINCE_REF", engine, schema="stg", if_exists="append", index=False, method="multi", chunksize=1000)
    ctx["ti"].xcom_push(key="provinces", value=int(len(out)))
    print(f"✅ Loaded {len(out)} rows into stg.PROVINCE_REF")
    return

    c_code = _first_col(df, "code_province", "code prov", "code", "id")
    c_name = _first_col(df, "nom_province", "province", "name", "nom")
    c_reg  = _first_col(df, "code_region", "region", "reg")
    if not (c_code and c_name):
        raise ValueError(f"[PROVINCE] Could not find code/name columns. Columns={df.columns.tolist()}")

    out = pd.DataFrame({
        "province_code": df[c_code].apply(_normalize_code),
        "province_name": df[c_name].astype(str).str.strip().str.upper(),
        "region_code":   (df[c_reg].apply(_normalize_code) if c_reg else pd.Series([None]*len(df))),
    })
    out = out[(out["province_code"]!="") & (out["province_name"]!="")].drop_duplicates()

    print("Sample provinces:\n", out.head(10))
    hook.run("TRUNCATE TABLE stg.PROVINCE_REF;")
    engine = hook.get_sqlalchemy_engine()
    out.to_sql("PROVINCE_REF", engine, schema="stg", if_exists="append", index=False, method="multi", chunksize=1000)
    ctx["ti"].xcom_push(key="provinces", value=int(len(out)))
    print(f"✅ Loaded {len(out)} rows into stg.PROVINCE_REF")

def load_communes(**ctx):
    """
    Your example headers:
      'Code commune' | 'Noms_Comm' | 'Nom_CommAr'
    We prefer the Latin name column; if multiple *nom* columns exist, we pick the first.
    """
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    run_id = _latest_run_id(hook)

    df = _read_dataset(hook, run_id, "%commune%.csv%")
    if df.empty:
        ctx["ti"].xcom_push(key="communes", value=0)
        print("No commune CSV found in RAW_ROWS (dataset=referentiels).")
        return

    # Choose code/name/province columns
    c_code = _first_col(df, "code_commune", "code com", "code", "id")
    # prefer explicit French name column if present, else first match on 'nom'/'name'/'commune'
    c_name = _first_col(df, "noms_comm", "nom_commune", "nom commune", "nom", "name", "commune")
    c_prov = _first_col(df, "code_province", "code prov", "province", "prov")

    if not (c_code and c_name):
        raise ValueError(f"[COMMUNE] Could not find code/name columns. Columns={df.columns.tolist()}")

    out = pd.DataFrame({
        "commune_code":  df[c_code].apply(_normalize_code),
        "commune_name":  df[c_name].astype(str).str.strip(),      # keep case; we only UPPER regions/provinces
        "province_code": (df[c_prov].apply(_normalize_code) if c_prov else pd.Series([None]*len(df))),
    })
    out = out[(out["commune_code"]!="") & (out["commune_name"]!="")].drop_duplicates()

    # If no province_code in file, derive from code prefix (try 5, then 4 digits)
    missing = out["province_code"].isna() | (out["province_code"].astype(str).str.strip()=="")
    if missing.any():
        out.loc[missing, "province_code"] = out.loc[missing, "commune_code"].str[:5]
        still_missing = out["province_code"].isna() | (out["province_code"].astype(str).str.strip()=="")
        out.loc[still_missing, "province_code"] = out.loc[still_missing, "commune_code"].str[:4]

    print("Sample communes:\n", out.head(10))

    hook.run("TRUNCATE TABLE stg.COMMUNE_REF;")
    engine = hook.get_sqlalchemy_engine()
    out.to_sql("COMMUNE_REF", engine, schema="stg", if_exists="append", index=False, method="multi", chunksize=1000)
    ctx["ti"].xcom_push(key="communes", value=int(len(out)))
    print(f"✅ Loaded {len(out)} rows into stg.COMMUNE_REF")

def patch_fk_with_hierarchical_codes(**ctx):
    """
    Derive missing foreign keys using hierarchical code structure:
      - Province → Region: first 1 or 2 digits (10/11/12 are 2-digit regions, else 1 digit)
      - Commune  → Province: first 5 digits (fallback 4)
    """
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)

    upd_p = hook.run(r"""
      UPDATE p
      SET p.region_code = CASE
          WHEN LEFT(p.province_code,2) IN ('10','11','12') THEN LEFT(p.province_code,2)
          ELSE LEFT(p.province_code,1)
      END
      FROM stg.PROVINCE_REF p
      WHERE (p.region_code IS NULL OR p.region_code='')
        AND p.province_code IS NOT NULL AND p.province_code<>'';
    """)
    print(f"Patched {upd_p} province→region links")

    upd_c5 = hook.run(r"""
      UPDATE c
      SET c.province_code = p.province_code
      FROM stg.COMMUNE_REF c
      JOIN stg.PROVINCE_REF p ON p.province_code = LEFT(c.commune_code,5)
      WHERE (c.province_code IS NULL OR c.province_code='')
        AND c.commune_code IS NOT NULL AND c.commune_code<>'';
    """)
    print(f"Patched {upd_c5} commune→province links (5-digit)")

    upd_c4 = hook.run(r"""
      UPDATE c
      SET c.province_code = p.province_code
      FROM stg.COMMUNE_REF c
      JOIN stg.PROVINCE_REF p ON p.province_code = LEFT(c.commune_code,4)
      WHERE (c.province_code IS NULL OR c.province_code='')
        AND c.commune_code IS NOT NULL AND c.commune_code<>'';
    """)
    print(f"Patched {upd_c4} commune→province links (4-digit fallback)")

def report(**ctx):
    r = ctx["ti"].xcom_pull(key="regions",   task_ids="load_regions")   or 0
    p = ctx["ti"].xcom_pull(key="provinces", task_ids="load_provinces") or 0
    c = ctx["ti"].xcom_pull(key="communes",  task_ids="load_communes")  or 0
    print(f"✅ [SILVER/REF] rows -> regions={r}, provinces={p}, communes={c}")
    if r == p == c == 0:
        raise ValueError("No referential rows produced. Check column mapping or file names.")

    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)

    # Provinces orphan (no region_code)
    orphan_p = hook.get_first("""
        SELECT COUNT(*) FROM stg.PROVINCE_REF 
        WHERE region_code IS NULL OR LTRIM(RTRIM(region_code))=''
    """)[0]
    if orphan_p > 0:
        print(f"⚠️  Warning: {orphan_p} provinces without region_code")

    # Communes orphan (no province_code)
    orphan_c = hook.get_first("""
        SELECT COUNT(*) FROM stg.COMMUNE_REF 
        WHERE province_code IS NULL OR LTRIM(RTRIM(province_code))=''
    """)[0]
    if orphan_c > 0:
        print(f"⚠️  Warning: {orphan_c} communes without province_code")

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
with DAG(
    "etl_silver_referentiels",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng"},
    tags=["observatoire", "silver", "referentiels"],
) as dag:

    t_ddl = PythonOperator(task_id="create_tables", python_callable=create_tables)
    t_reg = PythonOperator(task_id="load_regions",  python_callable=load_regions)
    t_pro = PythonOperator(task_id="load_provinces",python_callable=load_provinces)
    t_com = PythonOperator(task_id="load_communes", python_callable=load_communes)
    t_fix = PythonOperator(task_id="patch_fk_codes", python_callable=patch_fk_with_hierarchical_codes)
    t_rep = PythonOperator(task_id="report",        python_callable=report)

    t_ddl >> [t_reg, t_pro, t_com] >> t_fix >> t_rep

# pyright: reportMissingImports=false
from datetime import datetime
import os, re, unicodedata
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

# SqlHook compatibility (works across provider versions)
try:
    from airflow.providers.common.sql.hooks.sql import SqlHook  # Airflow >= 2.5
except Exception:  # pragma: no cover
    try:
        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook as SqlHook
    except Exception:
        try:
            from airflow.providers.common.sql.hooks.sql import DbApiHook as SqlHook
        except Exception:
            from airflow.hooks.dbapi import DbApiHook as SqlHook  # core fallback

BASE_DIR = os.environ.get("ETL_BASE_DIR", "/opt/etl/data")
STAGING_DB = os.environ.get("ETL_STAGING_DB", "OBS_STAGING")
SOURCE_DB = os.environ.get("ETL_PONDERATION_SOURCE_DB", os.environ.get("ETL_DEFAULT_SOURCE_DB", "OBSERVATOIRE"))

# ---------- Helpers ----------
def _read_any(path_base: str) -> pd.DataFrame:
    """
    Try CSV (utf-8 or cp1252) first, else XLSX with sheet 0.
    Pass 'path_base' with extension if you want, otherwise we try both (.csv/.xlsx).
    """
    import os
    base, ext = os.path.splitext(path_base)
    candidates = []
    if ext.lower() in (".csv", ".xlsx"):
        candidates = [path_base]
    else:
        candidates = [base + ".csv", base + ".xlsx"]

    for p in candidates:
        if not os.path.exists(p):
            continue
        if p.lower().endswith(".csv"):
            for enc in ("utf-8", "cp1252"):
                try:
                    return pd.read_csv(p, sep=";", encoding=enc, dtype=str, low_memory=False)
                except Exception:
                    try:
                        return pd.read_csv(p, encoding=enc, dtype=str, low_memory=False)
                    except Exception:
                        continue
        else:
            # XLSX
            return pd.read_excel(p, sheet_name=0, dtype=str)
    raise FileNotFoundError(f"File not found: {candidates}")

def _norm_text(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize('NFKD', str(s))
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[\s_\-]+", " ", s)
    return s

def _norm_cols(cols):
    out = []
    for c in cols:
        s = unicodedata.normalize('NFKD', str(c))
        s = ''.join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower().strip()
        s = re.sub(r"\s+", "_", s)
        s = re.sub(r"[^0-9a-z_]+", "", s)
        out.append(s)
    return out

def _pick_weight_col(df: pd.DataFrame):
    # 1) Explicit signals first
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl in ("poids", "weight", "ponderation", "pondÃ©ration"):
            return c
        if "poids" in cl or "ponderation" in cl or "pond" in cl or "weight" in cl:
            return c
    # 2) Year-range header (e.g., '2014-2024')
    for c in df.columns:
        if re.search(r"\b\d{4}\b\s*[-â€“]\s*\b\d{4}\b", str(c)):
            return c
    # 3) First fully numeric-looking column
    for c in df.columns:
        try:
            pd.to_numeric(df[c], errors="raise")
            return c
        except Exception:
            continue
    return None

# ---------- DDL (idempotent) ----------
DDL = r"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg');

-- Drop and recreate to ensure clean state
IF OBJECT_ID('stg.POND_VILLE_BASE','U') IS NOT NULL DROP TABLE stg.POND_VILLE_BASE;
CREATE TABLE stg.POND_VILLE_BASE(
  region       NVARCHAR(200) NOT NULL,
  ville        NVARCHAR(200) NOT NULL,
  poids_ville  DECIMAL(18,6) NOT NULL,
  source_file  NVARCHAR(256) NULL,
  load_ts      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_POND_VILLE_BASE PRIMARY KEY (region, ville)
);

IF OBJECT_ID('stg.POND_VARIETE_BASE','U') IS NOT NULL DROP TABLE stg.POND_VARIETE_BASE;
CREATE TABLE stg.POND_VARIETE_BASE(
  variete       NVARCHAR(200) NOT NULL PRIMARY KEY,
  poids_variete DECIMAL(18,6) NOT NULL,
  source_file   NVARCHAR(256) NULL,
  load_ts       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
"""

# ---------- Tasks ----------
def create_tables(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    hook.run(DDL)

def read_city_weights(**ctx):
    # You can point to either CSV or XLSX; we'll autodetect.
    rel = os.path.join("ponderations", "ponderations_villes_regions_output")  # no ext required
    candidates = [os.path.join(BASE_DIR, rel), os.path.join(os.path.dirname(__file__), "..", "data", rel)]
    path_base = next((p for p in candidates if os.path.exists(p + ".csv") or os.path.exists(p + ".xlsx")), None)
    if not path_base:
        raise FileNotFoundError(f"City weights file not found (.csv or .xlsx) under: {candidates}")

    df = _read_any(path_base)  # <-- NEW
    df.columns = _norm_cols(df.columns)

    # Identify columns
    col_region = next((c for c in df.columns if c.startswith("region")), None)
    # Handle 'AgglomÃ©ration' (accents) or 'Agglomeration' or 'ville'
    col_ville  = next(
        (c for c in df.columns
         if c.startswith("agglomeration") or c.startswith("agglomration") or c.startswith("agglom") or c.startswith("ville")),
        None
    )
    col_w = _pick_weight_col(df)
    if not (col_region and col_ville and col_w):
        raise ValueError(f"City weights columns not recognized. Found: {df.columns.tolist()}")

    out = df[[col_region, col_ville, col_w]].copy()
    out.columns = ["region", "ville", "poids_ville"]

    # Normalize unicode and trim
    out["region"] = out["region"].astype(str).apply(lambda x: unicodedata.normalize('NFC', x).strip())
    out["ville"]  = out["ville"].astype(str).apply(lambda x: unicodedata.normalize('NFC', x).strip())

    # Make numeric; drop empties
    out["poids_ville"] = pd.to_numeric(out["poids_ville"], errors="coerce")
    out = out.dropna(subset=["region","ville","poids_ville"])
    out = out[(out["region"] != "") & (out["ville"] != "")]

    # Aggregate duplicates, then normalize per region -> sum = 1
    out = out.groupby(["region", "ville"], as_index=False)["poids_ville"].sum()
    out["poids_ville"] = out.groupby("region")["poids_ville"].transform(lambda s: s / s.sum() if s.sum() else s)
    out["source_file"] = os.path.basename(path_base)  # 'ponderations_villes_regions_output'

    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    eng = hook.get_sqlalchemy_engine()
    hook.run("DELETE FROM stg.POND_VILLE_BASE;")
    out.to_sql("POND_VILLE_BASE", eng, schema="stg", if_exists="append", index=False, chunksize=1000)
    print(f"Inserted {len(out)} city weights from {os.path.basename(path_base)}")

def read_variete_weights(**ctx):
    # Expected at <BASE_DIR>/ponderations/ponderation_MC_prixindice.csv
    rel = os.path.join("ponderations", "ponderation_MC_prixindice.csv")
    paths = [os.path.join(BASE_DIR, rel), os.path.join(os.path.dirname(__file__), "..", "data", rel)]
    path = None
    for p in paths:
        if os.path.exists(p):
            path = p
            break
    if not path:
        raise FileNotFoundError(f"Variete weights file not found in: {paths}")

    df = _read_any(path)
    df.columns = _norm_cols(df.columns)
    # pick variete and first pond* numeric column
    col_var = next((c for c in df.columns if c.startswith("variete")), None)
    if not col_var:
        raise ValueError("Variete column not found in ponderation variete file")
    # find a column with 'pond' - look for the FIRST ponderation column (varieties)
    # Format: Corps;ActivitÃ©;Produit;VariÃ©tÃ©;Pond_varietes;Pond_produits;Pond_activites;...
    weight_cols = [c for c in df.columns if "pond" in c]
    col_w = None
    for c in weight_cols:
        try:
            pd.to_numeric(df[c], errors="raise")
            col_w = c; break
        except Exception:
            continue
    if not col_w:
        # fallback: scan remaining columns for numeric
        for c in df.columns:
            if c == col_var: continue
            try:
                pd.to_numeric(df[c], errors="raise"); col_w=c; break
            except Exception:
                continue
    if not col_w:
        raise ValueError("No numeric ponderation column found for variete")

    out = df[[col_var, col_w]].copy()
    out.columns = ["variete","poids_variete"]

    # Normalize unicode + trim so downstream comparisons behave
    out["variete"] = out["variete"].astype(str).apply(lambda x: unicodedata.normalize('NFC', x).strip())
    out["poids_variete"] = pd.to_numeric(out["poids_variete"], errors="coerce")
    out = out.dropna(subset=["variete","poids_variete"])
    out = out[out["variete"] != ""]

    # Build a normalized key so e.g. '2Ã¨me Choix' and '2Ã¨me choix' collapse together (SQL collation is case-insensitive)
    def _variete_key(val: str) -> str:
        key = _norm_text(val)
        key = re.sub(r"[^0-9a-z/ ]+", "", key)
        key = re.sub(r"\s+", " ", key).strip()
        return key

    out["variete_key"] = out["variete"].map(_variete_key)
    out = out[out["variete_key"] != ""]

    # Debug logging to highlight duplicates prior to aggregation
    dup_mask = out.duplicated(subset="variete_key", keep=False)
    if dup_mask.any():
        dup_view = (
            out.loc[dup_mask, ["variete_key", "variete", "poids_variete"]]
            .sort_values(["variete_key", "variete"])
        )
        print(f"INFO: Found {len(dup_view)} rows sharing the same normalized variety key (will be aggregated):")
        print(dup_view.head(20))

    def _pick_display_label(values) -> str:
        """Prefer a human-friendly label when multiple spellings map to the same key."""
        ranked = []
        for idx, raw in enumerate(values):
            val = (raw or "").strip()
            letters = [ch for ch in val if ch.isalpha()]
            if not letters:
                style_rank = 3  # numbers / symbols only
            else:
                uppers = sum(ch.isupper() for ch in letters)
                lowers = sum(ch.islower() for ch in letters)
                if uppers and lowers:
                    style_rank = 0  # mixed case -> looks most natural
                elif uppers:
                    style_rank = 1  # all caps
                else:
                    style_rank = 2  # all lower
            ranked.append((style_rank, idx, val))
        ranked.sort()
        return ranked[0][2] if ranked else ""

    agg = (
        out.groupby("variete_key", as_index=False, sort=False)
        .agg(
            poids_variete=("poids_variete", "sum"),
            variete=("variete", lambda s: _pick_display_label(s.tolist())),
        )
    )
    out = agg.drop(columns=["variete_key"])

    # Normalize across varietes so sum=1 (robust)
    tot = out["poids_variete"].sum()
    if tot and tot > 0:
        out["poids_variete"] = out["poids_variete"] / tot
    out["source_file"] = os.path.basename(path)

    # Final defensive check for duplicates after aggregation (case-insensitive)
    if out["variete"].str.casefold().duplicated().any():
        final_dups = out[out["variete"].str.casefold().duplicated(keep=False)]
        print("ERROR: Duplicate display names still present after aggregation:")
        print(final_dups)
        raise ValueError("Variety names remain duplicated after normalization")

    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    eng = hook.get_sqlalchemy_engine()
    
    # Ensure table is empty (defensive)
    hook.run("DELETE FROM stg.POND_VARIETE_BASE WHERE 1=1;")
    
    out.to_sql("POND_VARIETE_BASE", eng, schema="stg", if_exists="append", index=False, method=None, chunksize=1000)
    print(f"Inserted {len(out)} variety weights")

def build_pond_ville_raw(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    eng = hook.get_sqlalchemy_engine()
    src_hook = SqlHook(conn_id="mssql_default", schema=SOURCE_DB)
    # base years and entities from staged indices
    try:
        base = src_hook.get_pandas_df("""
            SELECT DISTINCT agglomeration AS ville, variete, annee
            FROM stg.PRIX_INDICE_RAW
            WHERE NULLIF(LTRIM(RTRIM(agglomeration)),'') IS NOT NULL
              AND NULLIF(LTRIM(RTRIM(variete)),'') IS NOT NULL
        """)
    except Exception as exc:
        msg = str(exc)
        needs_fallback = "Login failed for user" in msg or "Adaptive Server connection failed" in msg
        if not needs_fallback:
            raise
        print(f"WARN: Unable to read stg.PRIX_INDICE_RAW from SOURCE_DB={SOURCE_DB}: {exc}. Falling back to STAGING_DB={STAGING_DB}.")
        base = hook.get_pandas_df("""
            SELECT DISTINCT agglomeration AS ville, variete, annee
            FROM stg.PRIX_INDICE_RAW
            WHERE NULLIF(LTRIM(RTRIM(agglomeration)),'') IS NOT NULL
              AND NULLIF(LTRIM(RTRIM(variete)),'') IS NOT NULL
        """)
    if base.empty:
        print("WARN: stg.PRIX_INDICE_RAW returned 0 distinct (ville, variete, annee) rows; skipping build_pond_ville_raw.")
        return
    base["ville_key"]   = base["ville"].map(_norm_text)
    base["variete_key"] = base["variete"].map(_norm_text)

    vbase = hook.get_pandas_df("SELECT ville, region, poids_ville, source_file FROM stg.POND_VILLE_BASE")
    if vbase.empty:
        print("WARN: stg.POND_VILLE_BASE is empty; skipping build_pond_ville_raw.")
        return
    vbase["ville_key"] = vbase["ville"].map(_norm_text)

    rbase = hook.get_pandas_df("SELECT variete, poids_variete, source_file FROM stg.POND_VARIETE_BASE")
    if rbase.empty:
        print("WARN: stg.POND_VARIETE_BASE is empty; skipping build_pond_ville_raw.")
        return
    rbase["variete_key"] = rbase["variete"].map(_norm_text)

    df = base.merge(vbase[["ville_key","poids_ville"]], on="ville_key", how="left")
    df = df.merge(rbase[["variete_key","poids_variete"]], on="variete_key", how="left")
    df["poids_ville"].fillna(1.0, inplace=True)
    df["poids_variete"].fillna(1.0, inplace=True)
    df["poids"] = (df["poids_ville"] * df["poids_variete"]).astype(float)

    # Normalize per (ville, annee)
    df["poids"] = df.groupby(["ville","annee"])['poids'].transform(lambda s: s / s.sum() if s.sum() else s)

    out = df[["ville","variete","annee","poids"]].copy()
    out["source_file"] = "ponderations_ville_variete"

    hook.run("DELETE FROM stg.PONDERATION_VILLE_RAW;")
    out.to_sql("PONDERATION_VILLE_RAW", eng, schema="stg", if_exists="append", index=False, method=None, chunksize=1000)
    print(f"Inserted {len(out)} rows into stg.PONDERATION_VILLE_RAW.")

def build_pond_region_raw(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    eng = hook.get_sqlalchemy_engine()

    # Use region from city weights mapping
    vmap = hook.get_pandas_df("SELECT region, ville FROM stg.POND_VILLE_BASE")
    if vmap.empty:
        print("WARN: stg.POND_VILLE_BASE is empty; skipping build_pond_region_raw.")
        return
    vmap["ville_key"] = vmap["ville"].map(_norm_text)

    vraw = hook.get_pandas_df("SELECT ville, variete, annee, poids FROM stg.PONDERATION_VILLE_RAW")
    if vraw.empty:
        print("WARN: stg.PONDERATION_VILLE_RAW is empty; skipping build_pond_region_raw.")
        return
    vraw["ville_key"] = vraw["ville"].map(_norm_text)

    df = vraw.merge(vmap[["ville_key","region"]], on="ville_key", how="left")
    df = df.dropna(subset=["region"])
    grp = df.groupby(["region","variete","annee"], as_index=False)["poids"].sum()
    # Normalize per (region, annee)
    grp["poids"] = grp.groupby(["region","annee"])['poids'].transform(lambda s: s / s.sum() if s.sum() else s)
    grp["source_file"] = "ponderations_region_variete"

    hook.run("DELETE FROM stg.PONDERATION_REGION_RAW;")
    grp.to_sql("PONDERATION_REGION_RAW", eng, schema="stg", if_exists="append", index=False, method=None, chunksize=1000)
    print(f"Inserted {len(grp)} rows into stg.PONDERATION_REGION_RAW.")

def report(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema=STAGING_DB)
    c1 = hook.get_first("SELECT COUNT(*) FROM stg.POND_VILLE_BASE")[0]
    c2 = hook.get_first("SELECT COUNT(*) FROM stg.POND_VARIETE_BASE")[0]
    v  = hook.get_first("SELECT COUNT(*) FROM stg.PONDERATION_VILLE_RAW")[0]
    r  = hook.get_first("SELECT COUNT(*) FROM stg.PONDERATION_REGION_RAW")[0]
    print(f"Ponderations: base(ville)={c1}, base(variete)={c2}, RAW(ville)={v}, RAW(region)={r}")

with DAG(
    dag_id="etl_silver_ponderations",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner":"data-eng"},
    tags=["observatoire","silver","ponderations"],
) as dag:

    t_ddl  = PythonOperator(task_id="create_tables", python_callable=create_tables)
    t_city = PythonOperator(task_id="read_city_weights", python_callable=read_city_weights)
    t_var  = PythonOperator(task_id="read_variete_weights", python_callable=read_variete_weights)
    t_vraw = PythonOperator(task_id="build_pond_ville_raw", python_callable=build_pond_ville_raw)
    t_rraw = PythonOperator(task_id="build_pond_region_raw", python_callable=build_pond_region_raw)
    t_rep  = PythonOperator(task_id="report", python_callable=report)

    t_ddl >> [t_city, t_var] >> t_vraw >> t_rraw >> t_rep
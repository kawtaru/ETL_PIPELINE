# pyright: reportMissingImports=false
from datetime import datetime
import os, glob, shutil, zipfile, re, json, logging, yaml, unicodedata
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.exc import InterfaceError
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
# Compat: SqlHook may not exist in some provider versions.
# Fallback to MsSqlHook (preferred for MSSQL) or DbApiHook.
try:
    from airflow.providers.common.sql.hooks.sql import SqlHook  # Airflow >= 2.5 typically
except Exception:  # pragma: no cover - environment dependent
    try:
        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook as SqlHook
    except Exception:
        try:
            from airflow.providers.common.sql.hooks.sql import DbApiHook as SqlHook
        except Exception:
            from airflow.hooks.dbapi import DbApiHook as SqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)

BASE_DIR = os.environ.get("ETL_BASE_DIR", "/opt/etl/data")
RAW_DIR  = os.path.join(BASE_DIR, "raw", "prix_indices")
LAND_DIR = os.path.join(BASE_DIR, "landing", "prix_indices")
ARCH_DIR = os.path.join(BASE_DIR, "archive", "prix_indices")

CFG_PATH = os.path.join(os.path.dirname(__file__), "config", "prix_indices_columns.yml")
with open(CFG_PATH, "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

def _slug(x: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "-", str(x)).strip("-").lower()

def exec_to_sql_bulk(hook: SqlHook, table_qualified: str, df: pd.DataFrame):
    if df.empty: return 0
    # Force a SQLAlchemy pyodbc engine with fast_executemany for high-throughput inserts.
    # This avoids the SQL Server 1000 VALUES limit (by using executemany) and is faster than pymssql.
    # Resolve connection id across hook types
    conn_id = None
    if hasattr(hook, "conn_name_attr"):
        conn_id = getattr(hook, hook.conn_name_attr, None)
    if not conn_id and hasattr(hook, "conn_id"):
        conn_id = getattr(hook, "conn_id")
    if not conn_id:
        # Best-effort fallback for MsSqlHook
        conn_id = getattr(hook, "mssql_conn_id", "mssql_default")

    conn = BaseHook.get_connection(conn_id)
    host = conn.host
    port = conn.port or 1433
    user = conn.login or ""
    pwd  = conn.get_password() or ""
    db   = conn.schema or ""
    # Prefer explicit driver from connection extras, fallback to ODBC 18 which is installed in our image
    extra = {}
    try:
        extra = conn.extra_dejson or {}
    except Exception:
        extra = {}
    # Try pyodbc with multiple possible driver names; fallback to provider engine on failure
    driver_pref = extra.get("odbc_driver") or extra.get("driver")
    driver_candidates = [driver_pref] if driver_pref else [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
    ]
    encrypt = str(extra.get("Encrypt", "yes")).lower()
    trust   = str(extra.get("TrustServerCertificate", "yes")).lower()

    engine = None
    used_pyodbc = False
    last_err = None
    for drv in driver_candidates:
        try:
            params = f"driver={quote_plus(drv)}&Encrypt={encrypt}&TrustServerCertificate={trust}"
            url = f"mssql+pyodbc://{quote_plus(user)}:{quote_plus(pwd)}@{host}:{port}/{quote_plus(db)}?{params}"
            tmp_engine = create_engine(url, fast_executemany=True)
            # Eager connect to validate driver availability
            with tmp_engine.connect() as _conn:
                pass
            engine = tmp_engine
            used_pyodbc = True
            break
        except InterfaceError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue
    if engine is None:
        # Fallback to the hook's default SQLAlchemy engine (likely pymssql)
        engine = hook.get_sqlalchemy_engine()

    schema, table = table_qualified.split(".")
    if used_pyodbc:
        # Use executemany (method=None) to bypass 1000-VALUES limit; keep modest chunksize.
        df.to_sql(table, engine, schema=schema, if_exists="append", index=False, method=None, chunksize=1000)
    else:
        # On pymssql, keep multi with chunks of 1000 to respect MSSQL constraint
        df.to_sql(table, engine, schema=schema, if_exists="append", index=False, method="multi", chunksize=1000)
    return len(df)

def detect_zip(**ctx):
    zips = sorted(glob.glob(os.path.join(RAW_DIR, "*.zip")))
    if not zips:
        raise FileNotFoundError("Aucun ZIP trouvé dans raw/prix_indices")
    latest = zips[-1]
    logger.info(f"ZIP détecté: {latest}")
    ctx["ti"].xcom_push(key="src_zip", value=latest)

def unzip_to_landing(**ctx):
    src_zip = ctx["ti"].xcom_pull(key="src_zip", task_ids="detect_zip")
    run_folder = os.path.join(LAND_DIR, ctx["run_id"].replace(":", "_"))
    os.makedirs(run_folder, exist_ok=True)
    with zipfile.ZipFile(src_zip, 'r') as zf:
        zf.extractall(run_folder)
    logger.info(f"Unzip → {run_folder}")
    ctx["ti"].xcom_push(key="landing_dir", value=run_folder)

def stage_clear(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema="OBSERVATOIRE")
    hook.run("TRUNCATE TABLE stg.PRIX_INDICE_RAW;")
    hook.run("TRUNCATE TABLE stg.PONDERATION_VILLE_RAW;")
    hook.run("TRUNCATE TABLE stg.PONDERATION_REGION_RAW;")
    logger.info("Staging vidé.")

def _norm(s: str) -> str:
    if s is None:
        return ""
    # Remove diacritics and normalize whitespace/hyphens/underscores
    s = str(s)
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[\s_\-]+", " ", s)
    return s

def _find_col(df, candidates):
    cand_norm = {_norm(x) for x in candidates}
    for c in df.columns:
        if _norm(c) in cand_norm:
            return c
    return None

def stage_load_prix_indices(**ctx):
    landing_dir = ctx["ti"].xcom_pull(key="landing_dir", task_ids="unzip_to_landing")
    csvs = glob.glob(os.path.join(landing_dir, "**", "*.csv"), recursive=True)
    logger.info(f"{len(csvs)} CSV détectés (landing).")
    frames = []
    year_re = re.compile(CFG["indices"]["year_regex"])  # e.g., ^(19|20)\d{2}$
    for path in csvs:
        try:
            df = pd.read_csv(path, sep=";", encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(path, sep=";", encoding="cp1252", low_memory=False)

        # Try long-format first: columns like Année, Produit, Variété, Ville/Région, and price/index values per row
        handled = False
        try:
            year_single = _find_col(df, ["annee", "année", "year"])  # tolerate encoding glitches
            if year_single:
                aggl_col_l = _find_col(df, CFG["indices"]["ville"]) or _find_col(df, ["region", "région"]) or _find_col(df, ["national"])  # fallback
                prod_col_l = _find_col(df, CFG["indices"]["produit"])
                var_col_l  = _find_col(df, CFG["indices"]["variete"])
                price_col = None
                index_col = None
                for c in df.columns:
                    nc = _norm(c)
                    if price_col is None and ("prix" in nc) and ("indice" not in nc):
                        price_col = c
                    if index_col is None and ("indice" in nc) and ("prix" in nc):
                        index_col = c
                if aggl_col_l and (prod_col_l or var_col_l) and (price_col or index_col):
                    long_rows = []
                    for _, r in df.iterrows():
                        yv = r.get(year_single)
                        if pd.isna(yv):
                            continue
                        try:
                            yv = int(str(yv).strip())
                        except Exception:
                            continue
                        """
\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ vi\ =\ r\.get\(index_col\)\ if\ \(index_col\ is\ not\ None\ and\ index_col\ in\ df\.columns\)\ else\ None`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ pm\ =\ r\.get\(price_col\)\ if\ \(price_col\ is\ not\ None\ and\ price_col\ in\ df\.columns\)\ else\ None`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \#\ Determine\ geographic\ scope\ \(niveau/region/agglomeration\)`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ is_national\ =\ "national"\ in\ path\.lower\(\)`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ val_geo\ =\ str\(r\.get\(aggl_col_l,\ ""\)\)\.strip\(\)\ if\ aggl_col_l\ else\ ""`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ if\ is_national:`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ if\ val_geo:`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ niveau\ =\ "REGION";\ region\ =\ val_geo;\ aggl\ =\ None`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ else:`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ niveau\ =\ "NATIONAL";\ region\ =\ None;\ aggl\ =\ None`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ else:`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ niveau\ =\ "VILLE";\ region\ =\ None;\ aggl\ =\ val_geo`n`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ long_rows\.append\(\{`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ "source_file":\ os\.path\.basename\(path\),`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ "niveau":\ niveau,`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ "region":\ region,`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ "agglomeration":\ aggl,`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ "produit":\ str\(r\.get\(prod_col_l,\ ""\)\)\.strip\(\)\ if\ prod_col_l\ else\ "",`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ "variete":\ str\(r\.get\(var_col_l,\ ""\)\)\.strip\(\)\ if\ var_col_l\ else\ "",`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ "annee":\ yv,`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ "type_indice":\ "indice",`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ "valeur_indice":\ float\(vi\)\ if\ \(vi\ is\ not\ None\ and\ pd\.notna\(vi\)\)\ else\ None,`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ "prix_moyen":\ float\(pm\)\ if\ \(pm\ is\ not\ None\ and\ pd\.notna\(pm\)\)\ else\ None`n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ }\)
                        """
                        vi = r.get(index_col) if (index_col is not None and index_col in df.columns) else None
                        pm = r.get(price_col) if (price_col is not None and price_col in df.columns) else None
                        # Determine geographic scope (niveau/region/agglomeration)
                        is_national = "national" in path.lower()
                        val_geo = str(r.get(aggl_col_l, "")).strip() if aggl_col_l else ""
                        if is_national:
                            if val_geo:
                                niveau = "REGION"; region = val_geo; aggl = None
                            else:
                                niveau = "NATIONAL"; region = None; aggl = None
                        else:
                            niveau = "VILLE"; region = None; aggl = val_geo

                        long_rows.append({
                            "source_file": os.path.basename(path),
                            "niveau": niveau,
                            "region": region,
                            "agglomeration": aggl,
                            "produit": str(r.get(prod_col_l, "")).strip() if prod_col_l else "",
                            "variete": str(r.get(var_col_l, "")).strip() if var_col_l else "",
                            "annee": yv,
                            "type_indice": "indice",
                            "valeur_indice": float(vi) if (vi is not None and pd.notna(vi)) else None,
                            "prix_moyen": float(pm) if (pm is not None and pd.notna(pm)) else None
                        })
                    if long_rows:
                        logger.info(f"Lecture {path}: {len(df)} lignes (format long).")
                        frames.append(pd.DataFrame(long_rows))
                        handled = True
        except Exception:
            # Fallback to wide format if long-format detection fails
            handled = False

        if handled:
            continue

        aggl_col = _find_col(df, CFG["indices"]["ville"])
        prod_col = _find_col(df, CFG["indices"]["produit"])
        var_col  = _find_col(df, CFG["indices"]["variete"])
        # Detect year-like columns robustly (allows labels like "Année 2020")
        year_cols = []
        year_map = {}
        for c in df.columns:
            cs = str(c).strip()
            if year_re.fullmatch(cs):
                year_cols.append(c)
                year_map[c] = int(cs)
            else:
                m = re.search(r"(19|20)\d{2}", cs)
                if m:
                    year_cols.append(c)
                    year_map[c] = int(m.group(0))

        if not (aggl_col and (prod_col or var_col) and year_cols):
            logger.warning(f"Fichier ignoré (colonnes manquantes) : {path}")
            continue

        logger.info(f"Lecture {path}: {len(df)} lignes, {len(year_cols)} colonnes années.")
        long_rows = []
        for _, r in df.iterrows():
            for y in year_cols:
                v = r[y]
                if pd.isna(v): 
                    continue
                long_rows.append({
                    "source_file": os.path.basename(path),
                    "agglomeration": str(r.get(aggl_col, "")).strip(),
                    "produit": str(r.get(prod_col, "")).strip() if prod_col else "",
                    "variete": str(r.get(var_col, "")).strip() if var_col else "",
                    "annee": int(year_map[y]),
                    "type_indice": "indice",
                    "valeur_indice": float(v) if pd.notna(v) else None,
                    "prix_moyen": None
                })
        if long_rows:
            frames.append(pd.DataFrame(long_rows))

    if not frames:
        raise ValueError("Aucune ligne exploitable pour PRIX/INDICE")
    stg_df = pd.concat(frames, ignore_index=True)

    # Validations : années plausibles, valeurs non négatives
    current_year = datetime.utcnow().year
    before = len(stg_df)
    stg_df = stg_df[(stg_df["annee"] >= 1990) & (stg_df["annee"] <= current_year)]
    stg_df = stg_df[(stg_df["valeur_indice"].isna()) | (stg_df["valeur_indice"] >= 0)]
    logger.info(f"Staging indices: {before} → {len(stg_df)} lignes après validations.")

    hook = SqlHook(conn_id="mssql_default", schema="OBSERVATOIRE")
    inserted = exec_to_sql_bulk(hook, "stg.PRIX_INDICE_RAW", stg_df)
    logger.info(f"Staging PRIX_INDICE_RAW insert: {inserted} lignes.")

def stage_load_ponderations(**ctx):
    ref_dir = os.path.join(BASE_DIR, "referentiels")
    f_v = os.path.join(ref_dir, "ponderation_MC_prixindice.csv")
    f_r = os.path.join(ref_dir, "ponderations_villes_regions_output.csv")
    hook = SqlHook(conn_id="mssql_default", schema="OBSERVATOIRE")

    # Transform provided files into required staging schemas
    # 1) City distribution file: Region;Agglomération;2015-2024
    # 2) Variety weights file: Corps;Activité;Produit;Variété;... first numeric weight column
    # We will create PONDERATION_VILLE_RAW by combining city weights with varieties and years observed in staging indices.

    # Load city weights (region, ville, poids)
    city_path = f_r
    city_df = None
    if os.path.exists(city_path):
        try:
            city_df = pd.read_csv(city_path, sep=";", encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            city_df = pd.read_csv(city_path, sep=";", encoding="cp1252", low_memory=False)
        cn = {c: _norm(c) for c in city_df.columns}
        # Map common french headers
        col_region = next((c for c,n in cn.items() if n.startswith("region")), None)
        col_ville  = next((c for c,n in cn.items() if n.startswith("agglomeration") or n.startswith("agglomeration") or n.startswith("ville")), None)
        # weight column could be like '2015-2024' or similar
        col_weight = None
        for c in city_df.columns:
            if re.search(r"\d{4}.*\d{4}", str(c)):
                col_weight = c
                break
        if not (col_region and col_ville and col_weight):
            logger.warning("Fichier ponderations_villes_regions_output.csv: colonnes attendues introuvables. Skip chargement villes.")
            city_df = None
        else:
            city_df = city_df[[col_region, col_ville, col_weight]].copy()
            city_df.columns = ["region","ville","poids_ville"]
            # Clean types
            city_df["poids_ville"] = pd.to_numeric(city_df["poids_ville"], errors="coerce")
            city_df.dropna(subset=["ville","poids_ville"], inplace=True)
            city_df["ville"] = city_df["ville"].astype(str).str.strip()
    else:
        logger.warning("Fichier ponderations_villes_regions_output.csv introuvable.")

    # Load variety weights
    var_path = f_v
    var_df = None
    if os.path.exists(var_path):
        try:
            var_df = pd.read_csv(var_path, sep=";", encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            var_df = pd.read_csv(var_path, sep=";", encoding="cp1252", low_memory=False)
        cols_lower = {c: c.lower() for c in var_df.columns}
        var_col = None
        # Look for column named like 'variété'
        for c in var_df.columns:
            if _norm(c).startswith("variete"):
                var_col = c
                break
        # Find a numeric weight column among the remaining columns
        weight_cols = [c for c in var_df.columns if c != var_col]
        w_col = None
        for c in weight_cols:
            # Prefer ones that include 'pond'
            if "pond" in _norm(c):
                w_col = c
                break
        if w_col is None:
            # fallback to first numeric-looking column after the first 4
            for c in weight_cols[4:]:
                if pd.api.types.is_numeric_dtype(var_df[c]):
                    w_col = c
                    break
        if not (var_col and w_col):
            logger.warning("Fichier ponderation_MC_prixindice.csv: colonnes 'Variété' ou poids introuvables. Skip variety weights, assume 1.0.")
            var_df = None
        else:
            var_df = var_df[[var_col, w_col]].copy()
            var_df.columns = ["variete","poids_variete"]
            var_df["variete"] = var_df["variete"].astype(str).str.strip()
            var_df["poids_variete"] = pd.to_numeric(var_df["poids_variete"], errors="coerce").fillna(1.0)
            var_df = var_df.groupby("variete", as_index=False)["poids_variete"].mean()
    else:
        logger.warning("Fichier ponderation_MC_prixindice.csv introuvable.")

    inserted_total = 0
    if city_df is not None:
        # Fetch available variete/year per ville from staged indices to conform to NOT NULL constraints
        qry = (
            "SELECT DISTINCT agglomeration AS ville, variete, annee "
            "FROM stg.PRIX_INDICE_RAW WHERE variete IS NOT NULL AND annee IS NOT NULL"
        )
        base_df = hook.get_pandas_df(qry)
        if base_df.empty:
            logger.warning("Aucune donnée de staging indices pour créer les ponderations. Skip.")
        else:
            base_df["ville"] = base_df["ville"].astype(str).str.strip()
            # Normalize cities to improve join robustness (accents/case/spacing)
            base_df["_cn"] = base_df["ville"].map(_norm)
            city_df["_cn"] = city_df["ville"].map(lambda s: _norm(s))
            if var_df is not None:
                # Join with variety weights on normalized name
                base_df["_vn"] = base_df["variete"].map(_norm)
                vtmp = var_df.copy()
                vtmp["_vn"] = vtmp["variete"].map(_norm)
                base_df = base_df.merge(vtmp[["_vn","poids_variete"]], on="_vn", how="left")
                base_df.drop(columns=["_vn"], inplace=True)
            else:
                base_df["poids_variete"] = 1.0
            # Join on normalized city name, keep original ville from base_df
            df = base_df.merge(city_df[["_cn","poids_ville"]], left_on="_cn", right_on="_cn", how="inner")
            df["poids"] = (df["poids_ville"].fillna(1.0) * df["poids_variete"].fillna(1.0)).astype(float)
            out = df[["ville","variete","annee","poids"]].copy()
            out["source_file"] = os.path.basename(city_path)
            # Order columns per table
            out = out[["source_file","ville","variete","annee","poids"]]
            inserted = exec_to_sql_bulk(hook, "stg.PONDERATION_VILLE_RAW", out)
            inserted_total += inserted
            logger.info(f"Staging PONDERATION_VILLE_RAW insert (généré): {inserted} lignes.")

    # Region-level not generated due to ambiguous mapping; keep warning
    logger.warning("PONDERATION_REGION_RAW non généré automatiquement: fichier source ne contient pas 'variete'/'annee'.")

def ensure_default_hierarchy(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema="OBSERVATOIRE")
    hook.run("""
IF NOT EXISTS (SELECT 1 FROM dbo.METIER WHERE nom_metier=N'Construction')
    INSERT INTO dbo.METIER(nom_metier) VALUES (N'Construction');

DECLARE @id_metier INT = (SELECT id_metier FROM dbo.METIER WHERE nom_metier=N'Construction');

IF NOT EXISTS (SELECT 1 FROM dbo.CORPS_METIER WHERE nom_corps=N'Matériaux' AND id_metier=@id_metier)
    INSERT INTO dbo.CORPS_METIER(nom_corps,id_metier) VALUES (N'Matériaux', @id_metier);

DECLARE @id_corps INT = (SELECT c.id_corps FROM dbo.CORPS_METIER c JOIN dbo.METIER m ON c.id_metier=m.id_metier WHERE m.nom_metier=N'Construction' AND c.nom_corps=N'Matériaux');

IF NOT EXISTS (SELECT 1 FROM dbo.ACTIVITE WHERE nom_activite=N'Prix & Indices' AND id_corps=@id_corps)
    INSERT INTO dbo.ACTIVITE(nom_activite,id_corps) VALUES (N'Prix & Indices', @id_corps);
""")

def upsert_villes_from_staging(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema="OBSERVATOIRE")
    hook.run("""
;WITH src AS (
  SELECT DISTINCT agglomeration AS nom_ville
  FROM stg.PRIX_INDICE_RAW
  WHERE NULLIF(LTRIM(RTRIM(agglomeration)),'') IS NOT NULL
), prep AS (
  SELECT nom_ville,
         LOWER(REPLACE(REPLACE(REPLACE(nom_ville, N' ', N''), N'-', N''), CHAR(39), N'')) AS code_ville
  FROM src
)
INSERT INTO dbo.VILLE(nom_ville, code_ville, id_province_prefecture)
SELECT p.nom_ville, p.code_ville, NULL
FROM prep p
LEFT JOIN dbo.VILLE v ON v.nom_ville = p.nom_ville OR v.code_ville = p.code_ville
WHERE v.id_ville IS NULL;
""")

def upsert_produit_variete_from_staging(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema="OBSERVATOIRE")
    hook.run("""
DECLARE @id_activite INT = (
  SELECT a.id_activite
  FROM dbo.ACTIVITE a
  JOIN dbo.CORPS_METIER c ON a.id_corps=c.id_corps
  JOIN dbo.METIER m ON c.id_metier=m.id_metier
  WHERE m.nom_metier=N'Construction' AND c.nom_corps=N'Matériaux' AND a.nom_activite=N'Prix & Indices'
);

;WITH src AS (
  SELECT DISTINCT NULLIF(LTRIM(RTRIM(produit)),'') AS nom_produit
  FROM stg.PRIX_INDICE_RAW
  WHERE NULLIF(LTRIM(RTRIM(produit)),'') IS NOT NULL
)
INSERT INTO dbo.PRODUIT(nom_produit, id_activite)
SELECT s.nom_produit, @id_activite
FROM src s
LEFT JOIN dbo.PRODUIT p ON p.nom_produit = s.nom_produit AND p.id_activite=@id_activite
WHERE p.id_produit IS NULL;

;WITH src AS (
  SELECT DISTINCT
    NULLIF(LTRIM(RTRIM(variete)),'') AS nom_variete,
    NULLIF(LTRIM(RTRIM(produit)),'') AS nom_produit
  FROM stg.PRIX_INDICE_RAW
  WHERE NULLIF(LTRIM(RTRIM(variete)),'') IS NOT NULL
)
INSERT INTO dbo.VARIETE(nom_variete, id_produit)
SELECT s.nom_variete, p.id_produit
FROM src s
JOIN dbo.PRODUIT p ON p.nom_produit=s.nom_produit AND p.id_activite=@id_activite
LEFT JOIN dbo.VARIETE v ON v.nom_variete=s.nom_variete AND v.id_produit=p.id_produit
WHERE v.id_variete IS NULL;
""")

def load_fact_indices(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema="OBSERVATOIRE")
    # Deduplicate staging rows on business keys to avoid MERGE collisions on unique keys
    hook.run("""
;WITH d AS (
  SELECT agglomeration, variete, produit, annee, ISNULL(type_indice,N'') AS type_indice,
         ROW_NUMBER() OVER (
           PARTITION BY agglomeration, variete, produit, annee, ISNULL(type_indice,N'')
           ORDER BY load_ts DESC
         ) AS rn
  FROM stg.PRIX_INDICE_RAW
)
DELETE r
FROM stg.PRIX_INDICE_RAW r
JOIN d ON r.agglomeration=d.agglomeration AND r.variete=d.variete AND r.produit=d.produit
       AND r.annee=d.annee AND ISNULL(r.type_indice,N'')=d.type_indice
WHERE d.rn > 1;
""")
    hook.run("""
BEGIN TRY
    BEGIN TRAN;
    WITH src AS (
        SELECT
            r.annee,
            r.valeur_indice,
            r.prix_moyen,
            r.type_indice,
            v.id_variete   AS id_reference,
            vi.id_ville    AS id_ville
        FROM stg.PRIX_INDICE_RAW r
        JOIN dbo.VARIETE v ON v.nom_variete = r.variete
        JOIN dbo.PRODUIT p ON p.id_produit = v.id_produit AND p.nom_produit = r.produit
        JOIN dbo.ACTIVITE a ON a.id_activite = p.id_activite
        JOIN dbo.CORPS_METIER c ON c.id_corps = a.id_corps
        JOIN dbo.METIER m ON m.id_metier = c.id_metier
        JOIN dbo.VILLE vi ON vi.nom_ville = r.agglomeration
        WHERE m.nom_metier = N'Construction' AND c.nom_corps = N'Matériaux' AND a.nom_activite = N'Prix & Indices'
    )
    MERGE dbo.INDICE AS t
    USING src AS s
      ON  t.annee = s.annee
      AND t.id_reference = s.id_reference
      AND t.id_ville = s.id_ville
      AND ISNULL(t.type_indice,N'') = ISNULL(s.type_indice,N'')
    WHEN MATCHED THEN UPDATE
      SET t.valeur_indice = s.valeur_indice,
          t.prix_moyen    = s.prix_moyen
    WHEN NOT MATCHED THEN
      INSERT (annee, valeur_indice, prix_moyen, type_indice, id_reference, id_ville)
      VALUES (s.annee, s.valeur_indice, s.prix_moyen, s.type_indice, s.id_reference, s.id_ville);
    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;
    THROW;
END CATCH
""")

def load_fact_ponderations(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema="OBSERVATOIRE")
    hook.run("""
BEGIN TRY
  BEGIN TRAN;
  WITH src AS (
    SELECT pv.annee, pv.poids, N'ville' AS niveau,
           v.id_variete AS id_reference, vi.id_ville, CAST(NULL AS INT) AS id_region
    FROM stg.PONDERATION_VILLE_RAW pv
    JOIN dbo.VARIETE v ON v.nom_variete = pv.variete
    JOIN dbo.VILLE vi ON vi.nom_ville = pv.ville
  )
  MERGE dbo.PONDERATION AS t
  USING src AS s
    ON  t.annee = s.annee
    AND t.id_reference = s.id_reference
    AND t.niveau = s.niveau
    AND ISNULL(t.id_ville,-1) = ISNULL(s.id_ville,-1)
    AND ISNULL(t.id_region,-1) = ISNULL(s.id_region,-1)
  WHEN MATCHED THEN UPDATE SET t.poids = s.poids
  WHEN NOT MATCHED THEN
    INSERT (annee, poids, niveau, id_reference, id_ville, id_region)
    VALUES (s.annee, s.poids, s.niveau, s.id_reference, s.id_ville, s.id_region);
  COMMIT TRAN;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT > 0 ROLLBACK TRAN; THROW;
END CATCH
""")

    hook.run("""
BEGIN TRY
  BEGIN TRAN;
  WITH src AS (
    SELECT pr.annee, pr.poids, N'region' AS niveau,
           v.id_variete AS id_reference, CAST(NULL AS INT) AS id_ville, r.id_region
    FROM stg.PONDERATION_REGION_RAW pr
    JOIN dbo.VARIETE v ON v.nom_variete = pr.variete
    JOIN dbo.REGION r ON r.nom_region = pr.region
  )
  MERGE dbo.PONDERATION AS t
  USING src AS s
    ON  t.annee = s.annee
    AND t.id_reference = s.id_reference
    AND t.niveau = s.niveau
    AND ISNULL(t.id_ville,-1) = ISNULL(s.id_ville,-1)
    AND ISNULL(t.id_region,-1) = ISNULL(s.id_region,-1)
  WHEN MATCHED THEN UPDATE SET t.poids = s.poids
  WHEN NOT MATCHED THEN
    INSERT (annee, poids, niveau, id_reference, id_ville, id_region)
    VALUES (s.annee, s.poids, s.niveau, s.id_reference, s.id_ville, s.id_region);
  COMMIT TRAN;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT > 0 ROLLBACK TRAN; THROW;
END CATCH
""")

def dq_checks(**ctx):
    hook = SqlHook(conn_id="mssql_default", schema="OBSERVATOIRE")
    metrics = {
        "run_id": ctx["run_id"],
        "utc": datetime.utcnow().isoformat(),
        "counts": {
            "stg_prix_indice": hook.get_first("SELECT COUNT(*) FROM stg.PRIX_INDICE_RAW")[0],
            "stg_pond_ville":  hook.get_first("SELECT COUNT(*) FROM stg.PONDERATION_VILLE_RAW")[0],
            "stg_pond_region": hook.get_first("SELECT COUNT(*) FROM stg.PONDERATION_REGION_RAW")[0],
            "indice":          hook.get_first("SELECT COUNT(*) FROM dbo.INDICE")[0],
            "ponderation":     hook.get_first("SELECT COUNT(*) FROM dbo.PONDERATION")[0],
        }
    }
    if metrics["counts"]["indice"] == 0:
        raise ValueError("DQ FAIL: INDICE vide après chargement")
    report_dir = os.path.join(BASE_DIR, "reports", "dq")
    os.makedirs(report_dir, exist_ok=True)
    path = os.path.join(report_dir, f"dq_{_slug(ctx['run_id'])}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    logger.info(f"DQ Report → {path}`n{json.dumps(metrics, indent=2, ensure_ascii=False)}")

def archive_inputs(**ctx):
    src_zip = ctx["ti"].xcom_pull(key="src_zip", task_ids="detect_zip")
    landing_dir = ctx["ti"].xcom_pull(key="landing_dir", task_ids="unzip_to_landing")
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    os.makedirs(ARCH_DIR, exist_ok=True)
    shutil.move(src_zip, os.path.join(ARCH_DIR, f"{ts}__{os.path.basename(src_zip)}"))
    shutil.make_archive(os.path.join(ARCH_DIR, f"{ts}__landing"), "zip", root_dir=landing_dir)

with DAG(
    "etl_prix_indices",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng"},
    tags=["observatoire","etl","prix_indices"],
) as dag:

    ddl_staging = SQLExecuteQueryOperator(
        task_id="ddl_040_staging",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql="sql/observatoire/040_staging.sql",
    )

    ddl_constraints = SQLExecuteQueryOperator(
        task_id="ddl_095_constraints",
        conn_id="mssql_default",
        hook_params={"schema": "OBSERVATOIRE"},
        autocommit=True,
        sql="sql/observatoire/095_constraints.sql",
    )

    t_detect   = PythonOperator(task_id="detect_zip", python_callable=detect_zip)
    t_unzip    = PythonOperator(task_id="unzip_to_landing", python_callable=unzip_to_landing)

    t_stage_clear   = PythonOperator(task_id="stage_clear", python_callable=stage_clear)
    t_stage_indices = PythonOperator(task_id="stage_load_prix_indices", python_callable=stage_load_prix_indices)
    t_stage_pond    = PythonOperator(task_id="stage_load_ponderations", python_callable=stage_load_ponderations)

    t_ensure_chain  = PythonOperator(task_id="ensure_default_hierarchy", python_callable=ensure_default_hierarchy)
    t_up_villes     = PythonOperator(task_id="upsert_villes_from_staging", python_callable=upsert_villes_from_staging)
    t_up_refs       = PythonOperator(task_id="upsert_produit_variete_from_staging", python_callable=upsert_produit_variete_from_staging)

    t_load_indice   = PythonOperator(task_id="load_fact_indices", python_callable=load_fact_indices)
    t_load_pond     = PythonOperator(task_id="load_fact_ponderations", python_callable=load_fact_ponderations)

    t_dq            = PythonOperator(task_id="dq_checks", python_callable=dq_checks)
    t_archive       = PythonOperator(task_id="archive_inputs", python_callable=archive_inputs)

    ddl_staging >> ddl_constraints >> t_detect >> t_unzip >> t_stage_clear

    # Expand list chaining for Airflow versions without list >> list support
    # Ensure ponderations generate after indices are staged (they depend on staged variete/annee)
    t_stage_clear >> t_stage_indices >> t_stage_pond
    t_stage_indices >> t_ensure_chain
    t_stage_pond >> t_ensure_chain

    t_ensure_chain >> [t_up_villes, t_up_refs]
    t_up_villes >> [t_load_indice, t_load_pond]
    t_up_refs   >> [t_load_indice, t_load_pond]
    [t_load_indice, t_load_pond] >> t_dq
    t_dq >> t_archive


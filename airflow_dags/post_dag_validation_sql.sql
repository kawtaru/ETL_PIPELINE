-- ============================================================================
-- OBSERVATOIRE Post-DAG Validation Script
-- Run after: obs_create_schema → etl_bronze_raw → etl_silver_referentiels 
--                              → etl_gold_dimensions → etl_gold_facts
-- ============================================================================

USE OBSERVATOIRE;

PRINT '╔════════════════════════════════════════════════════════════════════════╗';
PRINT '║                   POST-DAG VALIDATION REPORT                          ║';
PRINT '║                        ' + CONVERT(VARCHAR, GETUTCDATE(), 121) + '                        ║';
PRINT '╚════════════════════════════════════════════════════════════════════════╝';

-- ============================================================================
-- 1. VERIFY SCHEMA STRUCTURE
-- ============================================================================
PRINT '';
PRINT '1️⃣  SCHEMA STRUCTURE VERIFICATION';
PRINT '───────────────────────────────────────────────────────────────────────';

DECLARE @table_count INT;
SELECT @table_count = COUNT(*)
FROM information_schema.tables
WHERE table_schema = 'dbo' AND table_type = 'BASE TABLE';

PRINT 'Tables in dbo schema: ' + CAST(@table_count AS VARCHAR);

SELECT 
  TABLE_NAME,
  (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_name = t.TABLE_NAME) AS column_count
FROM information_schema.tables t
WHERE table_schema = 'dbo' AND table_type = 'BASE TABLE'
ORDER BY TABLE_NAME;

-- ============================================================================
-- 2. COLUMN NAME VERIFICATION (CRITICAL)
-- ============================================================================
PRINT '';
PRINT '2️⃣  COLUMN NAME VERIFICATION (id_commune vs id_ville)';
PRINT '───────────────────────────────────────────────────────────────────────';

-- Check INDICE columns
SELECT 
  'INDICE' AS table_name,
  STRING_AGG(COLUMN_NAME, ', ') AS columns
FROM information_schema.columns
WHERE TABLE_NAME = 'INDICE' AND table_schema = 'dbo'
GROUP BY TABLE_NAME
UNION ALL
SELECT 
  'PONDERATION' AS table_name,
  STRING_AGG(COLUMN_NAME, ', ') AS columns
FROM information_schema.columns
WHERE TABLE_NAME = 'PONDERATION' AND table_schema = 'dbo'
GROUP BY TABLE_NAME;

-- Verify no legacy id_ville columns exist
DECLARE @legacy_count INT = (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE COLUMN_NAME = 'id_ville' AND table_schema = 'dbo'
);

IF @legacy_count > 0
  PRINT '❌ WARNING: Legacy id_ville columns still exist!';
ELSE
  PRINT '✅ OK: No legacy id_ville columns found';

-- ============================================================================
-- 3. DIMENSION TABLE COUNTS
-- ============================================================================
PRINT '';
PRINT '3️⃣  DIMENSION TABLE COUNTS';
PRINT '───────────────────────────────────────────────────────────────────────';

SELECT 
  'REGION' AS table_name,
  COUNT(*) AS row_count
FROM dbo.REGION
UNION ALL
SELECT 'PROVINCE_PREFECTURE', COUNT(*) FROM dbo.PROVINCE_PREFECTURE
UNION ALL
SELECT 'COMMUNE', COUNT(*) FROM dbo.COMMUNE
UNION ALL
SELECT 'METIER', COUNT(*) FROM dbo.METIER
UNION ALL
SELECT 'CORPS_METIER', COUNT(*) FROM dbo.CORPS_METIER
UNION ALL
SELECT 'ACTIVITE', COUNT(*) FROM dbo.ACTIVITE
UNION ALL
SELECT 'PRODUIT', COUNT(*) FROM dbo.PRODUIT
UNION ALL
SELECT 'VARIETE', COUNT(*) FROM dbo.VARIETE
ORDER BY table_name;

-- ============================================================================
-- 4. FACT TABLE COUNTS
-- ============================================================================
PRINT '';
PRINT '4️⃣  FACT TABLE COUNTS';
PRINT '───────────────────────────────────────────────────────────────────────';

BEGIN
  DECLARE @indice_cnt INT = (SELECT COUNT(*) FROM dbo.INDICE);
  DECLARE @pond_cnt INT = (SELECT COUNT(*) FROM dbo.PONDERATION);
  
  SELECT 
    'INDICE' AS table_name,
    @indice_cnt AS row_count,
    (SELECT COUNT(DISTINCT annee) FROM dbo.INDICE) AS years,
    (SELECT MIN(annee) FROM dbo.INDICE) AS min_year,
    (SELECT MAX(annee) FROM dbo.INDICE) AS max_year
  UNION ALL
  SELECT 
    'PONDERATION',
    @pond_cnt,
    (SELECT COUNT(DISTINCT annee) FROM dbo.PONDERATION),
    (SELECT MIN(annee) FROM dbo.PONDERATION),
    (SELECT MAX(annee) FROM dbo.PONDERATION);
END;

-- ============================================================================
-- 5. FOREIGN KEY INTEGRITY - INDICE
-- ============================================================================
PRINT '';
PRINT '5️⃣  FOREIGN KEY INTEGRITY - INDICE (id_commune & id_region)';
PRINT '───────────────────────────────────────────────────────────────────────';

BEGIN
  DECLARE @ind_total INT = (SELECT COUNT(*) FROM dbo.INDICE);
  DECLARE @ind_null_commune INT = (SELECT COUNT(*) FROM dbo.INDICE WHERE id_commune IS NULL);
  DECLARE @ind_null_region INT = (SELECT COUNT(*) FROM dbo.INDICE WHERE id_region IS NULL);
  DECLARE @ind_orphan_commune INT = (
    SELECT COUNT(*) FROM dbo.INDICE 
    WHERE id_commune IS NOT NULL 
    AND id_commune NOT IN (SELECT id_commune FROM dbo.COMMUNE)
  );
  DECLARE @ind_orphan_region INT = (
    SELECT COUNT(*) FROM dbo.INDICE 
    WHERE id_region IS NOT NULL 
    AND id_region NOT IN (SELECT id_region FROM dbo.REGION)
  );
  
  SELECT
    @ind_total AS total_indice_rows,
    @ind_null_commune AS null_id_commune,
    @ind_null_region AS null_id_region,
    @ind_orphan_commune AS orphaned_id_commune,
    @ind_orphan_region AS orphaned_id_region;
    
  IF @ind_orphan_commune > 0 OR @ind_orphan_region > 0
    PRINT '❌ WARNING: Orphaned foreign keys detected!';
  ELSE IF @ind_total = 0
    PRINT '⚠️  WARNING: INDICE table is empty';
  ELSE
    PRINT '✅ OK: INDICE foreign keys valid';
END;

-- ============================================================================
-- 6. FOREIGN KEY INTEGRITY - PONDERATION
-- ============================================================================
PRINT '';
PRINT '6️⃣  FOREIGN KEY INTEGRITY - PONDERATION (id_commune & id_region)';
PRINT '───────────────────────────────────────────────────────────────────────';

BEGIN
  DECLARE @pond_total INT = (SELECT COUNT(*) FROM dbo.PONDERATION);
  DECLARE @pond_commune_rows INT = (SELECT COUNT(*) FROM dbo.PONDERATION WHERE niveau = 'COMMUNE');
  DECLARE @pond_region_rows INT = (SELECT COUNT(*) FROM dbo.PONDERATION WHERE niveau = 'REGION');
  DECLARE @pond_null_commune INT = (
    SELECT COUNT(*) FROM dbo.PONDERATION 
    WHERE niveau = 'COMMUNE' AND id_commune IS NULL
  );
  DECLARE @pond_null_region INT = (
    SELECT COUNT(*) FROM dbo.PONDERATION 
    WHERE niveau = 'REGION' AND id_region IS NULL
  );
  DECLARE @pond_orphan_commune INT = (
    SELECT COUNT(*) FROM dbo.PONDERATION 
    WHERE id_commune IS NOT NULL 
    AND id_commune NOT IN (SELECT id_commune FROM dbo.COMMUNE)
  );
  DECLARE @pond_orphan_region INT = (
    SELECT COUNT(*) FROM dbo.PONDERATION 
    WHERE id_region IS NOT NULL 
    AND id_region NOT IN (SELECT id_region FROM dbo.REGION)
  );
  
  SELECT
    @pond_total AS total_ponderation_rows,
    @pond_commune_rows AS commune_level_rows,
    @pond_region_rows AS region_level_rows,
    @pond_null_commune AS commune_missing_fk,
    @pond_null_region AS region_missing_fk,
    @pond_orphan_commune AS orphaned_id_commune,
    @pond_orphan_region AS orphaned_id_region;
    
  IF @pond_orphan_commune > 0 OR @pond_orphan_region > 0
    PRINT '❌ WARNING: Orphaned foreign keys detected!';
  ELSE IF @pond_total = 0
    PRINT '⚠️  WARNING: PONDERATION table is empty (may be normal)';
  ELSE
    PRINT '✅ OK: PONDERATION foreign keys valid';
END;

-- ============================================================================
-- 7. GEOGRAPHIC HIERARCHY
-- ============================================================================
PRINT '';
PRINT '7️⃣  GEOGRAPHIC HIERARCHY (Region → Province → Commune)';
PRINT '───────────────────────────────────────────────────────────────────────';

SELECT TOP 10
  r.nom_region,
  COUNT(DISTINCT pp.id_province_prefecture) AS province_count,
  COUNT(DISTINCT c.id_commune) AS commune_count
FROM dbo.REGION r
LEFT JOIN dbo.PROVINCE_PREFECTURE pp ON pp.id_region = r.id_region
LEFT JOIN dbo.COMMUNE c ON c.id_province_prefecture = pp.id_province_prefecture
GROUP BY r.nom_region
ORDER BY region_name DESC;

-- ============================================================================
-- 8. INDICE DISTRIBUTION BY GEOGRAPHIC LEVEL
-- ============================================================================
PRINT '';
PRINT '8️⃣  INDICE DISTRIBUTION BY GEOGRAPHIC LEVEL';
PRINT '───────────────────────────────────────────────────────────────────────';

SELECT
  'REGION' AS level,
  COUNT(*) AS row_count,
  COUNT(DISTINCT id_region) AS distinct_locations,
  COUNT(DISTINCT annee) AS years
FROM dbo.INDICE
WHERE id_region IS NOT NULL
UNION ALL
SELECT
  'COMMUNE' AS level,
  COUNT(*),
  COUNT(DISTINCT id_commune),
  COUNT(DISTINCT annee)
FROM dbo.INDICE
WHERE id_commune IS NOT NULL;

-- ============================================================================
-- 9. INDICE BY TYPE
-- ============================================================================
PRINT '';
PRINT '9️⃣  INDICE DISTRIBUTION BY TYPE';
PRINT '───────────────────────────────────────────────────────────────────────';

SELECT
  ISNULL(type_indice, 'NULL') AS type_indice,
  COUNT(*) AS row_count,
  COUNT(DISTINCT annee) AS years,
  COUNT(DISTINCT id_reference) AS distinct_references,
  MIN(valeur_indice) AS min_value,
  MAX(valeur_indice) AS max_value,
  AVG(valeur_indice) AS avg_value
FROM dbo.INDICE
GROUP BY type_indice
ORDER BY row_count DESC;

-- ============================================================================
-- 10. PONDERATION DISTRIBUTION BY NIVEAU
-- ============================================================================
PRINT '';
PRINT '🔟 PONDERATION DISTRIBUTION BY NIVEAU';
PRINT '───────────────────────────────────────────────────────────────────────';

SELECT
  niveau,
  COUNT(*) AS row_count,
  COUNT(DISTINCT annee) AS years,
  COUNT(DISTINCT id_reference) AS distinct_references,
  MIN(poids) AS min_poids,
  MAX(poids) AS max_poids,
  AVG(poids) AS avg_poids
FROM dbo.PONDERATION
GROUP BY niveau
ORDER BY row_count DESC;

-- ============================================================================
-- 11. METIER HIERARCHY
-- ============================================================================
PRINT '';
PRINT '1️⃣1️⃣  METIER HIERARCHY (Construction → Corps → Activite → Produit → Variete)';
PRINT '───────────────────────────────────────────────────────────────────────';

SELECT
  m.nom_metier,
  COUNT(DISTINCT cm.id_corps) AS corps_count,
  COUNT(DISTINCT a.id_activite) AS activite_count,
  COUNT(DISTINCT p.id_produit) AS produit_count,
  COUNT(DISTINCT v.id_variete) AS variete_count
FROM dbo.METIER m
LEFT JOIN dbo.CORPS_METIER cm ON cm.id_metier = m.id_metier
LEFT JOIN dbo.ACTIVITE a ON a.id_corps = cm.id_corps
LEFT JOIN dbo.PRODUIT p ON p.id_activite = a.id_activite
LEFT JOIN dbo.VARIETE v ON v.id_produit = p.id_produit
GROUP BY m.nom_metier;

-- ============================================================================
-- 12. SAMPLE DATA - Top rows
-- ============================================================================
PRINT '';
PRINT '1️⃣2️⃣  SAMPLE DATA - Top 5 INDICE rows:';
PRINT '───────────────────────────────────────────────────────────────────────';

SELECT TOP 5
  i.id_indice,
  i.annee,
  i.type_indice,
  i.valeur_indice,
  i.prix_moyen,
  COALESCE(c.nom_commune, 'NULL') AS commune_name,
  COALESCE(r.nom_region, 'NULL') AS region_name
FROM dbo.INDICE i
LEFT JOIN dbo.COMMUNE c ON c.id_commune = i.id_commune
LEFT JOIN dbo.REGION r ON r.id_region = i.id_region
ORDER BY i.id_indice DESC;

PRINT '';
PRINT '1️⃣3️⃣  SAMPLE DATA - Top 5 PONDERATION rows:';
PRINT '───────────────────────────────────────────────────────────────────────';

SELECT TOP 5
  p.id_ponderation,
  p.annee,
  p.niveau,
  p.poids,
  COALESCE(c.nom_commune, 'NULL') AS commune_name,
  COALESCE(r.nom_region, 'NULL') AS region_name
FROM dbo.PONDERATION p
LEFT JOIN dbo.COMMUNE c ON c.id_commune = p.id_commune
LEFT JOIN dbo.REGION r ON r.id_region = p.id_region
ORDER BY p.id_ponderation DESC;

-- ============================================================================
-- 13. FINAL HEALTH CHECK
-- ============================================================================
PRINT '';
PRINT '1️⃣4️⃣  FINAL HEALTH CHECK';
PRINT '───────────────────────────────────────────────────────────────────────';

BEGIN
  DECLARE @reg_ok BIT = CASE WHEN (SELECT COUNT(*) FROM dbo.REGION) > 0 THEN 1 ELSE 0 END;
  DECLARE @prov_ok BIT = CASE WHEN (SELECT COUNT(*) FROM dbo.PROVINCE_PREFECTURE) > 0 THEN 1 ELSE 0 END;
  DECLARE @comm_ok BIT = CASE WHEN (SELECT COUNT(*) FROM dbo.COMMUNE) > 0 THEN 1 ELSE 0 END;
  DECLARE @ind_ok BIT = CASE WHEN (SELECT COUNT(*) FROM dbo.INDICE) > 0 THEN 1 ELSE 0 END;
  DECLARE @pond_ok BIT = CASE WHEN (SELECT COUNT(*) FROM dbo.PONDERATION) > 0 THEN 1 ELSE 0 END;
  DECLARE @all_ok BIT = CASE WHEN @reg_ok=1 AND @prov_ok=1 AND @ind_ok=1 THEN 1 ELSE 0 END;
  
  SELECT
    CASE WHEN @reg_ok = 1 THEN '✅' ELSE '❌' END + ' REGION' AS check_region,
    CASE WHEN @prov_ok = 1 THEN '✅' ELSE '❌' END + ' PROVINCE_PREFECTURE' AS check_province,
    CASE WHEN @comm_ok = 1 THEN '✅' ELSE '⚠️' END + ' COMMUNE' AS check_commune,
    CASE WHEN @ind_ok = 1 THEN '✅' ELSE '❌' END + ' INDICE' AS check_indice,
    CASE WHEN @pond_ok = 1 THEN '✅' ELSE '⚠️' END + ' PONDERATION' AS check_ponderation,
    CASE WHEN @all_ok = 1 THEN '🎉 ALL OK!' ELSE '⚠️  ISSUES DETECTED' END AS overall_status;
END;

PRINT '';
PRINT '╔════════════════════════════════════════════════════════════════════════╗';
PRINT '║                    END OF VALIDATION REPORT                           ║';
PRINT '╚════════════════════════════════════════════════════════════════════════╝';
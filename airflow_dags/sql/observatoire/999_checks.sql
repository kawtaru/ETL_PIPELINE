-- Optional validation checks to confirm objects exist

-- You can expand with THROWs if needed, but selects are harmless in operators.

SELECT 'REGION'   AS object_name, OBJECT_ID('dbo.REGION','U')             AS object_id;
SELECT 'PROVINCE' AS object_name, OBJECT_ID('dbo.PROVINCE_PREFECTURE','U') AS object_id;
SELECT 'VILLE'    AS object_name, OBJECT_ID('dbo.VILLE','U')              AS object_id;
SELECT 'COMMUNE'  AS object_name, OBJECT_ID('dbo.COMMUNE','U')            AS object_id;
SELECT 'QUARTIER' AS object_name, OBJECT_ID('dbo.QUARTIER','U')           AS object_id;

SELECT 'METIER'   AS object_name, OBJECT_ID('dbo.METIER','U')             AS object_id;
SELECT 'CORPS'    AS object_name, OBJECT_ID('dbo.CORPS_METIER','U')       AS object_id;
SELECT 'ACTIVITE' AS object_name, OBJECT_ID('dbo.ACTIVITE','U')           AS object_id;
SELECT 'PRODUIT'  AS object_name, OBJECT_ID('dbo.PRODUIT','U')            AS object_id;
SELECT 'VARIETE'  AS object_name, OBJECT_ID('dbo.VARIETE','U')            AS object_id;

SELECT 'PONDERATION' AS object_name, OBJECT_ID('dbo.PONDERATION','U')     AS object_id;
SELECT 'INDICE'      AS object_name, OBJECT_ID('dbo.INDICE','U')          AS object_id;


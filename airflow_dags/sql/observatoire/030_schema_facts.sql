
-- PONDERATION
IF OBJECT_ID('dbo.PONDERATION','U') IS NULL
CREATE TABLE dbo.PONDERATION (
  id_ponderation INT IDENTITY(1,1) PRIMARY KEY,
  annee INT NOT NULL,
  poids DECIMAL(18,6) NOT NULL,
  niveau NVARCHAR(50) NOT NULL,
  id_reference INT NOT NULL FOREIGN KEY REFERENCES dbo.VARIETE(id_variete),
  id_ville INT NULL FOREIGN KEY REFERENCES dbo.VILLE(id_ville),
  id_region INT NULL FOREIGN KEY REFERENCES dbo.REGION(id_region),

  -- Nouvelles FK pour refléter la hiérarchie complète :
  id_produit INT NULL FOREIGN KEY REFERENCES dbo.PRODUIT(id_produit),
  id_activite INT NULL FOREIGN KEY REFERENCES dbo.ACTIVITE(id_activite),
  id_corps INT NULL FOREIGN KEY REFERENCES dbo.CORPS_METIER(id_corps),
  id_metier INT NULL FOREIGN KEY REFERENCES dbo.METIER(id_metier),

  CONSTRAINT CK_POND_SCOPE CHECK(
    (niveau='region' AND id_region IS NOT NULL AND id_ville IS NULL) OR
    (niveau='ville'  AND id_ville  IS NOT NULL AND id_region IS NULL)
  )
);

-- INDICE
IF OBJECT_ID('dbo.INDICE','U') IS NULL
CREATE TABLE dbo.INDICE (
  id_indice INT IDENTITY(1,1) PRIMARY KEY,
  annee INT NOT NULL,
  valeur_indice DECIMAL(18,6) NULL,
  prix_moyen DECIMAL(18,6) NULL,
  type_indice NVARCHAR(50) NULL,
  id_reference INT NOT NULL FOREIGN KEY REFERENCES dbo.VARIETE(id_variete),
  id_ville INT NULL FOREIGN KEY REFERENCES dbo.VILLE(id_ville),
  CONSTRAINT CK_INDICE_MEAS CHECK (valeur_indice IS NOT NULL OR prix_moyen IS NOT NULL)
);

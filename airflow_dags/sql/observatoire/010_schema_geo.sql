-- Geographic schema: REGION, PROVINCE_PREFECTURE, VILLE, COMMUNE, QUARTIER

-- REGION
IF OBJECT_ID('dbo.REGION','U') IS NULL
CREATE TABLE dbo.REGION(
  id_region INT IDENTITY(1,1) PRIMARY KEY,
  nom_region NVARCHAR(150) NOT NULL,
  code_region NVARCHAR(20) NOT NULL UNIQUE
);

-- PROVINCE_PREFECTURE
IF OBJECT_ID('dbo.PROVINCE_PREFECTURE','U') IS NULL
CREATE TABLE dbo.PROVINCE_PREFECTURE(
  id_province_prefecture INT IDENTITY(1,1) PRIMARY KEY,
  nom_province_prefecture NVARCHAR(150) NOT NULL,
  code_province_prefecture NVARCHAR(20) NOT NULL UNIQUE,
  id_region INT NOT NULL
    FOREIGN KEY REFERENCES dbo.REGION(id_region)
);

-- VILLE (agglomération)  -- FK province temporairement NULL
IF OBJECT_ID('dbo.VILLE','U') IS NULL
CREATE TABLE dbo.VILLE(
  id_ville INT IDENTITY(1,1) PRIMARY KEY,
  nom_ville NVARCHAR(150) NOT NULL,
  code_ville NVARCHAR(50) NULL UNIQUE,
  id_province_prefecture INT NULL
    FOREIGN KEY REFERENCES dbo.PROVINCE_PREFECTURE(id_province_prefecture)
);

-- COMMUNE
IF OBJECT_ID('dbo.COMMUNE','U') IS NULL
CREATE TABLE dbo.COMMUNE(
  id_commune INT IDENTITY(1,1) PRIMARY KEY,
  nom_commune NVARCHAR(150) NOT NULL,
  code_commune NVARCHAR(20) NULL UNIQUE,
  id_province_prefecture INT NOT NULL
    FOREIGN KEY REFERENCES dbo.PROVINCE_PREFECTURE(id_province_prefecture),
  id_ville INT NULL
    FOREIGN KEY REFERENCES dbo.VILLE(id_ville)
);

-- QUARTIER (structure prête)
IF OBJECT_ID('dbo.QUARTIER','U') IS NULL
CREATE TABLE dbo.QUARTIER(
  id_quartier INT IDENTITY(1,1) PRIMARY KEY,
  nom_quartier NVARCHAR(150) NOT NULL,
  code_quartier NVARCHAR(20) NULL UNIQUE,
  id_ville INT NULL FOREIGN KEY REFERENCES dbo.VILLE(id_ville),
  id_commune INT NULL FOREIGN KEY REFERENCES dbo.COMMUNE(id_commune),
  type_rattachement NVARCHAR(20) NOT NULL,
  CONSTRAINT CK_QUARTIER_ONE_LINK CHECK(
    (type_rattachement='ville' AND id_ville IS NOT NULL AND id_commune IS NULL) OR
    (type_rattachement='commune' AND id_commune IS NOT NULL AND id_ville IS NULL)
  )
);


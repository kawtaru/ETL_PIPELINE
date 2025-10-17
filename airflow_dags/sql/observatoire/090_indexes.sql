-- Indexes for performance (idempotent)

-- PONDERATION indexes
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes 
  WHERE name = N'IX_POND_REF_SCOPE' 
    AND object_id = OBJECT_ID(N'dbo.PONDERATION')
)
BEGIN
  CREATE INDEX IX_POND_REF_SCOPE ON dbo.PONDERATION(id_reference, id_region, id_ville, annee);
END;

-- INDICE indexes
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes 
  WHERE name = N'IX_INDICE_REF_LOC' 
    AND object_id = OBJECT_ID(N'dbo.INDICE')
)
BEGIN
  CREATE INDEX IX_INDICE_REF_LOC ON dbo.INDICE(id_reference, id_ville, annee);
END;


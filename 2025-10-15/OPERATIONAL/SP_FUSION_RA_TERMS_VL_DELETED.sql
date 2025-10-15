CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FUSION_RA_TERMS_VL_DELETED()
BEGIN
  -- Step 1: Crear una tabla temporal con los RATERMBTERMID de RAW.RA_TERMS_VL que no están en STAGING.RA_TERMS_VL_PK
  CREATE TEMP TABLE ids_a_actualizar6 AS
  SELECT
    terms.RATERMBTERMID
  FROM
    `amrl-data-prd.RAW.FUSION_RA_TERMS_VL` AS terms
  LEFT JOIN
    `amrl-data-prd.STAGING.FUSION_RA_TERMS_VL_PK` AS terms_pk
  ON
    terms.RATERMBTERMID = terms_pk.RATERMBTERMID
  WHERE
    terms_pk.RATERMBTERMID IS NULL;

  -- Step 2: Actualizar la columna DELETED en la tabla RAW.RA_TERMS_VL para los registros que están en la tabla temporal
  UPDATE
    `amrl-data-prd.RAW.FUSION_RA_TERMS_VL`
  SET
    DELETED = '1'
  WHERE
    RATERMBTERMID IN (SELECT RATERMBTERMID FROM ids_a_actualizar6);

  -- Step 3: Limpiar la tabla temporal
  DROP TABLE ids_a_actualizar6;
END;
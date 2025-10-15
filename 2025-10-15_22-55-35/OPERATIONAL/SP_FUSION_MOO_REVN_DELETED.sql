CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FUSION_MOO_REVN_DELETED()
BEGIN
  -- Step 1: Crear una tabla temporal con los REVNID de RAW.MOO_REVN que no están en STAGING.MOO_REVN_PK
  CREATE TEMP TABLE ids_a_actualizar2 AS
  SELECT
    revn.REVENUEREVNID
  FROM
    `amrl-data-prd.RAW.FUSION_MOO_REVN` AS revn
  LEFT JOIN
    `amrl-data-prd.STAGING.FUSION_MOO_REVN_PK` AS revn_pk
  ON
    revn.REVENUEREVNID = revn_pk.REVENUEREVNID
  WHERE
    revn_pk.REVENUEREVNID IS NULL;

  -- Step 2: Actualizar la columna DELETED en la tabla RAW.MOO_REVN para los registros que están en la tabla temporal
  UPDATE
    `amrl-data-prd.RAW.FUSION_MOO_REVN`
  SET
    DELETED = '1'
  WHERE
    REVENUEREVNID IN (SELECT REVENUEREVNID FROM ids_a_actualizar2);

  -- Step 3: Limpiar la tabla temporal
  DROP TABLE ids_a_actualizar2;
END;
CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FUSION_MOO_OPTY_DELETED()
BEGIN
  -- Step 1: Crear una tabla temporal con los OPTYID de RAW.MOO_OPTY que no están en STAGING.MOO_OPTY_PK
  CREATE TEMP TABLE ids_a_actualizar AS
  SELECT
    opty.OPTYID
  FROM
    `amrl-data-prd.RAW.FUSION_MOO_OPTY` AS opty
  LEFT JOIN
    `amrl-data-prd.STAGING.FUSION_MOO_OPTY_PK` AS opty_pk
  ON
    opty.OPTYID = opty_pk.OPTYID
  WHERE
    opty_pk.OPTYID IS NULL;

  -- Step 2: Actualizar la columna DELETED en la tabla RAW.MOO_OPTY para los registros que están en la tabla temporal
  UPDATE
    `amrl-data-prd.RAW.FUSION_MOO_OPTY`
  SET
    DELETED = '1'
  WHERE
    OPTYID IN (SELECT OPTYID FROM ids_a_actualizar);

  -- Step 3: Limpiar la tabla temporal
  DROP TABLE ids_a_actualizar;
END;
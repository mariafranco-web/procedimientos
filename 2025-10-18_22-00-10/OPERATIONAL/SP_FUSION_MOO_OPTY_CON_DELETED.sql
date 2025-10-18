CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FUSION_MOO_OPTY_CON_DELETED()
BEGIN
  -- Step 1: Crear una tabla temporal con los OPTYCONID de RAW.MOO_OPTY_CON que no están en STAGING.MOO_OPTY_CON_PK
  CREATE TEMP TABLE ids_a_actualizar1 AS
  SELECT
    opty_con.OPTYCONID
  FROM
    `amrl-data-prd.RAW.FUSION_MOO_OPTY_CON` AS opty_con
  LEFT JOIN
    `amrl-data-prd.STAGING.FUSION_MOO_OPTY_CON_PK` AS opty_con_pk
  ON
    opty_con.OPTYCONID = opty_con_pk.OPTYCONID
  WHERE
    opty_con_pk.OPTYCONID IS NULL;

  -- Step 2: Actualizar la columna DELETED en la tabla RAW.MOO_OPTY_CON para los registros que están en la tabla temporal
  UPDATE
    `amrl-data-prd.RAW.FUSION_MOO_OPTY_CON`
  SET
    DELETED = '1'
  WHERE
    OPTYCONID IN (SELECT OPTYCONID FROM ids_a_actualizar1);

  -- Step 3: Limpiar la tabla temporal
  DROP TABLE ids_a_actualizar1;
END;
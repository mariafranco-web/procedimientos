CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FUSION_RA_CUSTOMER_TRX_ALL_DELETED()
BEGIN
  -- Step 1: Crear una tabla temporal con los RACUSTOMERTRXCUSTOMERTRXID de RAW.RA_CUSTOMER_TRX_ALL que no están en STAGING.RA_CUSTOMER_TRX_ALL_PK
  CREATE TEMP TABLE ids_a_actualizar3 AS
  SELECT
    trx.RACUSTOMERTRXCUSTOMERTRXID
  FROM
    `amrl-data-prd.RAW.FUSION_RA_CUSTOMER_TRX_ALL` AS trx
  LEFT JOIN
    `amrl-data-prd.STAGING.FUSION_RA_CUSTOMER_TRX_ALL_PK` AS trx_pk
  ON
    trx.RACUSTOMERTRXCUSTOMERTRXID = trx_pk.RACUSTOMERTRXCUSTOMERTRXID
  WHERE
    trx_pk.RACUSTOMERTRXCUSTOMERTRXID IS NULL;

  -- Step 2: Actualizar la columna DELETED en la tabla RAW.RA_CUSTOMER_TRX_ALL para los registros que están en la tabla temporal
  UPDATE
    `amrl-data-prd.RAW.FUSION_RA_CUSTOMER_TRX_ALL`
  SET
    DELETED = '1'
  WHERE
    RACUSTOMERTRXCUSTOMERTRXID IN (SELECT RACUSTOMERTRXCUSTOMERTRXID FROM ids_a_actualizar3);

  -- Step 3: Limpiar la tabla temporal
  DROP TABLE ids_a_actualizar3;
END;
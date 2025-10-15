CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FUSION_ZCA_SALES_ORDER_HEADERS_DELETED()
BEGIN
  -- Step 1: Crear una tabla temporal con los ORDERHEADERID de RAW.ZCA_SALES_ORDER_HEADERS que no están en STAGING.ZCA_SALES_ORDER_HEADERS_PK
  CREATE TEMP TABLE ids_a_actualizar7 AS
  SELECT
    headers.ORDERHEADERID
  FROM
    `amrl-data-prd.RAW.FUSION_ZCA_SALES_ORDER_HEADERS` AS headers
  LEFT JOIN
    `amrl-data-prd.STAGING.FUSION_ZCA_SALES_ORDER_HEADERS_PK` AS headers_pk
  ON
    headers.ORDERHEADERID = headers_pk.ORDERHEADERID
  WHERE
    headers_pk.ORDERHEADERID IS NULL;

  -- Step 2: Actualizar la columna DELETED en la tabla RAW.ZCA_SALES_ORDER_HEADERS para los registros que están en la tabla temporal
  UPDATE
    `amrl-data-prd.RAW.FUSION_ZCA_SALES_ORDER_HEADERS`
  SET
    DELETED = '1'
  WHERE
    ORDERHEADERID IN (SELECT ORDERHEADERID FROM ids_a_actualizar7);

  -- Step 3: Limpiar la tabla temporal
  DROP TABLE ids_a_actualizar7;
END;
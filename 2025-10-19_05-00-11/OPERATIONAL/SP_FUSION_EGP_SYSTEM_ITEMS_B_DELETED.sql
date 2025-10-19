CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FUSION_EGP_SYSTEM_ITEMS_B_DELETED()
BEGIN
-- Step 1: Crear una tabla temporal para almacenar los IDs que deben ser actualizados
CREATE TEMP TABLE ids_a_actualizar8 AS
SELECT
  items.ITEMBASEPEOORGANIZATIONID
FROM
  `amrl-data-prd.RAW.FUSION_EGP_SYSTEM_ITEMS_B` AS items
WHERE
  NOT EXISTS (
    SELECT 1
    FROM `amrl-data-prd.STAGING.FUSION_EGP_SYSTEM_ITEMS_B_PK` AS items_pk
    WHERE items.ITEMBASEPEOORGANIZATIONID = items_pk.ITEMBASEPEOORGANIZATIONID
  );

-- Step 2: Actualizar la columna DELETED en la tabla RAW.EGP_SYSTEM_ITEMS_B para los registros que están en la tabla temporal
UPDATE
  `amrl-data-prd.RAW.FUSION_EGP_SYSTEM_ITEMS_B`
SET
  DELETED = '1'
WHERE
  ITEMBASEPEOORGANIZATIONID IN (SELECT ITEMBASEPEOORGANIZATIONID FROM ids_a_actualizar8);

-- Step 3: Limpiar la tabla temporal
DROP TABLE ids_a_actualizar8;
END;
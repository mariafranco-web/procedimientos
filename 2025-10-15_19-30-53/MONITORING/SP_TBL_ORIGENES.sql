CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_ORIGENES()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de dimensión TBL_ORIGENES. Este procedimiento automatiza la sincronizacion de las tablas con el modulo de origen para cada aplicación.\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos, Análisis interno de analítica")
BEGIN

  CREATE OR REPLACE TABLE `amrl-data-prd`.MONITORING.TBL_ORIGENES
  --PARTITION BY 
  --CLUSTER BY
  OPTIONS (description = '''Tabla que permite identificar el aplicativo de origenes para las tablas almacenadas en las capas RAW% y STAGING, si no se tiene mapeado un orgigen retornara el valor de DESCONOCIDO''',
    labels = [('tipo', 'gobierno_datos')] 
  ) AS
  
  WITH ExtraerOrigen AS (
    SELECT
    tables.table_schema AS DATASET,
    REGEXP_EXTRACT(tables.table_name, r'^[^_]+') AS prefijo  -- Extrae Caracteres antes del primer underscore '_'

    FROM `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.TABLES AS tables
    WHERE
      (tables.table_schema LIKE 'RAW%' OR tables.table_schema LIKE 'STAGING%') 
  )

  SELECT DISTINCT
  SHA256(CONCAT(tables.table_schema,origen.prefijo)) AS SK_ORIGEN,
  tables.table_schema AS DATASET,
  origen.prefijo AS PREFIJO,    
    CASE
      WHEN origen.prefijo = 'FUSION' THEN 'ORACLE_FUSION'
      WHEN origen.prefijo = 'MAFP' THEN 'MAFP'
      WHEN origen.prefijo = 'INTERNAL' THEN 'INTERNAL'
      WHEN origen.prefijo = 'EXTERNAL' THEN 'EXTERNAL'
      WHEN origen.prefijo = 'TT' THEN 'TRACKING_TOOLS'
      --Añadir nuevos origenes
      ELSE 'DESCONOCIDO'
      END AS ORIGEN,
  CURRENT_TIMESTAMP() AS FECHA_EJECUCION
  
  
  FROM `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.TABLES AS tables
  
  INNER JOIN ExtraerOrigen origen ON tables.table_schema = origen.DATASET
  ORDER BY tables.table_schema
;
END;
CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_RELACION_TABLAS_MODULOS()
OPTIONS(
  description="Propósito: Crear o reemplazar la tabla `TBL_RELACION_TABLAS_MODULOS`, la cual establece la relación entre las tablas del ecosistema y su respectivo módulo y submódulo.  Esta asignación se realiza combinando el prefijo del nombre de tabla con palabras clave asociadas a cada submódulo, de acuerdo a la lógica definida en `MONITORING.TBL_MODULOS`.\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos\n  ")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_RELACION_TABLAS_MODULOS`
--PARTITION BY
--CLUSTER BY 
OPTIONS (
  description = '''
  Relación entre tablas del ecosistema de datos y sus módulos/submódulos funcionales.

  Esta tabla contiene una asignación funcional (MODULO y SUBMODULO) por cada tabla de los esquemas RAW y STAGING, usando como base:
  - El prefijo del nombre de la tabla (`CODIGO_MODULO`)
  - La coincidencia de palabras clave presentes en `TBL_MODULOS`.
 
  Esta tabla es generada por el procedimiento `SP_TBL_RELACION_TABLAS_MODULOS` y es clave para la organización lógica de los activos de datos.
  ''',
  labels = [('tipo', 'gobierno_datos')]
) AS


WITH DETALLE_ESQUEMAS AS (
  SELECT 
  tables.table_schema AS DATASET,
  tables.table_name AS TABLA,
  REGEXP_EXTRACT(tables.table_name, r'[^_]+_([^_]+)_') AS MODULO
  
FROM `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.TABLES AS tables  
),

MODULOS_EXPANDIDO AS (
  SELECT
    PREFIJO,
    MODULO,
    SUBMODULO,
    PALABRA_CLAVE,
  FROM `amrl-data-prd.MONITORING.TBL_MODULOS`,
  UNNEST(PALABRAS_CLAVE) AS PALABRA_CLAVE
),

MODULO_SUBMODULO AS (
  SELECT
    DE.*,
    ME.MODULO AS NOMBRE_MODULO,
    ME.SUBMODULO,
    ME.PALABRA_CLAVE AS PALABRA_COINCIDENTE
  FROM DETALLE_ESQUEMAS DE
  LEFT JOIN MODULOS_EXPANDIDO ME
    ON DE.MODULO = ME.PREFIJO
   AND LOWER(DE.TABLA) LIKE CONCAT('%', LOWER(ME.PALABRA_CLAVE), '%')
),

UNICOS_POR_TABLA AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY DATASET, TABLA
      ORDER BY LENGTH(PALABRA_COINCIDENTE) DESC, PALABRA_COINCIDENTE
    ) AS RN
  FROM MODULO_SUBMODULO
)

SELECT
  DATASET,
  TABLA,
  MODULO AS CODIGO_MODULO,
  COALESCE(NOMBRE_MODULO, 'DESCONOCIDO') AS MODULO,
  COALESCE(SUBMODULO, 'DESCONOCIDO') AS SUBMODULO,
  SHA256(CONCAT(DATASET, TABLA, COALESCE(MODULO,''))) AS SK_MODULO_SUBMODULO,
  CURRENT_TIMESTAMP() AS FECHA_EJECUCION,
FROM UNICOS_POR_TABLA
WHERE RN = 1
  AND (DATASET LIKE 'RAW%' OR DATASET LIKE 'STAGING%')
ORDER BY DATASET, TABLA;

END;
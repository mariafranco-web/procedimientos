CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_LINAJE_TABLAS()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de metadatos TBL_LINAJE_TABLAS. Este procedimiento automatiza la consolidación y extraccion de las tablas que componen un modelo de las capas de CONSUMPTION y PRESENTATION  Autor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos\n")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_LINAJE_TABLAS`
--PARTITION BY COLUMNA 1
--CLUSTER BY 
OPTIONS ( description = ''' Extraccion de tablas por modelo en BigQuery.\n
Este modelo analiza cada tabla del esquema, identifica la procedimeinto programado (SP) que actualiza o crea la tabla relacionada, analiza la consulta que compone la rutina, y extrae las tablas BASE del esquema que componen la tabla final ''',
   labels = [('tipo', 'gobierno_datos')] 
) AS 


WITH extraccion_tablas AS (
  SELECT
  rutina,
  ddl,
  sk_detalle_tabla,
  --Extrae todas las tablas que estas despues de las palabras reservadas FROM|JOIN
  REGEXP_EXTRACT_ALL(ddl, r'(?i)\b(?:FROM|JOIN)\s+([^\s,;()]+)') AS extraccion_tablas_ddl

  FROM `amrl-data-prd.MONITORING.TBL_DETALLE_RUTINAS`
),


filtrado_tablas AS 
(
SELECT 
rutina,
sk_detalle_tabla,
REGEXP_REPLACE(extraccion_tablas_ddl, r'[`"]', '') AS tabla_limpia

FROM extraccion_tablas,
UNNEST (extraccion_tablas_ddl) as extraccion_tablas_ddl
WHERE
--Filtra y elimina las tablas temporales y solo permite tablas de algun dataset de la forma proyecto.datset.tabla o dataset.tabla
REGEXP_CONTAINS( extraccion_tablas_ddl, r'^[`"]?[\w-]+[`"]?(?:\.[`"]?[\w-]+[`"]?){1,3};?$')
),

normalizacion_tablas AS 
(
SELECT
  rutina,
  sk_detalle_tabla,
  CASE 
  -- Si viene con 3 partes (proyecto.dataset.tabla), se deja de la forma DATASET.TABLA
    WHEN REGEXP_CONTAINS(tabla_limpia, r'^[^.]+\.[^.]+\.[^.]+$') 
    THEN REGEXP_EXTRACT(tabla_limpia, r'^[^.]+\.([^.]+\.[^.]+)$')
     -- Si viene con 2 partes (DATASET.TABLA), se deja igual
    ELSE tabla_limpia
  END AS tabla_normalizada,

FROM filtrado_tablas
)


SELECT DISTINCT
SHA256(RUTINA) AS BK_RUTINA,
sk_detalle_tabla AS BK_DETALLE_TABLA,
RUTINA,
TABLA_NORMALIZADA,
1 AS NIVEL
FROM normalizacion_tablas

WHERE
(REGEXP_EXTRACT(TABLA_NORMALIZADA, r'^([`"]?[\w-]+[`"]?)') IN  (SELECT schema_name AS DATASET FROM `amrl-data-prd`.INFORMATION_SCHEMA.SCHEMATA)
OR TABLA_NORMALIZADA LIKE '%INFORMATION_SCHEMA%')
;


END;
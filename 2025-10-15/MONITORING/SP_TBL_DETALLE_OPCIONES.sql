CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_DETALLE_OPCIONES()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de metadatos TBL_DETALLE_OPCIONES. Este procedimiento automatiza la consolidación de la información de`INFORMATION_SCHEMA` de BigQuery a nivel de proyecto/región para todas las tablas y vista\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos\n")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_DETALLE_OPCIONES`
--PARTITION BY COLUMNA 1
--CLUSTER BY DATASET, TABLA
OPTIONS ( description = ''' Tablas por esquema de BigQuery.
  Contiene metadatos a nivel de tabla para todos los datasets del proyecto y región, obtenidos de INFORMATION_SCHEMA.
  Contiene la descricpcion y los labels creados por cada tabla del esquema del proyecto amrl-data-prd
  Diseñado para la gestión y exploración de datos del equipo de analítica. ''',
  labels = [('tipo', 'gobierno_datos'),('estado', 'activo')] 
) AS 

WITH labels_parse AS (
  SELECT
    t.table_catalog AS project_id,
    t.table_schema AS dataset_id,
    t.table_name,
    REGEXP_EXTRACT(item, r'STRUCT\("([^"]+)"') AS key,
    REGEXP_EXTRACT(item, r',\s*"([^"]+)"\)') AS value
  FROM `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.TABLE_OPTIONS AS t
  CROSS JOIN UNNEST(REGEXP_EXTRACT_ALL(t.option_value, r'(STRUCT\("[^"]+",\s*"[^"]+"\))')) AS item
  WHERE t.option_name = 'labels'
),

descripcion_parse AS (
  SELECT
    t.table_catalog AS project_id,
    t.table_schema AS dataset_id,
    t.table_name,
    t.option_value AS descripcion
  FROM `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.TABLE_OPTIONS AS t
  WHERE t.option_name = 'description'
)

SELECT
  SHA256(CONCAT (t.table_schema, t.table_name)) AS SK_DETALLE_TABLA,
  t.table_schema AS DATASET,
  t.table_name AS TABLA,
  MAX(CASE WHEN lower(l.key) = 'tipo' THEN INITCAP(l.value) END) AS TIPOTABLA,
  MAX(CASE WHEN lower(l.key)= 'estado' THEN INITCAP(l.value) END) AS ESTADO,
  --Añadir nuevas etiquetas
  MAX(d.descripcion) AS DESCRIPCION,

  

FROM `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.TABLES AS t
LEFT JOIN labels_parse l ON  l.dataset_id = t.table_schema AND l.table_name = t.table_name
LEFT JOIN descripcion_parse d  ON l.dataset_id = d.dataset_id AND l.table_name = d.table_name
GROUP BY t.table_schema, t.table_name;

END;
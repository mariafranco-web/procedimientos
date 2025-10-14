CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_DETALLE_ESQUEMAS()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de metadatos TBL_DETALLE_ESQUEMAS. Este procedimiento automatiza la consolidación de la información   del `INFORMATION_SCHEMA` de BigQuery a nivel de proyecto/región para todas las tablas y vista\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos\n")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_DETALLE_ESQUEMAS`
--PARTITION BY COLUMNA 1
CLUSTER BY DATASET, TABLA
OPTIONS ( description = ''' Tablas por esquema de BigQuery.\n
  Contiene metadatos a nivel de tabla para todos los datasets del proyecto y región, obtenidos de INFORMATION_SCHEMA. 
  Incluye detalles sobre creación, almacenamiento (filas, estado, fecha), y configuración de particionamiento y clustering. 
  Diseñado para la gestión y exploración de datos del equipo de analítica. ''',
   labels = [('tipo', 'gobierno_datos')] 
) AS 

SELECT

  SHA256 ( CONCAT (tables.table_schema, tables.table_name)) AS SK_DETALLE_TABLA,
  SHA256 ( CONCAT (tables.table_schema,REGEXP_EXTRACT(tables.table_name, r'^[^_]+'))) AS BK_ORIGEN,  --DATASET, ORIGEN  (FUSION, MAFP) para tablas RAW
  SHA256 ( CONCAT (tables.table_schema, tables.table_name,COALESCE(REGEXP_EXTRACT(tables.table_name, r'[^_]+_([^_]+)_'),'') )) AS BK_MODULO_SUBMODULO, --DATASET, TABLA, _MODULO_


  tables.table_catalog AS PROYECTO, --PROYECTO
  tables.table_schema AS DATASET,   --DATASET
  tables.table_name AS TABLA,      --TABLA
  tables.table_type AS TIPO_TABLA,
  tables.is_insertable_into AS ES_INSERTABLE,
  tables.is_typed AS ES_TIPADA,
  storage.total_rows AS TOTAL_FILAS,
  columns.total_columns AS TOTAL_COLUMNAS,
  storage.almacenamiento AS ALMACENAMIENTO_TABLA,
  tables.creation_time AS FECHA_CREACION,        
  storage.last_modified AS FECHA_ULTIMA_MODIFICACION,  


  storage.update_status AS ACTUALIZADA,
  storage.storage_status AS ESTADO_ALMACENAMIENTO,

  columns.is_partitioned AS ES_PARTICIONADA,
  columns.partitioned_columns AS COLUMNAS_PARTICIONADAS,
  columns.cluster_1 AS CLUSTER_1,
  columns.cluster_2 AS CLUSTER_2,
  columns.cluster_3 AS CLUSTER_3,
  columns.cluster_4 AS CLUSTER_4,

  CURRENT_TIMESTAMP() AS FECHA_EJECUCION,

  --Extrae los caracteres entre el segundo y tercer underscore (_)
  REGEXP_EXTRACT(tables.table_name, r'[^_]+_([^_]+)_') AS CODIGO_MODULO,


  
FROM  `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.TABLES AS tables

  --Obtiene la información de almacenamiento de las tablas,estado almacenamiento y total de filas
  LEFT JOIN (
    SELECT
      table_schema,
      table_name,
      total_rows,
      storage_last_modified_time AS last_modified, 
      /* Define el estado de almacenamiento basado en la última modificación:
      'ACTIVE' si la tabla ha sido modificada en los últimos 90 días, de lo contrario 'LONG_TERM'.*/
      CASE WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), storage_last_modified_time, DAY) <= 90 THEN 'ACTIVE' ELSE 'LONG_TERM' END AS storage_status,
      /* Define el estado de actualización basado en la última modificación:
      'YES' si la tabla ha sido modificada y esta actualizada a la fecha actual, de lo contrario 'NO'.*/
      CASE WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), storage_last_modified_time, DAY) = 0 THEN 'YES' ELSE 'NO' END AS update_status,

      CASE WHEN total_logical_bytes <  1024.0*1024.0         THEN CONCAT(CAST(ROUND(total_logical_bytes/1024) AS STRING), ' ', 'KB')
           WHEN total_logical_bytes >= 1024.0*1024.0 
                AND total_logical_bytes <  1024.0*1024.0*1024.0  THEN CONCAT(CAST(ROUND((total_logical_bytes/1024)/1024) AS STRING), ' ', 'MB')
           ELSE CONCAT(CAST(ROUND(((total_logical_bytes/1024)/1024)/1024) AS STRING), ' ', 'GB')  END AS  ALMACENAMIENTO,

    FROM `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE 
  ) AS storage
    ON storage.table_name = tables.table_name
    AND storage.table_schema = tables.table_schema 

  --Obtiene la información de columnas de particición, ademas muestra columnas de clustering en posición cardinal
  LEFT JOIN (
    SELECT
      table_schema,
      table_name,
      CASE WHEN COUNTIF(is_partitioning_column = 'YES') > 0 THEN 'YES' ELSE 'NO' END AS is_partitioned,
      MAX(CASE WHEN is_partitioning_column = 'YES' THEN column_name END) AS partitioned_columns,
      --Uso de pivot con MAX(CASE) para extraer hasta 4 columnas de clustering en posición cardinal
      MAX(CASE WHEN clustering_ordinal_position = 1 THEN column_name END) AS cluster_1,
      MAX(CASE WHEN clustering_ordinal_position = 2 THEN column_name END) AS cluster_2,
      MAX(CASE WHEN clustering_ordinal_position = 3 THEN column_name END) AS cluster_3,
      MAX(CASE WHEN clustering_ordinal_position = 4 THEN column_name END) AS cluster_4,
      MAX (ordinal_position) AS total_columns
    FROM `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.COLUMNS
    GROUP BY table_schema, table_name
  ) AS columns
    ON columns.table_name = tables.table_name
    AND columns.table_schema = tables.table_schema
  ;    
--WHERE 
--tables.table_schema = 'RAW'

END;
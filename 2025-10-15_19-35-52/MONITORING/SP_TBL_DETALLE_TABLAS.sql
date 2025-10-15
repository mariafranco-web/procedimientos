CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_DETALLE_TABLAS()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de dimensión TABLE_DETALLE_TABLAS. Este procedimiento automatiza la extracción de metadatos detallados de columnas desde el `INFORMATION_SCHEMA` de BigQuery para todas las tablas del proyecto/región.\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos, Análisis interno de analítica")
BEGIN

  CREATE OR REPLACE TABLE `amrl-data-prd`.MONITORING.TBL_DETALLE_TABLAS
  --PARTITION BY 
  CLUSTER BY DATASET, TABLA, COLUMNA
  OPTIONS (
    description = '''Tabla que provee el detalle de las columnas para cada tabla dentro de los datasets. Incluye metadatos clave como tipo de dato,
    nulabilidad, posición ordinal, y descripciones de columna (si están disponibles), facilitando la exploración y el gobierno de datos a nivel de
    atributo.''',
    labels = [('tipo', 'gobierno_datos'),('dominio', 'metadatos_columna')] 
  ) AS
  SELECT
     SHA256 ( CONCAT (columns.table_schema, columns.table_name)) AS SK_DETALLE_TABLA,
    columns.table_catalog AS PROYECTO,
    columns.table_schema AS DATASET,          
    columns.table_name AS TABLA,             
    columns.column_name AS COLUMNA,            
    columns.ordinal_position AS POSICION_ORDINAL,       
    -- columns.is_nullable,          
    columns.data_type AS TIPO_DATO,       
    columns.is_hidden AS ESTA_OCULTA,              
    columns.is_system_defined AS DEFINIDA_POR_SISTEMA,      
    -- columns.is_partitioning_column, 
    -- columns.clustering_ordinal_position, 
    columns.column_default AS VALOR_POR_DEFECTO,        
    columns.rounding_mode AS MODO_REDONDEO,
    CURRENT_TIMESTAMP() AS FECHA_EJECUCION,

    
        
  FROM
    `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.COLUMNS AS columns
  WHERE
    1 = 1 
    --(columns.table_schema LIKE 'RAW%' OR columns.table_schema = 'STAGING') 
  /* 
  ORDER BY
    columns.table_catalog,
    columns.table_schema,
    columns.table_name,
    columns.ordinal_position;
  */
  ;
END;
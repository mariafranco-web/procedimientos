CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_DETALLE_RUTINAS()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de metadatos TBL_DETALLE_RUTINAS. Este procedimiento automatiza la consolidación de la información   del `INFORMATION_SCHEMA` de BigQuery a nivel de proyecto/región para todas las rutinas\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos\n")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_DETALLE_RUTINAS`
--PARTITION BY COLUMNA 1
CLUSTER BY DATASET, TABLA
OPTIONS ( description = ''' Tablas por esquema de BigQuery.
  Contiene metadatos de lsa rutinas o procedimientos almanecados SP_ para todos los datasets del proyecto y región, obtenidos de INFORMATION_SCHEMA. 
  Incluye detalles sobre creación, modificacion, dataet, tabla y sencia ddl. Diseñado para la gestión y exploración de datos del equipo de analítica. ''',
   labels = [('tipo', 'gobierno_datos'),('estado', 'en_construccion')] 
) AS 

WITH detail_routines AS (
SELECT
  routines.routine_catalog,
  routines.routine_schema,
  routines.routine_name,
  routines.routine_type,
  routines.data_type,
  routines.routine_body,
  routines.routine_definition,
  routines.created,
  routines.last_altered,
  routines.ddl,
  -- limpieza básica solo aquí
  REGEXP_REPLACE(routines.routine_name, r'^SP_', '') AS nombre_tabla_creada,
  --REGEXP_EXTRACT(routines.ddl, r'(?i)CREATE OR REPLACE TABLE\s+`?([^\s`]+)`?') AS tabla_creada_full,
    
  REGEXP_REPLACE(REGEXP_EXTRACT(routines.ddl, r'(?i)CREATE OR REPLACE TABLE\s+([^\s]+)'), r"[`']", '') AS create_or_replace,
  REGEXP_REPLACE(REGEXP_EXTRACT(routines.ddl, r'(?i)MERGE\s+([^\s]+)'), r"[`']", '') AS merge_into,
  REGEXP_REPLACE(REGEXP_EXTRACT(routines.ddl, r'(?i)INSERT INTO\s+([^\s]+)'), r"[`']", '') AS insert_into,


FROM `amrl-data-prd.region-us.INFORMATION_SCHEMA.ROUTINES`  routines

),

parse_dataset_tabla AS (
SELECT
d.*,
COALESCE(d.create_or_replace,d.merge_into,d.insert_into) AS nombre_completo_esquema,

CASE   -- Caso 1: NULL
  WHEN COALESCE(d.create_or_replace,d.merge_into,d.insert_into) IS NULL THEN NULL
  -- Caso 2: PROYECTO.DATASET.TABLA (con o sin backticks)
  WHEN REGEXP_CONTAINS(COALESCE(d.create_or_replace,d.merge_into,d.insert_into), r'^[`"]?\w[\w-]*[`"]?\.[`"]?\w+[`"]?\.[`"]?\w+[`"]?$') 
    THEN REGEXP_EXTRACT(COALESCE(d.create_or_replace,d.merge_into,d.insert_into), r'^[`"]?\w[\w-]*[`"]?\.\s*([^.]+)\.\w+[`"]?$')
  -- Caso 3: DATASET.TABLA (con o sin backticks)
  WHEN REGEXP_CONTAINS(COALESCE(d.create_or_replace,d.merge_into,d.insert_into), r'^[`"]?\w+[`"]?\.[`"]?\w+[`"]?$') 
    THEN REGEXP_EXTRACT(COALESCE(d.create_or_replace,d.merge_into,d.insert_into), r'^[`"]?([^.]+)[`"]?\.\w+[`"]?$')
  ELSE NULL
END AS dataset,

CASE 
  WHEN COALESCE(d.create_or_replace,d.merge_into,d.insert_into) IS NULL THEN NULL
    
  -- Caso con proyecto (con o sin backticks)
  WHEN REGEXP_CONTAINS(COALESCE(d.create_or_replace,d.merge_into,d.insert_into), r'^[`"]?\w[\w-]*[`"]?\.[`"]?\w+[`"]?\.[`"]?\w+[`"]?$') 
  THEN REGEXP_EXTRACT(COALESCE(d.create_or_replace,d.merge_into,d.insert_into), r'^[`"]?\w[\w-]*[`"]?\.[`"]?\w+[`"]?\.\s*[`"]?([^`"]+)[`"]?$')     
  -- Caso sin proyecto y sin backticks
  WHEN REGEXP_CONTAINS(COALESCE(d.create_or_replace,d.merge_into,d.insert_into), r'^\w+\.\w+$')
  THEN REGEXP_EXTRACT(COALESCE(d.create_or_replace,d.merge_into,d.insert_into), r'^\w+\.([^.]+)$')       
  ELSE NULL
END AS tabla
 FROM detail_routines d
)




SELECT

SHA256 ( CONCAT (pdt.dataset, pdt.tabla)) AS SK_DETALLE_TABLA,
 pdt.routine_catalog AS PROYECTO,
 pdt.routine_schema AS DATASET_RUTINA,
 pdt.routine_name AS RUTINA,
 pdt.routine_type AS TIPO,
 pdt.data_type AS TIPO_DATO_RUTINA,
 pdt.routine_body AS CUERPO_RUTINA,
 --pdt.routine_definition AS DEFINICION,
 pdt.created AS FECHA_DE_CREACION_RUTINA,
 pdt.last_altered AS FECHA_ULTIMA_MODIFICACION_RUTINA,
 pdt.ddl AS DDL,
 pdt.nombre_completo_esquema AS NOMBRE_COMPLETO_ESQUEMA,
 CASE 
  -- Si viene con 3 partes (proyecto.dataset.tabla), se deja de la forma DATASET.TABLA
    WHEN REGEXP_CONTAINS(nombre_completo_esquema, r'^[^.]+\.[^.]+\.[^.]+$') 
    THEN REGEXP_EXTRACT(nombre_completo_esquema, r'^[^.]+\.([^.]+\.[^.]+)$')
     -- Si viene con 2 partes (DATASET.TABLA), se deja igual
    ELSE nombre_completo_esquema
  END AS TABLA_NORMALIZADA,

 --pdt.nombre_tabla_creada AS NOMBRE_TABLA_CREADA,
 pdt.dataset AS DATASET,
 pdt.tabla AS TABLA

FROM parse_dataset_tabla AS pdt;

END;
CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_DESCRIPCION_COLUMNAS()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de metadatos TBL_DICCIONARIO_DATOS. Este procedimiento automatiza la consolidación de la información de`INFORMATION_SCHEMA` de BigQuery a nivel de proyecto/región para todas las tablas y vista\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos\n")
BEGIN


/*
CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_DESCRIPCION_COLUMNAS`
--PARTITION BY COLUMNA 1
--CLUSTER BY DATASET, TABLA
OPTIONS ( description = ''' Tabla que provee la descripcion de las columnas para los modelos disponibilizados en la capa de consumo''',
  labels = [('tipo', 'gobierno_datos'),('estado', 'en_construccion')] 
) AS 

SELECT
DATASET,
TABLA,
COLUMNA,
DESCRIPCION
FROM `MONITORING.TBl_TEMP_DESCRIPCION_COLUMNAS` 
;
*/

MERGE `amrl-data-prd.MONITORING.TBL_DESCRIPCION_COLUMNAS`  AS T1
USING
(
  SELECT
   * 
   FROM  `amrl-data-prd.MONITORING.TBL_CARGA_MANUAL_DESCRIPCIONES`

) AS T2

 ON T1.DATASET = T2.DATASET
 AND T1.TABLA = T2.TABLA
 AND T1.COLUMNA = T2.COLUMNA

 WHEN MATCHED THEN
UPDATE SET  
 T1.DATASET = T2.DATASET,
 T1.TABLA = T2.TABLA,
 T1.COLUMNA = T2.COLUMNA,
 T1.DESCRIPCION = T2.DESCRIPCION
 
 WHEN NOT MATCHED THEN
 INSERT (DATASET, TABLA, COLUMNA, DESCRIPCION)
 VALUES (T2.DATASET,T2.TABLA,T2.COLUMNA,T2.DESCRIPCION)

;










END;
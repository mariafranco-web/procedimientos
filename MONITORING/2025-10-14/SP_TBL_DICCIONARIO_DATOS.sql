CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_DICCIONARIO_DATOS()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de metadatos TBL_DICCIONARIO_DATOS. Este procedimiento automatiza la consolidación de la información de`INFORMATION_SCHEMA` de BigQuery a nivel de proyecto/región para todas las tablas y vista\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos\n")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_DICCIONARIO_DATOS`
--PARTITION BY COLUMNA 1
--CLUSTER BY DATASET, TABLA
OPTIONS ( description = ''' Tabla que provee el detalle y descripcion de las columnas para cada tabla dentro de los datasets. Incluye metadatos clave como tipo de dato,
    nulabilidad, posición ordinal, facilitando la exploración y el gobierno de datos a nivel de  atributos, tabla transversal para la constuccion del diccionario de datos, de lo smodelos disponibilizados en la capa de CONSUMPTION ''',
  labels = [('tipo', 'gobierno_datos'),('estado', 'en_construccion')] 
) AS 

SELECT
TDT.*,
TTDC.DESCRIPCION,
TO_HEX (TDT.SK_DETALLE_TABLA) AS SK_DETALLE_TABLA_STRING

FROM `MONITORING.TBL_DETALLE_TABLAS` TDT
LEFT JOIN `MONITORING.TBl_TEMP_DESCRIPCION_COLUMNAS` TTDC ON  TDT.DATASET = TTDC.DATASET AND TDT.TABLA = TTDC.TABLA AND TDT.COLUMNA = TTDC.COLUMNA
WHERE TDT.DATASET = 'CONSUMPTION'
;
  



END;
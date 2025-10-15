CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_AUDITLOG_DETALLE_MODIFICACIONES()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de auditoria de actualizacion de registros. Este procedimiento automatiza la consolidación de la información de la modificación y/o actualizacion de tablas en el proyecto amrl-data-prd, ob\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos\n")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_AUDITLOG_DETALLE_MODIFICACIONES`
--PARTITION BY COLUMNA 1
--CLUSTER BY COLIMNA 1
OPTIONS ( description = ''' Tablas por esquema de BigQuery.  Contiene la ultima fecha de modificación y el usuario que ejecuto la accion. ''',
   labels = [('tipo', 'gobierno_datos')] 
) AS 


WITH update_base AS (
SELECT
timestamp ,
protopayload_auditlog.authenticationInfo. principalEmail AS USUARIO,
protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration.query. statementType AS SENTENCIA,
COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration.query.destinationTable. datasetId
,REGEXP_EXTRACT(b.resource, r'^[^/]+/[^/]+/[^/]+/([^/]+)') ) AS DATASET,
COALESCE(REGEXP_REPLACE(protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration.query.destinationTable. tableId, r'_stg$', '')
,REGEXP_REPLACE(REGEXP_EXTRACT(b.resource, r'[^/]+$'), r'_stg$', '') ) AS TABLA,
b.resource,
b.permission

 FROM `amrl-data-prd.MONITORING.cloudaudit_googleapis_com_data_access`,
 UNNEST (protopayload_auditlog.authorizationInfo) b


WHERE 
(protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration.query. statementType NOT IN ('SELECT')
  OR protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration.query. statementType IS NULL)
AND b.permission IN ('bigquery.tables.update','bigquery.tables.updateData')
--AND protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration.query.destinationTable. datasetId = 'RAW'

ORDER BY timestamp DESC
),

--eliminar duplicados
update_deduplicado AS (
  SELECT AS VALUE
    ARRAY_AGG(ub ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)]
  FROM update_base ub
  GROUP BY DATASET, TABLA
)

SELECT 
  SHA256 ( CONCAT (tables.table_schema, tables.table_name)) AS SK_DETALLE_TABLA,
  tables.table_schema AS DATASET,
  tables.table_name AS TABLA,
  tables.creation_time AS FECHA_CREACION,
  udd.timestamp AS FECHA_ULTIMA_MODIFICACION,
  --TIMESTAMP_SUB(tsi.timestamp, INTERVAL 5 HOUR) AS FECHA_ULTIMA_MODIFICACION, HORA REAL
  COALESCE(udd.USUARIO,' ') AS ACTUALIZADO_POR,  
  CURRENT_TIMESTAMP() AS FECHA_EJECUCION

FROM `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.TABLES AS tables

LEFT JOIN update_deduplicado udd
  ON tables.table_schema = udd.DATASET AND tables.table_name = udd.TABLA
 ;
END;
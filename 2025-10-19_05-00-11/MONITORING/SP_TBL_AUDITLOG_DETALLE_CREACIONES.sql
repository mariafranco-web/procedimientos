CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_AUDITLOG_DETALLE_CREACIONES()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de auditoria de creacion de registros. Este procedimiento automatiza la consolidación de la información de la creacion de tablas en el proyecto amrl-data-prd, ob\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos\n")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_AUDITLOG_DETALLE_CREACIONES`
--PARTITION BY COLUMNA 1
--CLUSTER BY COLIMNA 1
OPTIONS ( description = ''' Tablas por esquema de BigQuery.\n
  Contiene la fecha de creacion y el usaurio que ejcuto la accion. ''',
   labels = [('tipo', 'gobierno_datos')] 
) AS 

--CREACION DE TABLAS POR JOBINSERT
WITH insertjob_base AS (
  SELECT
    timestamp,
    protopayload_auditlog.authenticationInfo.principalEmail AS USUARIO,
    resource.labels.dataset_id AS DATASET,
    REGEXP_REPLACE(REGEXP_EXTRACT(protopayload_auditlog.resourceName, r'[^/]+$'), r'_stg$', '') AS TABLA,
    protopayload_auditlog.methodName AS METODO
  FROM `amrl-data-prd.MONITORING.cloudaudit_googleapis_com_activity`
  WHERE
    protopayload_auditlog.methodName = 'google.cloud.bigquery.v2.JobService.InsertJob'
    AND protopayload_auditlog.authorizationInfo[SAFE_OFFSET(0)].permission IN ('bigquery.tables.updateData', 'bigquery.tables.create')
),

--eliminar duplicados
insertjob_deduplicado AS (
  SELECT AS VALUE
    ARRAY_AGG(ijb ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)]
  FROM insertjob_base ijb
  GROUP BY DATASET, TABLA
),

--CREACION DE TABLAS POR TALESERVICE.INSERT
tableservice_base AS (
  SELECT
    timestamp,
    protopayload_auditlog.authenticationInfo.principalEmail AS USUARIO,
    protopayload_auditlog.methodName AS METODO,
    protopayload_auditlog.servicedata_v1_bigquery.tableInsertRequest.resource.tableName.datasetId AS DATASET,
    REGEXP_REPLACE(protopayload_auditlog.servicedata_v1_bigquery.tableInsertRequest.resource.tableName.tableId, r'_stg$', '') AS TABLA
  FROM `amrl-data-prd.MONITORING.cloudaudit_googleapis_com_activity`
  WHERE protopayload_auditlog.methodName = 'tableservice.insert'
),

--eliminar duplicados
tableservice_deduplicado AS (
  SELECT AS VALUE
    ARRAY_AGG(t ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)]
  FROM tableservice_base t
  GROUP BY DATASET, TABLA
),


createdataaccess_base AS (
SELECT
timestamp,
protopayload_auditlog.authenticationInfo. principalEmail AS USUARIO,
protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration.query. statementType AS SENTENCIA,
protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration.query.destinationTable. datasetId AS DATASET,
protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration.query.destinationTable. tableId AS TABLA

 FROM `amrl-data-prd.MONITORING.cloudaudit_googleapis_com_data_access` 
 WHERE 
protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration.query. statementType IN ('CREATE_TABLE_AS_SELECT')
),

--eliminar duplicados
createdataaccess_deduplicado AS (
  SELECT AS VALUE
    ARRAY_AGG(c ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)]
  FROM createdataaccess_base c
  GROUP BY DATASET, TABLA
)

--CONSULTA PRINCIPAL
SELECT
  SHA256 ( CONCAT (tables.table_schema, tables.table_name)) AS SK_DETALLE_TABLA,
  tables.table_schema AS DATASET,
  tables.table_name AS TABLA,
  tables.creation_time AS FECHA_CREACION,
  COALESCE(tsi.USUARIO, ij.USUARIO , cda.USUARIO) AS CREADO_POR,
  
  CURRENT_TIMESTAMP() AS FECHA_EJECUCION

FROM `amrl-data-prd`.`region-us`.INFORMATION_SCHEMA.TABLES AS tables
LEFT JOIN tableservice_deduplicado tsi 
  ON tables.table_schema = tsi.DATASET AND tables.table_name = tsi.TABLA
LEFT JOIN insertjob_deduplicado ij 
  ON tables.table_schema = ij.DATASET AND tables.table_name = ij.TABLA
LEFT JOIN createdataaccess_deduplicado cda ON tables.table_schema = cda.DATASET AND tables.table_name = cda.TABLA


ORDER BY FECHA_CREACION DESC;

END;
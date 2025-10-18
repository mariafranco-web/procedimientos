CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_DETALLE_FACTURACION()
OPTIONS(
  description="Propósito: Crear y Actualizar la tabla de detalle de facturacion y consumo de BigQuery en el proyecto amrl-data-prd\n\nAutor: Anderson Murte\n\nUsos: Dashboard Diccionario de Datos\n")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_DETALLE_FACTURACION`
PARTITION BY DateJobStartTime
CLUSTER BY Dataset, UserId, TableId
 OPTIONS(
   description="Detalle costo y uso de bigquery para el proyecto amrl-data-prd",
   require_partition_filter=false
 ) AS

(SELECT
timestamp AS Date,
protopayload_auditlog.resourceLocation.currentLocations  AS currentLocations,
protopayload_auditlog.resourceLocation.originalLocations AS originalLocations,
resource.labels.project_id AS ProjectId,
protopayload_auditlog.serviceName AS ServiceName,
protopayload_auditlog.methodName AS MethodName,
protopayload_auditlog.status.code AS StatusCode,
protopayload_auditlog.status.message AS StatusMessage,
protopayload_auditlog.authenticationInfo.principalEmail AS UserId,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId AS JobId,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query AS Query,

ROUND(TIMESTAMP_DIFF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime, protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime, MILLISECOND)/1000,2) AS ExecutionSeconds,
    
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.projectId AS DestinationTableProjectId,

--protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.datasetId AS DestinationTableDatasetId,
CASE 
WHEN STARTS_WITH(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.datasetId, '_') THEN 'TEMPORAL'
WHEN protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.datasetId IS NULL THEN 'TEMPORAL'
ELSE protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.datasetId END AS  Dataset,

--protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId AS DestinationTableId,
CASE 
WHEN STARTS_WITH(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId, '_') THEN 'TEMPORAL'
WHEN STARTS_WITH(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId, 'anon') THEN 'TEMPORAL'
WHEN protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId IS NULL THEN 'TEMPORAL'
ELSE protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId END AS TableId,

protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.createDisposition AS CreateDisposition,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.writeDisposition AS WriteDisposition,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.dryRun AS DryRun,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.state AS JobState,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.code AS JobErrorCode,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.message AS JobErrorMessage,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.createTime AS JobCreateTime,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime AS JobStartTime,
DATE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime) AS DateJobStartTime,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime AS JobEndTime,
DATE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime) AS DateJobEndTime,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.billingTier AS BillingTier,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes AS TotalBilledBytes,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes AS TotalProcessedBytes,
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes / 1000000000 AS TotalBilledGigabytes,
(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes / 1000000000) / 1000 AS TotalBilledTerabytes,
((protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes / 1000000000) / 1000) * 6.25 AS TotalCost,
1 AS Queries,

CASE
WHEN protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes IS NULL THEN 0
ELSE 1 END AS BilledQueries,

CASE
WHEN protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes IS NULL THEN 1
ELSE 0 END AS UnBilledQueries,

CASE 
  WHEN protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes IS NULL THEN 'NO_COST'
  ELSE 'COST'
END AS QueryType,

CURRENT_TIMESTAMP() AS FECHA_EJECUCION,

FROM `amrl-data-prd.MONITORING.cloudaudit_googleapis_com_data_access`
WHERE protopayload_auditlog.serviceName = 'bigquery.googleapis.com'
AND protopayload_auditlog.methodName = 'jobservice.jobcompleted'
AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName = 'query_job_completed'
)
;

END;
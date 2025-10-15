CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_TABLES_DETAILS()
BEGIN  
        CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_TABLES_DETAILS` AS

        SELECT  p.table_catalog                       AS  PROYECTO,
                p.table_schema                        AS  DATASET,
                p.table_name                          AS  TABLA,
                p.total_rows                          AS  TOTAL_REGISTROS,
                p.total_logical_bytes                 AS  ALMACENAMIENTO_BYTES,
                CASE WHEN p.total_logical_bytes <  1024.0*1024.0         THEN CONCAT(CAST(ROUND(p.total_logical_bytes/1024) AS STRING), ' ', 'KB')
                     WHEN p.total_logical_bytes >= 1024.0*1024.0 
                      AND p.total_logical_bytes <  1024.0*1024.0*1024.0  THEN CONCAT(CAST(ROUND((p.total_logical_bytes/1024)/1024) AS STRING), ' ', 'MB')
                                                                         ELSE CONCAT(CAST(ROUND(((p.total_logical_bytes/1024)/1024)/1024) AS STRING), ' ', 'GB') 
                                                  END AS  ALMACENAMIENTO,
                p.last_modified_time                  AS  HORA_ULTIMA_MODIFICACION,
                p.storage_tier                        AS  STATUS_TABLA
        FROM    RAW.INFORMATION_SCHEMA.PARTITIONS AS p
        WHERE   2 = 2
        ORDER BY p.total_logical_bytes DESC;
END;
CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_DIM_TIEMPO()
BEGIN
    CREATE OR REPLACE TABLE `amrl-data-prd.PRESENTATION.DIM_TIEMPO` AS
    SELECT  CAST(FORMAT_DATE('%Y%m%d',FECHA) AS INT64)                                                    AS  SK_FECHA,
            FECHA                                                                                         AS  FECHA,
            EXTRACT(YEAR FROM FECHA)                                                                      AS  ANIO, 
            EXTRACT(QUARTER FROM FECHA)                                                                   AS  NUM_TRIMESTRE, 
            EXTRACT(MONTH FROM FECHA)                                                                     AS  NUM_MES, 
            EXTRACT(WEEK FROM FECHA)                                                                      AS  NUM_SEMANA, 
            EXTRACT(DAY FROM FECHA)                                                                       AS  NUM_DIA,
            EXTRACT(DAYOFWEEK  FROM FECHA)                                                                AS  NUM_DIA_SEMANA,
            UPPER(CONCAT('T',EXTRACT(QUARTER FROM FECHA),'/',RIGHT(CAST(EXTRACT(YEAR FROM FECHA) AS STRING),2))) AS  NOM_TRIMESTRE,
            UPPER(FORMAT_DATE("%B", FECHA))                                                                      AS  NOM_MES_INGLES,
            UPPER(CASE EXTRACT(MONTH FROM FECHA)
              WHEN 1 THEN  'Enero'
              WHEN 2 THEN  'Febrero' 
              WHEN 3 THEN  'Marzo' 
              WHEN 4 THEN  'Abril' 
              WHEN 5 THEN  'Mayo' 
              WHEN 6 THEN  'Junio' 
              WHEN 7 THEN  'Julio' 
              WHEN 8 THEN  'Agosto' 
              WHEN 9 THEN  'Septiembre' 
              WHEN 10 THEN  'Octubre'  
              WHEN 11 THEN  'Noviembre'  
              WHEN 12 THEN  'Diciembre'  
            END)                                                                                           AS  NOM_MES_ESP,
            UPPER(FORMAT_DATE('%A', FECHA))                                                                AS  NOM_DIA_SEMANA_ING,
            UPPER(CASE EXTRACT(DAYOFWEEK  FROM FECHA)
              WHEN 1 THEN  'Domingo'
              WHEN 2 THEN  'Lunes' 
              WHEN 3 THEN  'Martes' 
              WHEN 4 THEN  'Miércoles' 
              WHEN 5 THEN  'Jueves' 
              WHEN 6 THEN  'Viernes' 
              WHEN 7 THEN  'Sábado' 
            END)                                                                                           AS  NOM_DIA_SEMANA_ESP,
            CONCAT('FY',RIGHT(CAST(EXTRACT(YEAR FROM FECHA) AS STRING),2))                                 AS  FISCAL_YEAR_T1,
            CONCAT('P',EXTRACT(MONTH FROM FECHA))                                                          AS  FISCAL_PERIOD_T1,
            CONCAT('Q',EXTRACT(QUARTER FROM FECHA))                                                        AS  FISCAL_QUARTER_T1,
            'INTERNAL'                                                                                     AS  FUENTE,
            CURRENT_DATETIME("America/Bogota")                                                             AS  FECHA_ACTUALIZACION_BQ,
            CURRENT_DATETIME("America/Bogota")                                                             AS  FECHA_CARGUE_BQ
            --CONCAT(FORMAT_DATE("%Y", FECHA), IF(EXTRACT(QUARTER FROM FECHA) < 3, '-S1', '-S2')) AS semester
    FROM UNNEST(GENERATE_DATE_ARRAY('2000-01-01', '2080-12-31', INTERVAL 1 DAY)) FECHA;
END;
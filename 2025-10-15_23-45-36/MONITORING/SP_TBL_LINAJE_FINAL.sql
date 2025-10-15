CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_LINAJE_FINAL()
BEGIN
  -- 1) Armar el array de rutinas a procesar (dinámico desde la tabla)
  DECLARE rutina_list ARRAY<STRING>;
  SET rutina_list = (
    SELECT ARRAY_AGG(DISTINCT RUTINA)
    FROM `amrl-data-prd.MONITORING.TBL_LINAJE_TABLAS`
  );

  -- 2) Crear/limpiar la tabla destino (3 columnas) con 0 filas
  CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_LINAJE_FINAL` AS
  SELECT
    CAST(NULL AS BYTES) AS BK_RUTINA,
    CAST(NULL AS BYTES) AS BK_DETALLE_TABLA_RAW,
    CAST(NULL AS STRING) AS RUTINA,
    CAST(NULL AS STRING) AS TABLA_NORMALIZADA,
    CAST(NULL AS STRING) AS DATASET,
    CAST(NULL AS STRING) AS TABLA_RAW
  FROM UNNEST([]) AS _;

  -- 3) Iterar cada rutina y ejecutar la CTE recursiva
  FOR r IN (SELECT rutina FROM UNNEST(rutina_list) AS rutina) DO
    EXECUTE IMMEDIATE """
      INSERT INTO `amrl-data-prd.MONITORING.TBL_LINAJE_FINAL`
      WITH RECURSIVE linaje AS (
  -- Caso base
  SELECT
    l.RUTINA,
    l.TABLA_NORMALIZADA,
    1 AS NIVEL,
    [l.TABLA_NORMALIZADA] AS VISITADOS
  FROM `MONITORING.TBL_LINAJE_TABLAS` l
  WHERE l.RUTINA = @rutina


  UNION ALL

  -- Paso recursivo
  SELECT
    lin.RUTINA,
    h.TABLA_NORMALIZADA,
    lin.NIVEL + 1,
    ARRAY_CONCAT(lin.VISITADOS, [h.TABLA_NORMALIZADA])
  FROM linaje lin
  JOIN `MONITORING.TBL_DETALLE_RUTINAS` r
    ON lin.TABLA_NORMALIZADA = r.TABLA_NORMALIZADA
  JOIN `MONITORING.TBL_LINAJE_TABLAS` h
    ON r.RUTINA = h.RUTINA
  -- cortar si ya fue visitada
  WHERE NOT h.TABLA_NORMALIZADA IN UNNEST(lin.VISITADOS)
),

clasificacion_linaje AS (
     SELECT DISTINCT
       RUTINA,
       TABLA_NORMALIZADA,
       NIVEL,
       CASE 
         WHEN TABLA_NORMALIZADA LIKE 'RAW%' THEN 'FINAL'
         WHEN NIVEL = 1 THEN 'INMEDIATO'
         ELSE 'RECURSIVO'
       END AS TIPO_LINAJE
     FROM linaje
     )

SELECT DISTINCT
        SHA256(RUTINA) AS BK_RUTINA,
        SHA256 ( CONCAT (REGEXP_EXTRACT(TABLA_NORMALIZADA, r'^(.*?)\\.'), REGEXP_EXTRACT(TABLA_NORMALIZADA, r'\\.(.*)'))) AS BK_DETALLE_TABLA,
        RUTINA,
        TABLA_NORMALIZADA,
        REGEXP_EXTRACT(TABLA_NORMALIZADA, r'^(.*?)\\.') AS DATASET,
        REGEXP_EXTRACT(TABLA_NORMALIZADA, r'\\.(.*)') AS TABLA_RAW


      FROM clasificacion_linaje
      WHERE TIPO_LINAJE = 'FINAL'
    """ USING r.rutina AS rutina;
  END FOR;
END;
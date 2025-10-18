CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_OBT_TRACKING_ESCRITURACION()
OPTIONS(
  description="Propósito: Crear y actualizar la tabla OBT_FACT_TRACKING_ESCRITURACION. Este SP unifica en una tabla maestra los proceso gestionados por los equipos de escrituración (legalización, ordendes, escrituración, creditos, subsidios y entregas) en la herramienta TRACKING TOOL")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.CONSUMPTION.OBT_FACT_TRACKING_ESCRITURACION` PARTITION BY FESC CLUSTER BY OP AS

        SELECT  *
        FROM `PRESENTATION.FACT_TRACKING_ESCRITURACION`;
       
END;
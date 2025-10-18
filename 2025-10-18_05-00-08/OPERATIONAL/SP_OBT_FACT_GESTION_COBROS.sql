CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_OBT_FACT_GESTION_COBROS()
OPTIONS(
  description="Propósito: Crear y actualizar la tabla SP_OBT_DIM_COBRO consolidando todos los atributos de los cobros asociados a un concepto del plan de pagos\nAutor: Maria Fernanda Franco")
BEGIN
    CREATE OR REPLACE TABLE `amrl-data-prd.CONSUMPTION.OBT_FACT_GESTION_COBROS`  PARTITION BY FECHA CLUSTER BY NUMERO_DE_COBRO AS
      SELECT  
            dc.SK_COBRO
            ,dc.NUMERO_DE_COBRO
            ,dc.RECIBO
            ,dc.ESTADO_DEL_COBRO
            ,dc.IMPORTE_APLICADO
            ,dc.METODO_DE_COBRO
            ,dc.REFERENCIA_DE_APLICACION
            ,dc.NUMERO_DE_DOCUMENTO
            ,dc.TIPO_DE_COBRO
            ,dc.DIAS_DE_MORA
            ,dc.FECHA_DE_COBRO
            ,dc.FECHA_CONTABLE
            ,dc.FECHA_DE_APLICACION
            ,dc.NUMERO_DE_CUENTA_DEL_CLIENTE
            ,dc.NUMERO_DE_SITIO
            ,dc.BK_SITIO_CLIENTE
            ,dc.BK_PERSONA
            ,dc.FECHA_ACTUALIZACION_BQ   AS FECHA_ACTUALIZACION_BQ_DIM_COBRO   
            ,dc.FECHA_CARGUE_BQ          AS FECHA_CARGUE_BQ_DIM_COBRO            
            ,dt.*
      FROM `PRESENTATION.DIM_COBRO` dc
      LEFT JOIN `PRESENTATION.DIM_TIEMPO` dt ON dc.FECHA_DE_COBRO = dt.FECHA;     
    
END;
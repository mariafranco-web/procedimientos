CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_OBT_DIM_INVENTARIO()
OPTIONS(
  description="Propósito:Crear y actualizar la tabla OBT_DIM_INVENTARIO en el DataSet de COMSUPTION, La tabla consolida los atributos revelantes de los items vendidos y disponibles incluye articulos de CDA. \nAutor: Maria Fernanda Franco\nUsos: Tablero de Indicadores, Reporte Inmuebles Terminados (Control de Calidad)\nModificaciones: 2025-01-28: Se modifica la tabla origen del campo fop.BK_NUMERO_OP por din.BK_NUMERO_OP y se retira de la clausula WHERE la condición REFERENCIA <> 'CDA'\n2025-07-18: Se incluye la columna TIPO_DE_ARTICULO")
BEGIN
    CREATE OR REPLACE TABLE `amrl-data-prd.CONSUMPTION.OBT_DIM_INVENTARIO` PARTITION BY
        DATE(FECHA_ACTUALIZACION_BQ)   CLUSTER BY BK_ARTICULO AS

SELECT  ROW_NUMBER() OVER (ORDER BY din.SK_ARTICULO) AS SK_OBT_DIM_INVENTARIO
       ,din.SK_ARTICULO -- ID UNICO EN LA TABLA EFF_LINE INVENTARIO
       ,din.BK_ARTICULO --INVENTORY ITEM ID 
       ,din.NOMBRE_ARTICULO 
       ,din.CATEGORIA
       ,din.TIPO_DE_ARTICULO
       ,din.CLASE_DE_ARTICULO
       ,din.REFERENCIA
       ,din.AREA_CONSTRUIDA
       ,din.AREA_COMUN       
       ,din.PRECIO_UNITARIO
       ,din.ESTADO_DEL_ARTICULO
       ,din.FASE_CICLO_DE_VIDA
       ,din.MATRICULA_INMOBILIARIA
       ,din.ENCARGO_FIDUCIARIO
       ,din.FECHA_PROYECTADA_ESCRITURACION
       ,din.FECHA_PROYECTADA_CTO
       ,din.FECHA_REAL_CTO       
       ,din.FECHA_REAL_POLIZA_DECENAL
      -- ,din.FECHA_ENTREGA_ESTIMADA --campo temporal mientras se libera el reporte de inmuebles cerrados y habilitados  
      -- ,din.HABILITADO_PARA_ENTREGA --campo temporal mientras se libera el reporte de inmuebles cerrados y habilitados 
       ,din.BK_OP
       ,din.BK_PRECIO
       ,din.BK_COD_TRANSACCIONAL
       ,dse.TRANSACCIONAL
       ,dse.ESTRATO
       ,dmc.SK_COD_MACROCONSOLIDADOR
       ,dmc.MACROCONSOLIDADOR
       ,dmp.SK_COD_MACROPROYECTO
       ,dmp.MACROPROYECTO       
       ,din.FECHA_ACTUALIZACION_PRECIO
       ,din.FECHA_ACTUALIZACION_BQ
       ,din.FECHA_CARGUE_BQ
FROM `PRESENTATION.DIM_INMUEBLES` AS din
LEFT JOIN `PRESENTATION.DIM_SUB_ETAPA` dse ON din.BK_COD_TRANSACCIONAL = dse.SK_COD_TRANSACCIONAL
LEFT JOIN `PRESENTATION.DIM_MACROCONSOLIDADOR` dmc ON dse.BK_COD_MACROCONSOLIDADOR = dmc.SK_COD_MACROCONSOLIDADOR 
LEFT JOIN `PRESENTATION.DIM_MACROPROYECTO` dmp ON dse.BK_COD_MACROPROYECTO = dmp.SK_COD_MACROPROYECTO;
--WHERE REFERENCIA <> 'CDA';


END;
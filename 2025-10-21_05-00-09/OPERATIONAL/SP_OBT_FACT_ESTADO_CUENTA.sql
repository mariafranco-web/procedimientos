CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_OBT_FACT_ESTADO_CUENTA()
OPTIONS(
  description="Propósito: Crear y actualizar la tabla OBT_FACT_ESTADO_CUENTA` en el DataSet de CONMSUPTION, La tabla muestra una fila por cada obligacion asociada a una oportunidad, se incluyen los datos basicos del comprador principal y secundario \nModificaciones:\n2025-08-15: se incluye columna TIPO_DE_TRANSACCION\n2025-08-21: se incluye columna ORIGEN_DE_TRANSACCION\n2025-09-01: se incluye columna FECHA_DE_CREACION_TRANSACCION y TRANSACCION_CREADA_POR")
BEGIN
    CREATE OR REPLACE TABLE `amrl-data-prd.CONSUMPTION.OBT_FACT_ESTADO_CUENTA` PARTITION BY DATE(FECHA_ACTUALIZACION_PLAN_DE_PAGOS) CLUSTER BY BK_NUMERO_OP , ID_CONCEPTO  AS
       
           SELECT  ROW_NUMBER() OVER (ORDER BY fae.BK_NUMERO_OP) AS SK_OBT_FACT_ESTADO_CUENTA,
              fae.BK_NUMERO_OP                                   AS BK_NUMERO_OP,
              fae.ID_CONCEPTO, 
              fae.CONCEPTO ,
              fae.ORIGEN_DE_TRANSACCION,
              fae.TIPO_DE_TRANSACCION,
              fae.FECHA_DE_CREACION_TRANSACCION,
              fae.TRANSACCION_CREADA_POR,
              fae.NUMERO_DE_TRANSACCION,
              fae.VALOR_PACTADO,
              fae.PAGO_REALIZADO,
              fae.FECHA_LIMITE_DE_PAGO,
              fae.VALOR_EN_MORA,
              fae.ESTADO_DE_TRANSACCION,
              fae.CANTIDAD_DIAS_EN_MORA,
              fae.ESTADO_CARTERA,
              fae.MOTIVO_DE_INCUMPLIMIENTO_DE_PAGO,
              fae.NOTAS_INCUMPLIMIENTO_DE_PAGO,
              fae.FECHA_NOTA_INCUMPLIMIENTO_DE_PAGO,
              fae.ENTIDAD_FINANCIERA,
              fo.BK_PERSONA                            AS BK_COMPRADOR1,             
              dm.NOMBRE_COMPLETO                        AS NOMBRE_COMPRADOR1,
              dm.TIPO_IDENTIFICACION                    AS TIPO_IDENTIFICACION_COMPRADOR1,
              dm.NUMERO_DE_IDENTIFICACION               AS NUMERO_IDENTIFICACION_COMPRADOR1,
              dm.EMAIL                                  AS EMAIL_COMPRADOR1,
              dm.TELEFONO                               AS TELEFONO_COMPRADOR1,
              dm.AUTORIZA_CORREOS_ELECTRONICOS          AS AUTORIZA_CORREOS_ELECTRONICOS_COMPRADOR1,
              dm.AUTORIZA_MENSAJES_DE_TEXTO             AS AUTORIZA_MENSAJES_DE_TEXTO_COMPRADOR1,
              -- Los CASE permiten diferenciar si hay un COMPRADOR #2 o es una persona de contacto referenciada en la OP
              CASE WHEN fo.PORCENTAJE_COMPRADOR1 < 100 THEN dm2.BK_PERSONA ELSE NULL END BK_COMPRADOR2,
              CASE WHEN fo.PORCENTAJE_COMPRADOR1 < 100 THEN UPPER(dm2.NOMBRE_COMPLETO) ELSE NULL END NOMBRE_COMPRADOR2,
              CASE WHEN fo.PORCENTAJE_COMPRADOR1 < 100 THEN dm2.TIPO_IDENTIFICACION ELSE NULL END AS TIPO_IDENTIFICACION_COMPRADOR2,
              CASE WHEN fo.PORCENTAJE_COMPRADOR1 < 100 THEN dm2.NUMERO_DE_IDENTIFICACION ELSE NULL END AS NUMERO_IDENTIFICACION_COMPRADOR2,
              CASE WHEN fo.PORCENTAJE_COMPRADOR1 < 100 THEN dm2.EMAIL ELSE NULL END AS EMAIL_COMPRADOR2,
              CASE WHEN fo.PORCENTAJE_COMPRADOR1 < 100 THEN dm2.TELEFONO ELSE NULL END AS TELEFONO_COMPRADOR2,
              CASE WHEN fo.PORCENTAJE_COMPRADOR1 < 100 THEN dm2.AUTORIZA_CORREOS_ELECTRONICOS ELSE NULL END AS AUTORIZA_CORREOS_ELECTRONICOS_COMPRADOR2,              
              CASE WHEN fo.PORCENTAJE_COMPRADOR1 < 100 THEN dm2.AUTORIZA_MENSAJES_DE_TEXTO   ELSE NULL END AS AUTORIZA_MENSAJES_DE_TEXTO_COMPRADOR2,
              fae.FECHA_ACTUALIZACION_PLAN_DE_PAGOS,
              fae.BK_COBRO,
              fae.BK_NUMERO_DE_CUENTA,
              fae.BK_SITIO_CLIENTE,
              fae.NUMERO_DE_SITIO,
              fae.FECHA_ACTUALIZACION_BQ,
              fae.FECHA_CARGUE_BQ
      FROM `PRESENTATION.FACT_ESTADO_CUENTA` fae
      LEFT JOIN (-- Obtiene el porcentaje de participacion en la venta, para identificar si la persona 2 es un contacto o un comprador
                  SELECT DISTINCT BK_NUMERO_OP,
                                  PORCENTAJE_COMPRADOR1
                                  ,BK_PERSONA
                  FROM `PRESENTATION.FACT_OPORTUNIDAD`
                ) fo ON  fae.BK_NUMERO_OP = fo.BK_NUMERO_OP
      LEFT JOIN `PRESENTATION.DIM_PERSONA` dm ON fo.BK_PERSONA = dm.BK_PERSONA
      LEFT JOIN `PRESENTATION.DIM_PERSONA` dm2 ON dm.BK_COMPRADOR2 = dm2.BK_PERSONA;
      -- WHERE fae.BK_PERSONA = 300000352132082
      --WHERE fae.BK_NUMERO_OP = '477173' 

END;
CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_OBT_FACT_GESTION_PERSONAS()
OPTIONS(
  description="Propósito: Crear y actualizar la tabla SP_OBT_DIM_PERSONA consolidando todos los atributos segun el tipo de persona: Empleado, Proveedor, Cliente \nAutor: Maria Fernanda Franco")
BEGIN
    CREATE OR REPLACE TABLE `amrl-data-prd.CONSUMPTION.OBT_FACT_GESTION_PERSONAS`  PARTITION BY DATE(FECHA_VINCULACION) CLUSTER BY BK_PERSONA AS
      SELECT dp.BK_PERSONA
             ,dp.TIPO_TERCERO
             ,dp.CLASIFICACION_TERCERO
             ,dp.TIPO_IDENTIFICACION
             ,dp.NUMERO_DE_IDENTIFICACION
             ,dp.LUGAR_DE_EXPEDICION
             ,dp.NOMBRE_COMPLETO
             ,dp.PRIMER_NOMBRE
             ,dp.SEGUNDO_NOMBRE
             ,dp.PRIMER_APELLIDO
             ,dp.SEGUNDO_APELLIDO
             ,dp.FECHA_VINCULACION
             ,dp.FECHA_ACTUALIZACION_SAGRILAFT
             ,dp.FECHA_DE_NACIMIENTO
             ,dp.PAIS_DE_RESIDENCIA
             ,dp.CIUDAD_DE_RESIDENCIA
             ,dp.DEPARTAMENTO_DE_RESIDENCIA
             ,dp.ESTADO_ACTIVACION
             ,dp.DIRECCION_RESIDENCIA
             ,dp.EMAIL
             ,dp.TELEFONO
             ,dp.AUTORIZA_MENSAJES_DE_TEXTO
             ,dp.AUTORIZA_CORREOS_ELECTRONICOS
             ,dp.GENERO
             ,dp.ESTADO_CIVIL
             ,dp.PROFESION
             ,dp.OCUPACION
             ,dp.CARGO
             ,dp.DONDE_LABORA
             ,dp.TIEMPO_SERVICIO
             ,dp.INGRESO_MENSUAL
             ,dp.EGRESOS_MENSUALES
             ,dp.TOTAL_ACTIVOS
             ,dp.TOTAL_PASIVOS
             ,dp.PEP
             ,dp.PUBLICAMENTE_EXPUESTO
             ,dp.BANCO
             ,dp.TIPO_DE_CUENTA
             ,dp.CUENTA
             ,dp.BK_PROVEEDOR
             ,dp.BK_COMPRADOR2
             ,dp.FECHA_ACTUALIZACION_BQ AS FECHA_ACTUALIZACION_BQ_DIM_PERSONA
             ,dp.FECHA_CARGUE_BQ AS FECHA_CARGUE_BQ_DIM_PERSONA
             ,dt.*
      FROM `PRESENTATION.DIM_PERSONA` AS dp
      LEFT JOIN `PRESENTATION.DIM_TIEMPO` AS dt ON DATE(dp.FECHA_VINCULACION) = dt.FECHA;
    
END;
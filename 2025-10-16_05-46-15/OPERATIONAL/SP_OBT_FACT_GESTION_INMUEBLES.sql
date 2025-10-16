CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_OBT_FACT_GESTION_INMUEBLES()
OPTIONS(
  description="Propósito:Crear y actualizar la tabla OBT_FACT_GESTION_INMUEBLES_NEW\n en el DataSet de COMSUPTION, La tabla consolida los atributos revelantes de los items vendidos y disponibles incluye articulos de CDA. \nAutor: Maria Fernanda Franco Modificaciones: 2025-09-24: se ajusta CTE OPORTUNIDAD_ACTIVA_INVENTARIO para traer solo la oportunidad mas reciente por articulo \n02-10-2025: Se retira el campo FECHA_PROYECTADA_POLIZA_DECENAL también eliminado en Oracle\n02-10-2025: Se incluyen las columnas FECHA_PROYECTADA_DE_ENTREGA, FECHA_PROYECTADA_DE_ENTREGA_EDITABLE,FECHA_PROYECTADA_DE_HABILITACION,INMUEBLE_CERRADO_OBRA,INMUEBLE_HABILITADO_OBRA,INMUEBLE_ENTREGADO_OBRA\n06-10-2025: Se incluye la columna FO.ACEPTAR_VENTA y FO.AFEN\n08-10-2025: Se incluye descripcion y etiqeutas de la tabla")
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.CONSUMPTION.OBT_FACT_GESTION_INMUEBLES` PARTITION BY DATE(FECHA_ACTUALIZACION_PRECIO) CLUSTER BY SK_ARTICULO

 
OPTIONS (--Se incluye descripcion y etiquetas para la tabla, V 1.0. AM 08102025
description = ''' El modelo transaccional de gestión de inmuebles consolida el detalle de los atributos asociados al inventario inmobiliario de la compañía.
Incluye información específica a nivel de inventario, proyecto y subetapa en la que se encuentra cada inmueble. Adicionalmente, integra datos relacionados con el estado comercial del inmueble, identificando si posee una venta activa o, en caso contrario, el detalle de desistimientos asociados. El modelo también incorpora información general sobre la venta, el cliente vinculado y el estado de cuenta correspondiente a la oportunidad, junto con los indicadores de cumplimiento de trámites asociados al proceso de venta. ''',
  labels = [('tipo', 'modelo_datos'),('estado', 'en_estabilizacion')] 
)
 
 AS
        --Subconsulta #2
      WITH OPORTUNIDAD_ACTIVA_INVENTARIO AS (
            SELECT BK_ARTICULO
                   --,SK_OP AS SK_OP_ACTUAL
                   ,ARRAY_AGG(SK_OP ORDER BY FECHA_DE_SEPARACION DESC) [OFFSET(0)] AS  SK_OP_ACTUAL
                   --,BK_NUMERO_OP,
            FROM `PRESENTATION.FACT_OPORTUNIDAD`
            WHERE ETAPA_DE_VENTA IN ('2. Separación', '3. Promesa','4. Escrituración','5. Entrega')
            AND DESISTIDO = 'N'
            GROUP BY BK_ARTICULO
                  -- AND BK_ARTICULO = 300000072302123       
      ),
           
      --Subconsulta #4
      ULTIMA_OPORTUNIDAD_DESISTIDA AS (
            SELECT T1.BK_ARTICULO
                   ,T1.SK_OP_DESISTIDA
                   ,T2.TOTAL_DESISTIMIENTOS
                   ,T1.FECHA_ULTIMO_DESISTIMIENTO
                   ,T3.TOTAL_OPORTUNIDADES_OPCIONADAS
            FROM (
                  FROM (--T1: OBTIENE LA ULTIMA OPORTUNIDAD DESISTIDA            
                              SELECT BK_ARTICULO
                                    ,SK_OP AS SK_OP_DESISTIDA 
                                    ,FECHA_ULTIMO_DESISTIMIENTO                 
                              FROM (SELECT BK_ARTICULO
                                          ,SK_OP
                                          ,BK_NUMERO_OP
                                          ,FECHA_DE_SEPARACION
                                          ,FECHA_DESISTIDO AS FECHA_ULTIMO_DESISTIMIENTO
                                          ,ROW_NUMBER() OVER (PARTITION BY BK_ARTICULO  ORDER BY FECHA_DE_SEPARACION DESC) AS row_num
                                    FROM `PRESENTATION.FACT_OPORTUNIDAD`
                                    WHERE ETAPA_DE_VENTA IN ('2. Separación', '3. Promesa','4. Escrituración','5. Entrega')
                                          AND DESISTIDO = 'Y' --AND BK_ARTICULO = 300000072197467
                                    ) 
                              WHERE row_num =1  
                        ) AS T1 
                  LEFT JOIN (--T2: OBTIENE LA CANTIDAD DE VECES QUE EL INMUEBLE FUE DESISTIDO  
                              SELECT BK_ARTICULO
                                    ,COUNT(SK_OP) AS TOTAL_DESISTIMIENTOS                 
                              FROM `PRESENTATION.FACT_OPORTUNIDAD`
                              WHERE ETAPA_DE_VENTA IN ('2. Separación', '3. Promesa','4. Escrituración','5. Entrega') 
                                    AND DESISTIDO = 'Y' --AND BK_ARTICULO = 300000072197467
                              GROUP BY BK_ARTICULO
                        ) AS T2 ON T1.BK_ARTICULO = T2.BK_ARTICULO     
                  LEFT JOIN (--T3:OBTIENE LA CANTIDAD DE VECES QUE EL INMUEBLE FUE OPCIONADO EN UNA OPORTUNIDAD  
                              SELECT BK_ARTICULO
                                    ,COUNT(SK_OP) AS TOTAL_OPORTUNIDADES_OPCIONADAS                 
                              FROM `PRESENTATION.FACT_OPORTUNIDAD`
                              WHERE ETAPA_DE_VENTA IN ('1. Visita', '2. Separación', '3. Promesa','4. Escrituración','5. Entrega') 
                              -- AND BK_ARTICULO = 300000072197467
                              GROUP BY BK_ARTICULO 
                        ) AS T3 ON  T1.BK_ARTICULO = T3.BK_ARTICULO      
                  )
            --WHERE T1.BK_ARTICULO = 300000072197467
      ),       
            
      --*Subconsulta #8 
      --##Pendiente Incluir en PIVOT estado de cuenta
      ESTADO_CUENTA_AGREGADO AS (
              SELECT   BK_NUMERO_OP,
                      -- CUOTA INICIAL (agrupación de conceptos previos a escrituración)
                      SUM(CASE 
                        WHEN CONCEPTO LIKE 'CUOTA %' OR CONCEPTO = 'SEPARACION' OR CONCEPTO = 'ULTIMA CUOTA'
                          OR CONCEPTO = 'CONFIRMACION' OR CONCEPTO LIKE 'CESANTIAS %'
                          OR CONCEPTO LIKE 'SUBSIDIO %' OR CONCEPTO LIKE 'AHORRO PROGRAMAD %'
                          OR CONCEPTO LIKE 'AHORRO PROGRAMADO %' OR CONCEPTO LIKE 'CUENTA AFC %'
                          OR CONCEPTO LIKE 'PENS VOLUNT%' THEN VALOR_PACTADO
                        ELSE 0 END) AS TOTAL_CUOTA_INICIAL,
                      -- TOTAL PAGADO
                      SUM(PAGO_REALIZADO) AS TOTAL_PAGADO,
                      -- VALOR EN MORA
                      SUM(CASE WHEN ESTADO_CARTERA <> 'AL DÍA' THEN VALOR_EN_MORA ELSE 0 END) AS VALOR_EN_MORA,
                      -- ANTIGUEDAD MORA
                      MAX(CASE WHEN ESTADO_CARTERA <> 'AL DÍA' THEN CANTIDAD_DIAS_EN_MORA END) AS CANTIDAD_DIAS_EN_MORA,
                      MAX(CASE WHEN ESTADO_CARTERA <> 'AL DÍA' THEN ESTADO_CARTERA END) AS ANTIGUEDAD_MORA,
                      -- CANTIDAD DE CUOTAS (cuotas y separación)
                      COUNT(CASE WHEN CONCEPTO LIKE 'CUOTA %' OR CONCEPTO = 'ULTIMA CUOTA' THEN 1 END) AS CANTIDAD_DE_CUOTAS,
                      SUM(CASE WHEN CONCEPTO LIKE 'CUOTA %' OR CONCEPTO = 'ULTIMA CUOTA' THEN VALOR_PACTADO END) AS MONTO_TOTAL_CUOTAS,
                      -- PAGOS INDIVIDUALES
                      MAX(CASE WHEN CONCEPTO = 'SEPARACION' THEN PAGO_REALIZADO END) AS SEPARACION,
                      MAX(CASE WHEN CONCEPTO = 'CONFIRMACION' THEN PAGO_REALIZADO END) AS CONFIRMACION,
                      -- COMPONENTES ESPECÍFICOS DE CUOTA INICIAL
                      SUM(CASE WHEN CONCEPTO LIKE 'CESANTIAS 1%' OR CONCEPTO LIKE 'CESANTIAS 2%' THEN VALOR_PACTADO END) AS CESANTIAS,
                      SUM(CASE WHEN CONCEPTO LIKE 'AFC 1%' OR CONCEPTO LIKE 'AFC 2%' THEN VALOR_PACTADO END) AS AFC,
                      SUM(CASE WHEN CONCEPTO LIKE 'AHORRO PROGRAMAD%' THEN VALOR_PACTADO END) AS AHORRO_PROGRAMADO,
                      SUM(CASE WHEN CONCEPTO LIKE 'SUBSIDIO %' THEN VALOR_PACTADO END) AS SUBSIDIO,
                      -- CRÉDITOS
                      SUM(CASE WHEN CONCEPTO LIKE 'CREDITO%' THEN VALOR_PACTADO END) AS CREDITO_TOTAL,
                      SUM(CASE WHEN CONCEPTO LIKE 'CREDITO' THEN VALOR_PACTADO END) AS MONTO_CREDITO,
                      MAX(CASE WHEN CONCEPTO LIKE 'CREDITO' THEN ENTIDAD_FINANCIERA END) AS ENTIDAD_CREDITO,
                      SUM(CASE WHEN CONCEPTO LIKE 'CREDITO TERCERO 1' THEN VALOR_PACTADO END) AS MONTO_CREDITO_TER1,
                      MAX(CASE WHEN CONCEPTO LIKE 'CREDITO TERCERO 1' THEN ENTIDAD_FINANCIERA END) AS ENTIDAD_CREDITO_TER1,
                      SUM(CASE WHEN CONCEPTO LIKE 'CREDITO TERCERO 2' THEN VALOR_PACTADO END) AS MONTO_CREDITO_TER2,
                      MAX(CASE WHEN CONCEPTO LIKE 'CREDITO TERCERO 2' THEN ENTIDAD_FINANCIERA END) AS ENTIDAD_CREDITO_TER2,
                      -- ENTIDADES SUBSIDIO
                      MAX(CASE WHEN CONCEPTO LIKE 'SUBSIDIO 1' THEN ENTIDAD_FINANCIERA END) AS ENTIDAD_SUBSIDIO1,
                      MAX(CASE WHEN CONCEPTO LIKE 'SUBSIDIO 2' THEN ENTIDAD_FINANCIERA END) AS ENTIDAD_SUBSIDIO2
              FROM `PRESENTATION.FACT_ESTADO_CUENTA`
              GROUP BY BK_NUMERO_OP
      ),
      
      --*Subconsulta #9
      DIM_INMUEBLES AS (
            SELECT *
            FROM (
                  SELECT DISTINCT SK_ARTICULO,      
                          BK_COD_TRANSACCIONAL,
                          BK_ARTICULO,
                          NOMBRE_ARTICULO, 
                          CATEGORIA,
                          TIPO_DE_ARTICULO, 
                          CLASE_DE_ARTICULO,
                          REFERENCIA,
                          AREA_CONSTRUIDA,
                          AREA_COMUN,
                          BK_PRECIO,
                          PRECIO_UNITARIO,--Es el precio por articulo tal como se encuentra en la oportunidad 
                          PRECIO_DE_LISTA, --Es el precio con el que nace el articulo en CPQ   
                          ESTADO_DEL_ARTICULO,
                          FASE_CICLO_DE_VIDA,
                          MATRICULA_INMOBILIARIA,
                          ENCARGO_FIDUCIARIO,
                          FECHA_PROYECTADA_ESCRITURACION,
                          FECHA_PROYECTADA_CTO,
                          FECHA_REAL_CTO,                          
                          FECHA_REAL_POLIZA_DECENAL,
                          FECHA_PROYECTADA_DE_ENTREGA,
                          FECHA_PROYECTADA_DE_ENTREGA_EDITABLE,
                          FECHA_PROYECTADA_DE_HABILITACION,
                          INMUEBLE_CERRADO_OBRA,
                          INMUEBLE_HABILITADO_OBRA,
                          INMUEBLE_ENTREGADO_OBRA,                          
                          BK_OP,
                          FECHA_ACTUALIZACION_PRECIO,
                          FECHA_ACTUALIZACION_BQ,
                          FECHA_CARGUE_BQ,                
                          ROW_NUMBER() OVER (PARTITION BY NOMBRE_ARTICULO  ORDER BY BK_PRECIO DESC) AS row_num_precio_unitario 
                  FROM `PRESENTATION.DIM_INMUEBLES`
                  )
            WHERE row_num_precio_unitario = 1 --AND  NOMBRE_ARTICULO = 'CJMC23136-1-2804-1' 
      ),

      --*Subconsulta #6
      --##validar si lo podemos incluir en la subconsulta ULTIMA_OPORTUNIDAD_DESISTIDA y le cambiamos el nombre a la tabla
      OPORTUNIDAD_SELECCIONADA AS (
            SELECT COALESCE(OAI.BK_ARTICULO, UOD.BK_ARTICULO) AS BK_ARTICULO,
                    OAI.SK_OP_ACTUAL,
                    UOD.SK_OP_DESISTIDA,
                    UOD.TOTAL_DESISTIMIENTOS,
                    UOD.FECHA_ULTIMO_DESISTIMIENTO,
                    UOD.TOTAL_OPORTUNIDADES_OPCIONADAS,
                    COALESCE(OAI.SK_OP_ACTUAL, UOD.SK_OP_DESISTIDA) AS SK_OP_SELECCIONADA
            FROM DIM_INMUEBLES IB
            LEFT JOIN OPORTUNIDAD_ACTIVA_INVENTARIO OAI ON IB.SK_ARTICULO = OAI.BK_ARTICULO
            LEFT JOIN ULTIMA_OPORTUNIDAD_DESISTIDA UOD ON IB.SK_ARTICULO = UOD.BK_ARTICULO
      )
   
        --Consulta INDEX
        SELECT 
	      --*********************Atributos del articulo
		    DISTINCT IB.SK_ARTICULO,      
                IB.BK_COD_TRANSACCIONAL,
                IB.BK_ARTICULO,
                IB.NOMBRE_ARTICULO, 
                IB.CATEGORIA,
                IB.TIPO_DE_ARTICULO, 
                IB.CLASE_DE_ARTICULO,
                IB.REFERENCIA,
		    SAFE.REGEXP_EXTRACT(IB.REFERENCIA, r'^([^\\-]+)') AS TORRE,
                SAFE.REGEXP_EXTRACT(IB.REFERENCIA, r'^[^-]+-([^-]+)') AS APTO,
                IB.AREA_CONSTRUIDA,
                IB.AREA_COMUN,
                IB.BK_PRECIO,
                IB.PRECIO_UNITARIO,--Es el precio por articulo tal como se encuentra en la oportunidad 
                IB.PRECIO_DE_LISTA, --Es el precio con el que nace el articulo en CPQ   
                IB.ESTADO_DEL_ARTICULO,
		    CASE WHEN ESTADO_DEL_ARTICULO = 'Vendido' AND VDPT.FESC IS NOT NULL THEN 'D. ESCRITURADO'
                    WHEN ESTADO_DEL_ARTICULO = 'Vendido' AND VDPT.LLPT IS NOT NULL THEN 'C. VENDIDO PROMETIDO'
                    WHEN ESTADO_DEL_ARTICULO = 'Vendido' AND VDPT.LLPT IS NULL THEN 'B. VENDIDO NO PROMETIDO'
                    WHEN ESTADO_DEL_ARTICULO != 'Vendido' THEN 'A. NO VENDIDO'
			  ELSE 'A. NO VENDIDO' END AS  CLASIFICACION,
		    CASE WHEN ESTADO_DEL_ARTICULO = 'Vendido' THEN 1 ELSE 0 END VENDIDO,
                IB.FASE_CICLO_DE_VIDA,
                IB.MATRICULA_INMOBILIARIA,
                IB.ENCARGO_FIDUCIARIO,
                IB.FECHA_PROYECTADA_ESCRITURACION,
                IB.FECHA_PROYECTADA_CTO,
                IB.FECHA_REAL_CTO,                          
                CASE WHEN IB.FECHA_REAL_CTO IS NOT NULL THEN 1 ELSE 0 END TIENE_CTO, 
                IB.FECHA_REAL_POLIZA_DECENAL,
                CASE WHEN IB.FECHA_REAL_POLIZA_DECENAL IS NOT NULL THEN 1 ELSE 0 END TIENE_POLIZA_DECENAL,
                IB.FECHA_PROYECTADA_DE_ENTREGA,
                IB.FECHA_PROYECTADA_DE_ENTREGA_EDITABLE,
                IB.FECHA_PROYECTADA_DE_HABILITACION,
                IB.INMUEBLE_CERRADO_OBRA,
                IB.INMUEBLE_HABILITADO_OBRA,
                IB.INMUEBLE_ENTREGADO_OBRA,                               
                IB.BK_OP,
                IB.FECHA_ACTUALIZACION_PRECIO,		   
		--*********************Atributos de la Venta
		    FO.INMUEBLE_PRINCIPAL, --validar usos, una oportunidad tiene solo un inmueble principal y por la referencia se puede identificar
                FO.BK_NUMERO_OP AS OPORTUNIDAD,
                DP.SALA_DE_VENTAS,
                DP.SALA_DE_VENTAS_ALTERNATIVA,
		    FO.IMPORTE_TOTAL_OP,
                FO.FECHA_DE_SEPARACION,
                FO.FECHA_DE_SEPARACION_AJUSTADA,
                FO.DESISTIDO,
                FO.FECHA_DESISTIDO,
                CASE WHEN FO.DESISTIDO = 'Y' THEN FO.IMPORTE_TOTAL_OP_PRIMERA_VERSION ELSE 0 END AS PRECIO_DESISTIMIENTO,
                COALESCE(OS.TOTAL_DESISTIMIENTOS,0) AS TOTAL_DESISTIMIENTOS,
		    OS.FECHA_ULTIMO_DESISTIMIENTO,
		    CASE 
                    WHEN EXTRACT(YEAR FROM OS.FECHA_ULTIMO_DESISTIMIENTO) = EXTRACT(YEAR FROM CURRENT_DATE()) 
                         AND EXTRACT(MONTH FROM OS.FECHA_ULTIMO_DESISTIMIENTO) = EXTRACT(MONTH FROM CURRENT_DATE()) THEN 1  
                    ELSE 0 END AS DESISTIDO_ESTE_MES,
                OS.TOTAL_OPORTUNIDADES_OPCIONADAS, -- se modifico el nombre TOTAL_OPORTUNIDADES_INMUEBLE 
		    CASE WHEN EXTRACT(YEAR FROM FO.FECHA_DE_SEPARACION_AJUSTADA) = EXTRACT(YEAR FROM CURRENT_DATE()) 
                          AND EXTRACT(MONTH FROM FO.FECHA_DE_SEPARACION_AJUSTADA) = EXTRACT(MONTH FROM CURRENT_DATE()) THEN 1  ELSE 0 END AS VENDIDO_ESTE_MES,
		    FO.ETAPA_DE_VENTA,
                FO.ACEPTAR_VENTA,
		    CASE WHEN FO.BLOQUEO = 'Y' THEN 1 ELSE 0 END AS BLOQUEADO,
                UPPER(dpv.NOMBRE_COMPLETO) AS VENDEDOR,
                FO.OP_CREADA_POR,                
                UPPER(dpc.NOMBRE_COMPLETO) AS COMPRADOR1,
                UPPER(dpc.NUMERO_DE_IDENTIFICACION) AS IDENTIFICACION_COMPRADOR1,
                dpc.FECHA_DE_NACIMIENTO AS FECHA_DE_NACIMIENTO_COMPRADOR1,
                UPPER(dpc.PAIS_DE_RESIDENCIA) AS PAIS_COMPRADOR1,
                UPPER(dpc.CIUDAD_DE_RESIDENCIA) AS CIUDAD_COMPRADOR1,
                UPPER(dpc.DIRECCION_RESIDENCIA) AS DIRECCION_COMPRADOR1,
                UPPER(dpc.EMAIL) AS EMAIL_COMPRADOR1,
                UPPER(dpc.TELEFONO) AS TELEFONO_COMPRADOR1,
                UPPER(dpc.OCUPACION) AS OCUPACION_COMPRADOR1,
                UPPER(dpc2.NOMBRE_COMPLETO) AS COMPRADOR2,
                UPPER(dpc2.NUMERO_DE_IDENTIFICACION) AS IDENTIFICACION_COMPRADOR2,
                dpc2.FECHA_DE_NACIMIENTO AS FECHA_DE_NACIMIENTO_COMPRADOR2,
                UPPER(dpc2.PAIS_DE_RESIDENCIA)  AS PAIS_COMPRADOR2,
                UPPER(dpc2.CIUDAD_DE_RESIDENCIA) AS CIUDAD_COMPRADOR2,
                UPPER(dpc2.DIRECCION_RESIDENCIA) AS DIRECCION_COMPRADOR2,
                UPPER(dpc2.EMAIL) AS EMAIL_COMPRADOR2,
                UPPER(dpc2.TELEFONO) AS TELEFONO_COMPRADOR2,
                UPPER(dpc2.OCUPACION) AS OCUPACION_COMPRADOR2,
		--*********************Atributos del proyecto
                DP.MACROCONSOLIDADOR AS MACROCONSOLIDADOR,
                DP.TRANSACCIONAL AS TRANSACCIONAL,				       
                DP.TIPO_DE_PROYECTO,
                DP.DIRECTOR_DE_VENTAS_VIVIENDAS_SUBETAPA AS DIRECTOR_DE_VENTAS,
                DP.CIUDAD AS CIUDAD_PROYECTO,       
                COALESCE(DP.TOTAL_VIVIENDAS_MACROPROYECTO,0) AS TOTAL_VIVIENDAS_MACROPROYECTO,  --REVISAR
                COALESCE(DP.TOTAL_COMERCIOS_MACROPROYECTO,0) AS TOTAL_COMERCIOS_MACROPROYECTO, --REVISAR
                COALESCE(DP.TOTAL_VIVIENDAS_SUBETAPA,0) AS TOTAL_VIVIENDAS_SUBETAPA,
                COALESCE(DP.TOTAL_COMERCIOS_SUBETAPA,0) AS TOTAL_COMERCIOS_SUBETAPA,
                COALESCE(DP.RITMO_VENTAS_TRANSACCIONAL,0) AS RITMO_VENTAS_TRANSACCIONAL,
                DP.FECHA_PUNTO_DE_EQUILIBRIO AS FECHA_PUNTO_DE_EQUILIBRIO_SUBETAPA,
                CASE WHEN DP.TIPO_DE_PROYECTO LIKE '%MAYOR A VIS%' THEN DP.TOTAL_VIVIENDAS_SUBETAPA * 0.6
                     WHEN DP.TIPO_DE_PROYECTO = 'EMPRESARIAL' THEN DP.TOTAL_VIVIENDAS_SUBETAPA * 0.6
                     ELSE DP.TOTAL_VIVIENDAS_SUBETAPA * 0.6 END AS PUNTO_EQUILIBRIO_SUBETAPA,
                DP.BANCO_CONSTRUCTOR,
		    DP.FECHA_PROYECTADA_RPH,
                DP.FECHA_REAL_RPH,
		    CASE WHEN ECA.SUBSIDIO IS NULL THEN 0
                    WHEN (ENTIDAD_SUBSIDIO1 NOT LIKE 'CAJA DE COMPENSACION FAMILIAR%' AND ENTIDAD_SUBSIDIO1 NOT LIKE '%VIVIENDA FONVIVIENDA%') THEN 1
                    WHEN (ENTIDAD_SUBSIDIO2 NOT LIKE 'CAJA DE COMPENSACION FAMILIAR%' AND ENTIDAD_SUBSIDIO2 NOT LIKE '%VIVIENDA FONVIVIENDA%') THEN 1
                ELSE 0 END AS OTRO_SUBSIDIO,
                CASE WHEN (ENTIDAD_SUBSIDIO1 LIKE '%CAJA DE COMPENSACION FAMILIAR%' OR ENTIDAD_SUBSIDIO2 LIKE '%CAJA DE COMPENSACION FAMILIAR%') THEN 1 
                     ELSE 0 END AS TIENE_SUBSIDIO_CAJA,
                CASE WHEN (ENTIDAD_SUBSIDIO1 LIKE '%VIVIENDA FONVIVIENDA%' OR ENTIDAD_SUBSIDIO2 LIKE '%VIVIENDA FONVIVIENDA%') THEN 1 ELSE 0 END AS TIENE_MI_CASA_YA,
		    CASE WHEN DP.FECHA_REAL_RPH IS NOT NULL THEN 1 ELSE 0 END TIENE_RPH,
		    DP.FECHA_MAXIMA_CUOTA_INICIAL,
		    DATE_DIFF(DATE(DP.FECHA_MAXIMA_CUOTA_INICIAL), CURRENT_DATE(), MONTH) AS PLAZO_CUOTA_INICIAL_MESES,
                DATE_DIFF(DATE(DP.FECHA_MAXIMA_CUOTA_INICIAL), CURRENT_DATE(), DAY) AS PLAZO_CUOTA_INICIAL_DIAS, 
            --*********************Atributos del estado de cuenta				
                ECA.TOTAL_CUOTA_INICIAL,
                ECA.TOTAL_PAGADO,
                ECA.VALOR_EN_MORA,
                ECA.CANTIDAD_DIAS_EN_MORA,
                ECA.ANTIGUEDAD_MORA,
                ECA.CANTIDAD_DE_CUOTAS,
                ECA.MONTO_TOTAL_CUOTAS,
                ECA.SEPARACION,
                ECA.CONFIRMACION,
                ECA.CESANTIAS,
                ECA.AFC,
                ECA.AHORRO_PROGRAMADO,
                ECA.SUBSIDIO,
                ECA.ENTIDAD_SUBSIDIO1,
                ECA.ENTIDAD_SUBSIDIO2,
                ECA.MONTO_CREDITO,
                ECA.ENTIDAD_CREDITO,
                ECA.MONTO_CREDITO_TER1,
                ECA.ENTIDAD_CREDITO_TER1,
                ECA.MONTO_CREDITO_TER2,
                ECA.ENTIDAD_CREDITO_TER2,
                ECA.CREDITO_TOTAL,        
            --*********************Tramites			
                VDPT.RVDF AS RVDF_FECHA_VINCULACION, 
                VDPT.BSTR AS BSTR_FECHA_PRIMER_BLOQUEO,
		    CASE WHEN VDPT.RVDF IS NOT NULL THEN 1 ELSE 0 END AS RVDF_VINCULADO,
		    VDPT.LLPT AS LLPT_FECHA_PROMESA,
                CASE WHEN VDPT.LLPT IS NOT NULL THEN 1 ELSE 0 END AS VDPT_PROMETIDO,
		    UPPER(FORMAT_DATE('%B', LLPT,"America/Bogota")) AS MES_LLPT,
		    VDPT.EPP5,
                CASE WHEN VDPT.EPP5 IS NOT NULL THEN 1 ELSE 0 END AS EPP5_PACTADO_ESCRITURA,  
		    UPPER(FORMAT_DATE('%B', EPP5,"America/Bogota")) AS MES_EPP5,
		    VDPT.VCAV AS VCAV,								
                CASE WHEN DATE(VDPT.VCAV) >= CURRENT_DATE()  THEN 1 ELSE 0 END AS VCAV_ESTADO_CARTA_CREDITO,
		    CASE WHEN VDPT.VCAV IS NOT NULL THEN 1 ELSE 0 END AS VCAV_VENCIMIENTO_APROBACION_CLIENTE,
		    VDPT.AFEC,
		    CASE WHEN VDPT.AFEC IS NOT NULL THEN 1 ELSE 0 END AS AFEC_AGENDADO_ESCRITURACION,	
		    VDPT.FESC AS FESC_FECHA_ESCRITURACION,
                CASE WHEN VDPT.FESC IS NOT NULL THEN 1 ELSE 0 END AS FESC_ESCRITURADO,
		    VDPT.EIRE AS EIRE_FECHA_ENTREGA,
                CASE WHEN VDPT.EIRE IS NOT NULL THEN 1 ELSE 0 END AS EIRE_ENTREGADO,		
		    VDPT.RASB,
                VDPT.AFEN,
	  FROM DIM_INMUEBLES IB
        LEFT JOIN `PRESENTATION.DIM_PROYECTO` DP ON IB.BK_COD_TRANSACCIONAL = DP.SK_COD_TRANSACCIONAL
        LEFT JOIN OPORTUNIDAD_SELECCIONADA OS ON IB.SK_ARTICULO = OS.BK_ARTICULO
        LEFT JOIN `PRESENTATION.FACT_OPORTUNIDAD` AS FO ON FO.SK_OP = OS.SK_OP_SELECCIONADA --AND FO.BK_ARTICULO = IB.SK_ARTICULO
        LEFT JOIN  `PRESENTATION.DIM_PERSONA` dpv ON  fo.BK_VENDEDOR = dpv.BK_PERSONA 
        LEFT JOIN  `PRESENTATION.DIM_PERSONA` dpc ON  fo.BK_PERSONA = dpc.BK_PERSONA --obtiene la informacion del comprador principal
        LEFT JOIN  `PRESENTATION.DIM_PERSONA` dpc2 ON  dpc.BK_COMPRADOR2 = dpc2.BK_PERSONA --obtiene la informacion del comprador secundario
        LEFT JOIN ESTADO_CUENTA_AGREGADO AS ECA ON ECA.BK_NUMERO_OP = FO.BK_NUMERO_OP
        LEFT JOIN `PRESENTATION.VW_PIVOT_DimTramites` VDPT ON FO.SK_OP = VDPT.BK_OP;
        --where NOMBRE_ARTICULO = 'CJMC23136-1-2804-1';    
END;
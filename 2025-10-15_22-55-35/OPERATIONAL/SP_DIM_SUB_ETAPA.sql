CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_DIM_SUB_ETAPA()
OPTIONS(
  description="Propósito: Crear y actualizar la tabla DIM_SUB_ETAPA consolidando todos los atributos que se encuentren a nivel de subetapa\nAutor: Maria Fernanda Franco\nModificaciones:\n2025-07-17: Se ajusta la subconsulta T2 para incluir en una sola fila todos los tipos de proyecto que pueda tener una subetapa el cambio soluciona el error en el campo tipo de proyecto cuando los proyectos son mixtos (VIS y VIP)\n2025-07-17: Se incluye la columna ESTADO_PROYECTO\n2025-07-29: Se modifica la linea 62 de INNER JOIN a LEFT JOIN ")
BEGIN

CREATE TEMP TABLE TEMP_SUBETAPA AS (

    SELECT ROW_NUMBER() OVER (ORDER BY HZRE.RECORD_NAME  ) AS ID_DIM_SUBETAPA,
            HZRE.EXTN_ATTRIBUTE_CHAR007                     AS NOMBRE_PROYECTO_HV,  --Campo nuevo, editado y usado por inteligencia de negocios en la hoja de vida del proyecto (modulo: otros > proyectos)
            HZRE.EXTN_ATTRIBUTE_CHAR008                     AS TRANSACCIONAL,
            HZRE.RECORD_NAME                                AS SK_COD_TRANSACCIONAL,	            								
            FTN.PARENTPK1VALUE                              AS BK_COD_MACROCONSOLIDADOR,
            HZRE.EXTN_ATTRIBUTE_CHAR036                     AS BK_COD_MACROPROYECTO,	
            T1.TIPO_DE_PROYECTO         			              AS TIPO_DE_PROYECTO,
            MOT.EXTN_ATTRIBUTE_CHAR007                      AS ESTADO_PROYECTO --Pendiente confirmar si todos los proyectos estan activos.
            ,CASE WHEN HZRE.EXTN_ATTRIBUTE_TIMESTAMP004 IS NOT NULL
                    THEN 'Patrimonio'
                    ELSE 'Preventa'
            END ESTADO_FINANCIERO_PROYECTO,
            HZRE.EXTN_ATTRIBUTE_CHAR009          AS CIUDAD,
            HZRE.EXTN_ATTRIBUTE_CHAR011          AS DIRECCION_PROYECTO,
            HZRE.EXTN_ATTRIBUTE_CHAR039          AS ESTRATO,
            HZRE.EXTN_ATTRIBUTE_CHAR010 			   AS SALA_DE_VENTAS,
            MOT.RECORD_NAME                      AS SALA_DE_VENTAS_ALTERNATIVA,
            HZRE.EXTN_ATTRIBUTE_CHAR012 				 AS ENTIDAD_FIDUCIARIA, 
            HZRE.EXTN_ATTRIBUTE_CHAR014 				 AS BANCO_CONSTRUCTOR,
            HZRE.EXTN_ATTRIBUTE_CHAR020 				 AS BANCO_RECAUDADOR,
            HZRE.EXTN_ATTRIBUTE_CHAR021 				 AS CUENTA_RECAUDADORA,
            HZRE.EXTN_ATTRIBUTE_TIMESTAMP001     AS FECHA_MAXIMA_CUOTA_INICIAL,
            HZRE.EXTN_ATTRIBUTE_NUMBER008        AS VALOR_SEPARACION,
            HZRE.EXTN_ATTRIBUTE_NUMBER006        AS VALOR_CONFIRMACION,
            HZRE.EXTN_ATTRIBUTE_NUMBER030				 AS TOTAL_VIVIENDAS_TRANSACCIONAL,
            HZRE.EXTN_ATTRIBUTE_NUMBER031        AS TOTAL_COMERCIOS_TRANSACCIONAL,
            HZRE.EXTN_ATTRIBUTE_NUMBER026        AS ENCARGO_FIDUCIARIO_PROYECTO,
            HZRE.EXTN_ATTRIBUTE_CHAR071          AS DIRECTOR_DE_VENTAS_VIVIENDAS_TRANSACCIONAL,
            HZRE.EXTN_ATTRIBUTE_CHAR078          AS DIRECTOR_DE_VENTAS_COMERCIOS_TRANSACCIONAL,	
            HZRE.EXTN_ATTRIBUTE_NUMBER032        AS RITMO_VENTAS_TRANSACCIONAL,
            HZRE.EXTN_ATTRIBUTE_TIMESTAMP010     AS FECHA_INICIO_VENTAS_TRANSACCIONAL,
            HZRE.EXTN_ATTRIBUTE_CHAR045          AS COORDINADOR_ENCARGADO,		
            HZRE.CREATION_DATE			 						 AS	FECHA_CREACION_TRANSACCIONAL,	            
            HZRE.EXTN_ATTRIBUTE_TIMESTAMP004     AS FECHA_PUNTO_DE_EQUILIBRIO,
            HZRE.EXTN_ATTRIBUTE_TIMESTAMP009     AS FECHA_PROYECTADA_RPH,
            HZRE.EXTN_ATTRIBUTE_TIMESTAMP011     AS FECHA_REAL_RPH,            
            HZRE.LAST_UPDATE_DATE                AS FECHA_ULTIMA_ACTUALIZACION_ORC,
						CURRENT_DATETIME("America/Bogota")   AS FECHA_ACTUALIZACION_BQ,
						CURRENT_DATETIME("America/Bogota")   AS FECHA_CARGUE_BQ 
						FROM `RAW.FUSION_HZ_REF_ENTITIES` HZRE 
						LEFT JOIN ( --Obtiene el Tipo de Proyecto
												SELECT *
											 	FROM 
													(
														SELECT ATTRIBUTE_CHAR1,
                                   STRING_AGG(TIPO_DE_PROYECTO, ' - ' ORDER BY TIPO_DE_PROYECTO) AS TIPO_DE_PROYECTO
                            FROM ( SELECT DISTINCT ATTRIBUTE_CHAR1
                                         ,ATTRIBUTE_CHAR5 AS TIPO_DE_PROYECTO
                                      --	,LAST_UPDATE_DATE 
                                  FROM `RAW.FUSION_EGO_ITEM_EFF_B`						
                                  WHERE CONTEXT_CODE = 'Inmuebles' AND CATEGORY_CODE = 'INMUEBLE' AND ATTRIBUTE_CHAR2 IS NOT NULL AND ATTRIBUTE_CHAR5 <> 'CDA' 
                                  --AND ATTRIBUTE_CHAR1 = 'PRVV20191'
                                )                                 
                            GROUP BY ATTRIBUTE_CHAR1                            
                            ) AS T2											 
											)	AS T1	ON HZRE.RECORD_NAME = T1.ATTRIBUTE_CHAR1 
            LEFT JOIN `RAW.FUSION_FND_TREE_NODE` AS FTN ON FTN.PK1STARTVALUE = HZRE.RECORD_NAME --Obtine el codigo Macroconsolidador
            LEFT JOIN `RAW.FUSION_MOT_REF_ENTITIES` AS MOT ON MOT.ID = HZRE.EXTN_ATTRIBUTE_NUMBER001
						WHERE HZRE.ATTRIBUTE_CATEGORY = 'OCS_Proyectos_c' --AND HZRE.EXTN_ATTRIBUTE_CHAR008 LIKE '%AQUA%'          
					  ORDER BY HZRE.EXTN_ATTRIBUTE_CHAR008

    );


--CREATE OR REPLACE TABLE PRESENTATION.DIM_SUB_ETAPA AS 
--SELECT * FROM TEMP_SUBETAPA 

    MERGE   PRESENTATION.DIM_SUB_ETAPA       AS T
    USING   (SELECT * FROM TEMP_SUBETAPA)   AS S
    ON      (T.SK_COD_TRANSACCIONAL = S.SK_COD_TRANSACCIONAL)
    WHEN MATCHED THEN
    UPDATE SET  T.NOMBRE_PROYECTO_HV              =   S.NOMBRE_PROYECTO_HV,
                T.TRANSACCIONAL                   =   S.TRANSACCIONAL,
                T.BK_COD_MACROCONSOLIDADOR        =   S.BK_COD_MACROCONSOLIDADOR,  
                T.BK_COD_MACROPROYECTO            =   S.BK_COD_MACROPROYECTO,                
                T.TIPO_DE_PROYECTO                =   S.TIPO_DE_PROYECTO,
                T.ESTADO_PROYECTO                 =   S.ESTADO_PROYECTO,
                T.ESTADO_FINANCIERO_PROYECTO      =   S.ESTADO_FINANCIERO_PROYECTO,
                T.CIUDAD                          =   S.CIUDAD,
                T.DIRECCION_PROYECTO              =   S.DIRECCION_PROYECTO,
                T.ESTRATO                         =   S.ESTRATO,
                T.SALA_DE_VENTAS                  =   S.SALA_DE_VENTAS,
                T.SALA_DE_VENTAS_ALTERNATIVA      =   S.SALA_DE_VENTAS_ALTERNATIVA, 
                T.ENTIDAD_FIDUCIARIA              =   S.ENTIDAD_FIDUCIARIA,
                T.BANCO_CONSTRUCTOR               =   S.BANCO_CONSTRUCTOR,
                T.BANCO_RECAUDADOR                =   S.BANCO_RECAUDADOR,
                T.CUENTA_RECAUDADORA              =   S.CUENTA_RECAUDADORA,
                T.FECHA_MAXIMA_CUOTA_INICIAL      =   S.FECHA_MAXIMA_CUOTA_INICIAL,
                T.VALOR_SEPARACION                =   S.VALOR_SEPARACION,
                T.VALOR_CONFIRMACION              =   S.VALOR_CONFIRMACION,
                T.TOTAL_VIVIENDAS_TRANSACCIONAL   =   S.TOTAL_VIVIENDAS_TRANSACCIONAL,
                T.TOTAL_COMERCIOS_TRANSACCIONAL   =   S.TOTAL_COMERCIOS_TRANSACCIONAL,
                T.ENCARGO_FIDUCIARIO_PROYECTO     =   S.ENCARGO_FIDUCIARIO_PROYECTO,
                T.DIRECTOR_DE_VENTAS_VIVIENDAS_TRANSACCIONAL            = S.DIRECTOR_DE_VENTAS_VIVIENDAS_TRANSACCIONAL,
                T.DIRECTOR_DE_VENTAS_COMERCIOS_TRANSACCIONAL            = S.DIRECTOR_DE_VENTAS_COMERCIOS_TRANSACCIONAL, 
                T.RITMO_VENTAS_TRANSACCIONAL                            = S.RITMO_VENTAS_TRANSACCIONAL,
                T.FECHA_INICIO_VENTAS_TRANSACCIONAL                     = S.FECHA_INICIO_VENTAS_TRANSACCIONAL,
                T.COORDINADOR_ENCARGADO                                 = S.COORDINADOR_ENCARGADO,
                T.FECHA_CREACION_TRANSACCIONAL                          = S.FECHA_CREACION_TRANSACCIONAL,
                T.FECHA_PUNTO_DE_EQUILIBRIO                             = S.FECHA_PUNTO_DE_EQUILIBRIO,
                T.FECHA_PROYECTADA_RPH                                  = S.FECHA_PROYECTADA_RPH,
                T.FECHA_REAL_RPH                                        = S.FECHA_REAL_RPH,
                T.FECHA_ULTIMA_ACTUALIZACION_ORC                        = S.FECHA_ULTIMA_ACTUALIZACION_ORC,
                T.FECHA_ACTUALIZACION_BQ                                = CURRENT_DATETIME("America/Bogota")
    WHEN NOT MATCHED THEN
    INSERT  (   ID_DIM_SUBETAPA,
                NOMBRE_PROYECTO_HV,
                TRANSACCIONAL,
                SK_COD_TRANSACCIONAL,                
                BK_COD_MACROCONSOLIDADOR,                
                BK_COD_MACROPROYECTO,
                TIPO_DE_PROYECTO,
                ESTADO_PROYECTO,
                ESTADO_FINANCIERO_PROYECTO,
                CIUDAD, 
                DIRECCION_PROYECTO,
                ESTRATO,
                SALA_DE_VENTAS,
                SALA_DE_VENTAS_ALTERNATIVA,
                ENTIDAD_FIDUCIARIA,
                BANCO_CONSTRUCTOR,
                BANCO_RECAUDADOR,
                CUENTA_RECAUDADORA,
                FECHA_MAXIMA_CUOTA_INICIAL,
                VALOR_SEPARACION,
                VALOR_CONFIRMACION,
                TOTAL_VIVIENDAS_TRANSACCIONAL,
                TOTAL_COMERCIOS_TRANSACCIONAL,
                ENCARGO_FIDUCIARIO_PROYECTO,
                DIRECTOR_DE_VENTAS_VIVIENDAS_TRANSACCIONAL,
                DIRECTOR_DE_VENTAS_COMERCIOS_TRANSACCIONAL,
                RITMO_VENTAS_TRANSACCIONAL,
                FECHA_INICIO_VENTAS_TRANSACCIONAL,
                COORDINADOR_ENCARGADO,
                FECHA_CREACION_TRANSACCIONAL,                
                FECHA_PUNTO_DE_EQUILIBRIO,
                FECHA_PROYECTADA_RPH,
                FECHA_REAL_RPH,
                FECHA_ULTIMA_ACTUALIZACION_ORC,
                FECHA_ACTUALIZACION_BQ,
                FECHA_CARGUE_BQ
            )
    VALUES (    S.ID_DIM_SUBETAPA,
                S.NOMBRE_PROYECTO_HV,
                S.TRANSACCIONAL,
                S.SK_COD_TRANSACCIONAL,                
                S.BK_COD_MACROCONSOLIDADOR,                
                S.BK_COD_MACROPROYECTO,               
                S.TIPO_DE_PROYECTO,
                S.ESTADO_PROYECTO,
                S.ESTADO_FINANCIERO_PROYECTO,
                S.CIUDAD, 
                S.DIRECCION_PROYECTO,
                S.ESTRATO,
                S.SALA_DE_VENTAS,
                S.SALA_DE_VENTAS_ALTERNATIVA,
                S.ENTIDAD_FIDUCIARIA,
                S.BANCO_CONSTRUCTOR,
                S.BANCO_RECAUDADOR,
                S.CUENTA_RECAUDADORA,
                S.FECHA_MAXIMA_CUOTA_INICIAL,
                S.VALOR_SEPARACION,
                S.VALOR_CONFIRMACION,
                S.TOTAL_VIVIENDAS_TRANSACCIONAL,
                S.TOTAL_COMERCIOS_TRANSACCIONAL,
                S.ENCARGO_FIDUCIARIO_PROYECTO,
                S.DIRECTOR_DE_VENTAS_VIVIENDAS_TRANSACCIONAL,
                S.DIRECTOR_DE_VENTAS_COMERCIOS_TRANSACCIONAL,
                S.RITMO_VENTAS_TRANSACCIONAL,
                S.FECHA_INICIO_VENTAS_TRANSACCIONAL,
                S.COORDINADOR_ENCARGADO,
                S.FECHA_CREACION_TRANSACCIONAL,                
                S.FECHA_PUNTO_DE_EQUILIBRIO,
                S.FECHA_PROYECTADA_RPH,
                S.FECHA_REAL_RPH,
                S.FECHA_ULTIMA_ACTUALIZACION_ORC,
                CURRENT_DATETIME("America/Bogota"),
                CURRENT_DATETIME("America/Bogota")
            );
END;
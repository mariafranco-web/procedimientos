CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FACT_OPORTUNIDAD()
OPTIONS(
  description="Propósito: Crear y actualizar la tabla FACT_OPORTUNIDADES, consolidando todos los atributos que se encuentran en la pestaña resumen de la oportunidad de Oracle Fusion. La tabla muestra el total de oportunidades, la etapa en la que se encuentra la venta y el ID de los articulos asociados a la oporunidad (bk_articulo).\nAutor: Maria Fernanda Franco\nUsos: Tablero de Indicadores, TrackingTools\nModificaciones: 2025-02-07: Se incluye la columna FECHA_SEPARACION_AJUSTADA\nModificaciones: 2025-03-28: Se incluye CASE para la columna BK_PERSONA antes solo se llamaba el campo PRCONPARTYID\nModificaciones: 2025-04-11: Se incluye LEFT JOIN con la tabla RAW.FUSION_ZCA_SALES_ORDER_HEADERS \nModificaciones: 2025-05-14: Se incluye la condición ORDERTOTAL > 0 para la subtabla ZSOH\nModificaciones: 2025-07-01: Se retira DISTINCT de la linea 6\nModificaciones: 2025-07-29: Se incluye la subconsulta T1 para incluir los porcentajes de participación en la compra\nModificaciones: 2025-10-03: Se incluye la subconsulta T2 para añadir campo Grupo de gestores")
BEGIN
  CREATE OR REPLACE TABLE `amrl-data-prd.PRESENTATION.FACT_OPORTUNIDAD` AS

            SELECT MO.OPTYID                                                                    AS SK_OP
                  ,MO.OPTYNUMBER                                                                AS BK_NUMERO_OP       
                  ,MO.NAME                                                                      AS NOMBRE_OP
                  ,MO.SALESMETHODID                                                             AS METODO_DE_VENTAS
                  ,MST.NAME                                                                     AS ETAPA_DE_VENTA
                  ,MO.EXTNATTRIBUTECHAR024                                                      AS INMUEBLE_PRINCIPAL
                  ,MO.EXTNATTRIBUTENUMBER021                                                    AS IMPORTE_TOTAL_OP
                  ,ZSOH.IMPORTE_TOTAL_OP_PRIMERA_VERSION                                        AS IMPORTE_TOTAL_OP_PRIMERA_VERSION
                  ,MO.OWNERRESOURCEID                                                           AS BK_VENDEDOR
                  ,MO.CREATEDBY                                                                 AS OP_CREADA_POR
                  ,MO.CREATIONDATE                                                              AS FECHA_DE_CREACION_OP
                  ,MO.LASTUPDATEDBY                                                             AS OP_ACTUALIZADA_POR
                  ,MO.LASTUPDATEDATE                                                            AS FECHA_ULTIMA_ACTUALIZACION_OP
                  ,MO.EXTNATTRIBUTECHAR091                                                      AS ACEPTAR_VENTA
                  ,MO.EXTNATTRIBUTECHAR028                                                      AS BK_COD_TRANSACCIONAL        
                  ,MO.EXTNATTRIBUTECHAR019                                                      AS TIPO_VENTA
                  ,MO.EXTNATTRIBUTECHAR048                                                      AS DESTINO
                  ,MO.EXTNATTRIBUTECHAR034                                                      AS REFERIDO
                  ,MO.COMMENTS                                                                  AS OBSERVACIONES
                  ,MO.EXTNATTRIBUTENUMBER012                                                    AS PORCENTAJE_COMPRADORES
                  ,COALESCE(T1.PORCENTAJE_COMPRADOR1,0)                                         AS PORCENTAJE_COMPRADOR1
                  ,COALESCE(T1.PORCENTAJE_COMPRADOR2,0)                                         AS PORCENTAJE_COMPRADOR2
                  ,COALESCE(CAST(mo.EXTNATTRIBUTETIMESTAMP014 AS DATE),CAST('1900-01-01' AS DATE)) AS FECHA_DE_SEPARACION --FECHA DE SEPARACION ORACLE
                  ,CASE --FECHA_DE_SEPARACION_AJUSTADA
                        WHEN IEVV.NUEVA_FECHA IS NOT NULL THEN COALESCE(CAST(IEVV.NUEVA_FECHA AS DATE),CAST('1900-01-01' AS DATE)) 
                        ELSE COALESCE(CAST(mo.EXTNATTRIBUTETIMESTAMP014 AS DATE),CAST('1900-01-01' AS DATE)) 
                   END AS FECHA_DE_SEPARACION_AJUSTADA --FECHA USADA PARA EL CALCULO DE VENTAS NETAS INCLUYENDO LAS EXCLUSIONES E INCLUSIONES REALIZADAS POR IN 
                  ,MO.EXTNATTRIBUTETIMESTAMP009                                                 AS FECHA_COMPROMISO_RADICACION_CREDITO 
                  ,MO.EXTNATTRIBUTETIMESTAMP013                                                 AS FECHA_COMPROMISO_RADICACION_SUBSIDIO 
                  ,MO.EXTNATTRIBUTENUMBER014                                                    AS INMUEBLE_OPCIONADO
                  ,MO.EXTNATTRIBUTECHAR100                                                      AS ENTIDAD_CREDITO
                  ,MO.EXTNATTRIBUTENUMBER022                                                    AS MONTO_CREDITO
                  ,MO.EXTNATTRIBUTECHAR101                                                      AS ENTIDAD_CREDITO_TER_1
                  ,MO.EXTNATTRIBUTENUMBER024                                                    AS MONTO_CREDITO_TER_1      
                  ,MO.EXTNATTRIBUTECHAR102                                                      AS ENTIDAD_CREDITO_TER_2
                  ,MO.EXTNATTRIBUTENUMBER026                                                    AS MONTO_CREDITO_TER_2
                  ,MO.EXTNATTRIBUTECHAR103                                                      AS ENTIDAD_SUBSIDIO_1
                  ,MO.EXTNATTRIBUTENUMBER028                                                    AS MONTO_SUBSIDIO_1
                  ,MO.EXTNATTRIBUTECHAR104                                                      AS ENTIDAD_SUBSIDIO_2
                  ,MO.EXTNATTRIBUTENUMBER030                                                    AS MONTO_SUBSIDIO_2
                  ,MO.EXTNATTRIBUTETIMESTAMP003                                                 AS PUNTO_DE_EQUILIBRIO
                  ,MO.EXTNATTRIBUTECHAR029                                                      AS BLOQUEO
                  ,MO.EXTNATTRIBUTECHAR030                                                      AS NUMERO_DE_ESCRITURA
                  ,MO.EXTNATTRIBUTETIMESTAMP006                                                 AS FECHA_DE_ESCRITURA
                  ,MO.EXTNATTRIBUTECHAR031                                                      AS NOTARIA
                  ,MO.EXTNATTRIBUTECHAR032                                                      AS CIRCULO_NOTARIAL                
                  ,MO.EXTNATTRIBUTECHAR016                                                      AS DESISTIDO		
                  ,MO.EXTNATTRIBUTETIMESTAMP005                                                 AS FECHA_DESISTIDO                 
                  ,CASE --FECHA_DESISTIDO_AJUSTADA
                        WHEN IEVD.NUEVA_FECHA IS NOT NULL THEN COALESCE(CAST(IEVD.NUEVA_FECHA AS TIMESTAMP))         
                        ELSE MO.EXTNATTRIBUTETIMESTAMP005 
                   END AS FECHA_DESISTIDO_AJUSTADA ----FECHA USADA PARA EL CALCULO DE VENTAS NETAS INCLUYENDO LAS EXCLUSIONES E INCLUSIONES REALIZADAS POR IN 
                  ,MO.EXTNATTRIBUTETIMESTAMP004                                                 AS FECHA_RADICADO_DOCUMENTOS_DESISTIMIENTO
                  ,MO.EXTNATTRIBUTECHAR010                                                      AS CAUSA_DESISTIMIENTO
                  ,MO.EXTNATTRIBUTECHAR012                                                      AS TIPO_DESISTIMIENTO
                  ,MO.EXTNATTRIBUTECHAR075                                                      AS ARRAS_SANCION_DESISTIMIENTO
                  ,MO.EXTNATTRIBUTECHAR037                                                      AS CHECK_ENVIAR_APROBACION_DESISTIMIENTO
                  ,MO.EXTNATTRIBUTETIMESTAMP036                                                 AS FECHA_ENVIO_APROBACION_DESISTIMIENTO
                  ,MO.EXTNATTRIBUTECHAR038                                                      AS CHECK_APROBACION_DIRECTOR
                  ,MO.EXTNATTRIBUTETIMESTAMP021                                                 AS FECHA_DESISTIMIENTO_DIRECTOR
                  ,MO.EXTNATTRIBUTECLOB001                                                      AS OBSERVACION_DESISTIMIENTO 
                  ,MO.EXTNATTRIBUTECHAR118                                                      AS ID_TRABAJO_DESISTIMIENTO
                  ,MO.EXTNATTRIBUTECHAR116                                                      AS RECIBO_GENERADO_DESISTIMIENTO
                  ,MO.EXTNATTRIBUTECHAR119                                                      AS INFORMACION_PROCESO_DESISTIMIENTO
                  ,T2.RECORD_NAME                                                               AS GRUPO_RESPONSABLE_ASESOR
                  ,EIEB.EFF_LINE_ID                                                             AS BK_ARTICULO                  
                  ,CASE --BK_PERSONA
                        WHEN PRCONPARTYID IS NULL THEN CUSTPARTYID
                        ELSE PRCONPARTYID
                   END BK_PERSONA --IDENTIFICA SI EL CLIENTE ES UNA PERSONA O UNA ORGANIZACION
                  ,MO.EXTNATTRIBUTENUMBER001                                                    AS BK_OP_CDA                  
                  ,CURRENT_DATETIME("America/Bogota")                                           AS FECHA_ACTUALIZACION_BQ
                  ,CURRENT_DATETIME("America/Bogota")                                           AS FECHA_CARGUE_BQ,
                  --,EIEB.INVENTORY_ITEM_ID                                                     AS BK_ARTICULO
            FROM `RAW.FUSION_MOO_OPTY` AS MO 
            INNER JOIN (-- Traduce la etapa de venta
                        SELECT *
                        FROM `RAW.FUSION_MOO_STG_TL` AS MST
                        WHERE MST.LANGUAGE = 'E') AS MST ON MO.CURRSTGID = MST.STGID 
            RIGHT JOIN (-- Tabla puente entre los atributos del inventario y el historico de precios
                        SELECT REVENUEINVENTORYITEMID    
                        ,REVENUEREVNID
                        ,REVENUEREVNLASTUPDATEDATE
                        ,REVENUEUPSIDEAMT
                        ,OPPORTUNITIESOPTYID
                        ,REVENUEOPTYID
                        FROM `RAW.FUSION_MOO_REVN`    
                        WHERE REVENUEINVENTORYITEMID IS NOT NULL 
                              AND DELETED IS NULL --Excluye duplicados
                              -- AND OPPORTUNITIESOPTYID = 300000683923837
                        ORDER BY REVENUEINVENTORYITEMID                                          
                      ) AS MR ON MO.OPTYID = MR.REVENUEOPTYID --AND MO.EXTNATTRIBUTENUMBER021 = MR.UPSIDEAMT
            LEFT JOIN ( -- Incluye los atributos del articulo
                        SELECT *
                        FROM `RAW.FUSION_EGO_ITEM_EFF_B`
                        WHERE CONTEXT_CODE = 'Inmuebles') AS EIEB  ON MR.REVENUEINVENTORYITEMID = EIEB.INVENTORY_ITEM_ID 
            LEFT JOIN `RAW.FUSION_EGP_SYSTEM_ITEMS_B` AS ESIB ON EIEB.INVENTORY_ITEM_ID = ESIB.ITEMBASEPEOINVENTORYITEMID
            LEFT JOIN (-- Incluye el precio total de la primera version de la OP
                        SELECT *
                        FROM (
                              SELECT ACTIVEVERSIONFLAG, 
                                    LASTUPDATEDATE, 
                                    ORDERTOTAL AS IMPORTE_TOTAL_OP_PRIMERA_VERSION,
                                    OPTYID,
                                    ROW_NUMBER() OVER (PARTITION BY OPTYID ORDER BY LASTUPDATEDATE ASC) AS row_num
                              FROM `RAW.FUSION_ZCA_SALES_ORDER_HEADERS`
                              WHERE ORDERTOTAL > 0 --Omite las multiples versiones N_1 
                                    --AND OPTYID= 300000649748372 
                              ORDER BY LASTUPDATEDATE ASC
                        )  AS T0
                        WHERE T0.row_num = 1
                  ) AS ZSOH ON ZSOH.OPTYID = MO.OPTYID	
            LEFT JOIN (-- Incluye la FECHA_DE_SEPARACION_AJUSTADA
                        SELECT *
                        FROM `RAW.INTERNAL_EXCLUSION_VENTAS` 
                        WHERE ACCION LIKE '%Ventas%'
                  ) AS IEVV ON MO.OPTYNUMBER = IEVV.BK_NUMERO_OP
            LEFT JOIN (-- Incluye la FECHA_DESISTIDO_AJUSTADA
                        SELECT *
                        FROM `RAW.INTERNAL_EXCLUSION_VENTAS` 
                        WHERE ACCION LIKE '%Desistimiento%'
                  ) AS IEVD ON MO.OPTYNUMBER = IEVD.BK_NUMERO_OP
            LEFT JOIN (-- Incluye el porcentaje de participacion de cada comprador
                        SELECT OPTYID
                              ,MAX(CASE WHEN row_num = '1'  THEN PORCENTAJE_COMPRADOR END) AS PORCENTAJE_COMPRADOR1
                              ,MAX(CASE WHEN row_num = '2'  THEN PORCENTAJE_COMPRADOR END) AS PORCENTAJE_COMPRADOR2       
                        FROM (
                              SELECT CAST(ROW_NUMBER() OVER (PARTITION BY OPTYID ORDER BY PORCENTAJE_COMPRADOR DESC) AS STRING) AS row_num
                                    ,OPTYID
                                    ,PORCENTAJE_COMPRADOR
                              FROM (
                                    SELECT A.OPTYID,
                                          CASE 
                                                WHEN B.EXTN_ATTRIBUTE_NUMBER006 IS NULL THEN C.EXTNATTRIBUTENUMBER004 
                                                WHEN C.EXTNATTRIBUTENUMBER004  IS NULL THEN B.EXTN_ATTRIBUTE_NUMBER006 
                                                ELSE 0
                                          END PORCENTAJE_COMPRADOR                                   
                                          --,B.EXTN_ATTRIBUTE_NUMBER006 AS PORCENTAJE_PERSONA_JURIDICA
                                          --,C.EXTNATTRIBUTENUMBER004 AS PORCENTAJE_PERSONA_NATURAL
                                    FROM `RAW.FUSION_MOO_OPTY` AS A 
                                    LEFT JOIN `RAW.FUSION_SVC_REF_ENTITIES_XMM` AS B ON  A.OPTYID = B.SOURCE_ID1 
                                    LEFT JOIN  `RAW.FUSION_MOO_OPTY_CON` AS C ON A.OPTYID = C.OPTYID
                                    --WHERE A.OPTYID IN (300000072894008,300000233288417,300000249866064)
                                    ) AS T1
                              ) AS T2  
                        GROUP BY OPTYID
                  ) AS T1 ON T1.OPTYID = MO.OPTYID
            LEFT JOIN (-- Incluye campo Grupo Responsable Asesor del resumen de la OP V1.0.1 AM 03102025
                        SELECT 
                        A.OPTYID,
                        B.RECORD_NAME
                        FROM `RAW.FUSION_MOO_OPTY` AS A
                        LEFT JOIN `RAW.FUSION_HZ_REF_ENTITIES` B ON A.EXTNATTRIBUTENUMBER003 = B.ID
            ) AS T2 ON T2.OPTYID = MO.OPTYID
            WHERE MST.LANGUAGE = 'E';
                  --AND MO.OPTYNUMBER  IN('599033','187948');
END;
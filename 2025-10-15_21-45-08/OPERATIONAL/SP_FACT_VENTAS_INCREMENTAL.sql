CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FACT_VENTAS_INCREMENTAL()
BEGIN
  --CREATE OR REPLACE TABLE `amrl-data-prd.PRESENTATION.FACT_VENTAS` PARTITION BY BK_FECHA_VENTA AS

  DECLARE  FECHA_INICIO_SP DEFAULT  DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH);
  DECLARE  FECHA_FIN_SP DEFAULT  CURRENT_DATE("America/Bogota");

  DELETE FROM `amrl-data-prd.PRESENTATION.FACT_VENTAS`
  WHERE BK_FECHA_VENTA BETWEEN FECHA_INICIO_SP AND FECHA_FIN_SP;

  INSERT INTO `amrl-data-prd.PRESENTATION.FACT_VENTAS`
  WITH
  VENTAS AS (
  SELECT      eieb.ATTRIBUTE_CHAR1                                                              AS  BK_SUB_ETAPA,
              esit.ITEMTRANSLATIONPEOINVENTORYITEMID                                            AS  BK_PRODUCTO,
              COALESCE(CAST(mo.EXTNATTRIBUTETIMESTAMP014 AS DATE),CAST('1900-01-01' AS DATE))   AS  BK_FECHA_VENTA,
              CASE MO.EXTNATTRIBUTECHAR025   
                WHEN 'PERSONA_NATURAL'  THEN  MO.PRCONPARTYID
                WHEN 'PERSONA_JURIDICA' THEN
                  CASE 
                    WHEN zrex.EXTN_ATTRIBUTE_CHAR008 = 'Y'  THEN zrex.TARGET_ID1 
                  END
              END                                                                               AS  BK_COMPRADOR_PRINCIPAL,
              CASE MO.EXTNATTRIBUTECHAR025   
                WHEN 'PERSONA_NATURAL'  THEN  comps.PERPARTYID
                WHEN 'PERSONA_JURIDICA' THEN
                  CASE 
                    WHEN zrex.EXTN_ATTRIBUTE_CHAR008 = 'N'  THEN zrex.TARGET_ID1 
                  END
              END                                                                               AS  BK_COMPRADOR_SECUNDARIO,
              mo.OWNERRESOURCEID                                                                AS  BK_VENDEDOR,
              mo.EXTNATTRIBUTECHAR025                                                           AS  TIPO_COMPRADOR,
              mo.OPTYNUMBER                                                                     AS  CODIGO_OPORTUNIDAD,
              COALESCE(UPPER(mo.COMMENTS),'')                                                   AS  COMENTARIOS,
              CASE MO.EXTNATTRIBUTECHAR025   
                WHEN 'PERSONA_NATURAL'  THEN  comp.EXTNATTRIBUTENUMBER004 
                WHEN 'PERSONA_JURIDICA' THEN
                  CASE 
                    WHEN zrex.EXTN_ATTRIBUTE_CHAR008 = 'Y'  THEN zrex.EXTN_ATTRIBUTE_NUMBER006 
                  END
              END                                                                               AS  PARTICIPACION_COMPRA_PRINCIPAL,
              COALESCE(UPPER(mo.EXTNATTRIBUTECHAR048),'')                                       AS  DESTINO,
              mr.QTY                                                                            AS  CANTIDAD,
              COALESCE(mr.UPSIDEAMT,0)                                                          AS  SUB_TOTAL,
              0                                                                                 AS  DESCUENTO,
              COALESCE(mr.UPSIDEAMT,0)                                                          AS  VALOR_NETO,
              ROW_NUMBER() OVER (PARTITION BY CONCAT(MO.OPTYID,INVENTORYITEMID) ORDER BY  REVNLASTUPDATEDATE desc ) AS ROW           
              --mo.EXTNATTRIBUTETIMESTAMP014                                AS  FECHA_RADICACION_VTA_FORMATO_2,
              --mo.CREATEDBY                                                AS  CREADO_POR,
              --CAST(NULL AS DATE)                                          AS  FECHA_VINCULACION,              -- MOO_REF_ENTITIES_XMM, Oportunity
              --CAST(NULL AS DATE)                                          AS  FECHA_FIRMA_PROMESA_CLIENTE,    -- MOO_REF_ENTITIES_XMM, Oportunity
              --CAST(NULL AS DATE)                                          AS  FECHA_LLEGADA_PROMESA_TRAMITE,  -- MOO_REF_ENTITIES_XMM, Oportunity
              --CAST(NULL AS DATE)                                          AS  FECHA_ESCRITURA,                -- MOO_REF_ENTITIES_XMM, Oportunity
              --CAST(NULL AS DATE)                                          AS  FECHA_ENTREGA_INMUEBLE,         -- MOO_REF_ENTITIES_XMM, Oportunity
              --CAST(NULL AS DATE)                                          AS  FECHA_NOT_INICIO_FIRMA_PROMESA, -- MOO_REF_ENTITIES_XMM, Oportunity
              --CAST(NULL AS DATE)                                          AS  FECHA_PACTADA_ENTRE_INMUEBLE,   -- MOO_REF_ENTITIES_XMM, Oportunity
              --CAST(NULL AS DATE)                                          AS  FECHA_PRORROGA,                 -- MOO_REF_ENTITIES_XMM, Oportunity
              --CASE WHEN mo.PRCONPARTYID IS NOT NULL THEN 'Principal' END  AS  TIPO_COMPRADOR,                 -- comes Oportunity
  FROM        `amrl-data-prd.RAW.FUSION_MOO_OPTY`                AS mo --1
  INNER JOIN  `amrl-data-prd.RAW.FUSION_MOO_REVN`                AS mr   ON  mo.OPTYID                               = mr.OPTYID -- 4
  INNER JOIN  `amrl-data-prd.RAW.FUSION_EGP_SYSTEM_ITEMS_B`      AS esib ON  mr.INVENTORYITEMID                      = esib.ITEMBASEPEOINVENTORYITEMID -- INVENTORYITEM_ID -- 3
                                                                  AND mr.INVENTORYORGID                       = esib.ITEMBASEPEOORGANIZATIONID  -- ORGANIZATION_ID
  INNER JOIN `amrl-data-prd.RAW.FUSION_EGP_SYSTEM_ITEMS_TL`      AS esit ON  esib.ITEMBASEPEOINVENTORYITEMID         = esit.ITEMTRANSLATIONPEOINVENTORYITEMID -- 3
                                                                  AND esit.ITEMTRANSLATIONPEOLANGUAGE         = 'E'
  INNER JOIN `amrl-data-prd.RAW.FUSION_EGO_ITEM_EFF_B`           AS eieb ON  esit.ITEMTRANSLATIONPEOINVENTORYITEMID  = eieb.INVENTORY_ITEM_ID
  INNER JOIN `amrl-data-prd.RAW.FUSION_HZ_REF_ENTITIES`          AS hre  ON  eieb.ATTRIBUTE_CHAR1                    = hre.RECORD_NAME
  INNER JOIN `amrl-data-prd.RAW.FUSION_ZCA_SALES_ORDER_HEADERS`  AS zso  ON  mo.OPTYID                               = zso.OPTYID
  INNER JOIN `amrl-data-prd.RAW.FUSION_MOO_STG_TL`               AS mst  ON  mo.CURRSTGID                            = mst.STGID AND mst.LANGUAGE ='E'
  LEFT JOIN `amrl-data-prd.RAW.FUSION_SVC_REF_ENTITIES_XMM`      AS zrex ON  mo.OPTYID = zrex.SOURCE_ID1
  LEFT JOIN (SELECT T1.OPTYID,
                    T1.PRCONPARTYID,T2.PERPARTYID,
                    T2.EXTNATTRIBUTENUMBER004,
                    CASE 
                      WHEN  T1.PRCONPARTYID = T2.PERPARTYID THEN 'PRINCIPAL' 
                      ELSE 'SECUNDARIO' 
                    END COMPRADOR 
            FROM `amrl-data-prd.RAW.FUSION_MOO_OPTY` T1
            LEFT JOIN RAW.FUSION_MOO_OPTY_CON T2 ON  T1.OPTYID = T2.OPTYID AND T1.PRCONPARTYID = T2.PERPARTYID 
            /*WHERE T1.OPTYID = 300000617705199*/) AS comp ON mo.OPTYID = comp.OPTYID
  LEFT JOIN (SELECT * 
  FROM (SELECT T1.OPTYID,
               T1.PRCONPARTYID,
               T2.PERPARTYID,
               T2.EXTNATTRIBUTENUMBER004,
               CASE 
                WHEN  T1.PRCONPARTYID = T2.PERPARTYID THEN 'PRINCIPAL' 
                ELSE 'SECUNDARIO' 
               END COMPRADOR,
               ROW_NUMBER() OVER (PARTITION BY CONCAT(T1.OPTYID) ORDER BY  T2.EXTNATTRIBUTENUMBER004 desc ) AS ROW 
        FROM `amrl-data-prd.RAW.FUSION_MOO_OPTY` T1
        LEFT JOIN RAW.FUSION_MOO_OPTY_CON T2 ON  T1.OPTYID = T2.OPTYID AND T1.PRCONPARTYID != T2.PERPARTYID
        /*WHERE T1.OPTYID = 300000617705199*/) A WHERE A.ROW = 1)  AS comps ON mo.OPTYID = comps.OPTYID            
  WHERE   2 = 2
  AND		(eieb.CONTEXT_CODE='Inmuebles' OR eieb.CONTEXT_CODE='INMUEBLES')
  AND   mr.INVENTORYITEMID    IS NOT NULL
  AND   eieb.ATTRIBUTE_CHAR4  NOT LIKE '%CDA%'
  AND   zso.ACTIVEVERSIONFLAG = 'Y'
  AND   hre.ATTRIBUTE_CATEGORY  <> 'OCS_Solicitud_Servicio_c'
  )

  SELECT  dr.SK_PRODUCTO,
          dp.SK_MACROPROYECTO,
          dp.SK_SUB_ETAPA,
          IFNULL(dsp.SK_PERSONA,SHA256('INDETERMINADO'))  AS SK_PERSONA_COMPRADOR_PRINCIPAL,
          IFNULL(dss.SK_PERSONA,SHA256('INDETERMINADO'))  AS SK_PERSONA_COMPRADOR_SECUNDARIO,
          IFNULL(dsv.SK_PERSONA,SHA256('INDETERMINADO'))  AS SK_PERSONA_VENDEDOR,
          dt.SK_FECHA,
          fv.*
  FROM    VENTAS AS fv
  LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_PRODUCTO`  AS dr  ON fv.BK_PRODUCTO               = dr.BK_PRODUCTO
  LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_TIEMPO`    AS dt  ON fv.BK_FECHA_VENTA            = dt.FECHA
  LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_SUB_ETAPA` AS dp  ON fv.BK_SUB_ETAPA              = dp.BK_SUB_ETAPA
  LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_PERSONA`   AS dsp ON fv.BK_COMPRADOR_PRINCIPAL    = dsp.BK_PERSONA
  LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_PERSONA`   AS dss ON fv.BK_COMPRADOR_SECUNDARIO   = dss.BK_PERSONA
  LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_PERSONA`   AS dsv ON fv.BK_VENDEDOR               = dsv.BK_PERSONA
  WHERE BK_FECHA_VENTA BETWEEN FECHA_INICIO_SP AND FECHA_FIN_SP
  --WHERE   2 = 2
  --AND CODIGO_OPORTUNIDAD         = '640749'
  --WHERE CODIGO_OPORTUNIDAD         = '644475'
  AND ROW = 1;
END;
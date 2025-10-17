CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FACT_DESISTIMIENTOS_INCREMENTAL()
BEGIN
  --CREATE OR REPLACE TABLE `amrl-data-prd.PRESENTATION.FACT_DESISTIMIENTOS` PARTITION BY BK_FECHA_DESISTIMIENTO AS

DECLARE  FECHA_INICIO_SP DEFAULT  DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH);
DECLARE  FECHA_FIN_SP DEFAULT  CURRENT_DATE("America/Bogota");

DELETE FROM `amrl-data-prd.PRESENTATION.FACT_DESISTIMIENTOS`
WHERE BK_FECHA_DESISTIMIENTO BETWEEN FECHA_INICIO_SP AND FECHA_FIN_SP;

INSERT INTO `amrl-data-prd.PRESENTATION.FACT_DESISTIMIENTOS`
WITH
DESISTIMIENTOS AS 
(
SELECT  mo.OPTYID                                               AS  ID,                       -- Identificador de la oportunidad
        mo.OPTYNUMBER                                           AS  OPORTUNIDAD,              -- Número de la oportunidad
        COALESCE(mo.EXTNATTRIBUTECHAR048,'')                    AS  DESTINO,                  -- Finalidad de la compra del inmueble
        COALESCE(mo.EXTNATTRIBUTECHAR038,'')                    AS  APROBACION_DIRECTOR,      -- Permite conocer exactamanete que oportunidad ha sido desistida
        CAST(mo.EXTNATTRIBUTETIMESTAMP005 AS DATE)              AS  BK_FECHA_DESISTIMIENTO,   -- Fecha de desistimiento
        ( SELECT DISTINCT hzs.LOCATIONPEOATTRIBUTE17  
          FROM   `amrl-data-prd.RAW.FUSION_HZ_PARTY_SITES` AS hzs 
          WHERE  hzs.PARTYID = hp.PARTYID 
          AND   (SUBSTR(SUBSTR(hzs.PARTYSITENUMBER,INSTR(hzs.PARTYSITENUMBER,'.') +1,10),0,INSTR(SUBSTR(hzs.PARTYSITENUMBER,INSTR(hzs.PARTYSITENUMBER,'.') +1),'.')-1)) = mo.OPTYNUMBER  
          AND   hzs.LOCATIONPEOATTRIBUTE19 = 'Principal'
          AND   hzs.LOCATIONPEOATTRIBUTE17 IS NOT NULL 
          AND   CAST(hzs.PARTYSITEID AS STRING) <> '300000122945937' 
          AND   SUBSTR(hzs.PARTYSITENUMBER,1,INSTR(hzs.PARTYSITENUMBER,'.',1)-1) = mo.EXTNATTRIBUTECHAR024
        )                                                       AS  ENCARGO_FIDUCIARIO,       -- Codigo de fiduciaria a la cual el cliente está pagando
        COALESCE(hp.PARTYID,0)                                  AS  BK_COMPRADOR_PRINCIPAL,   -- Si el mismo de la venta Viene de la DIM_PERSONA
        COALESCE(mo.EXTNATTRIBUTECHAR010,'')                    AS  CONCEPTO,                 -- Causal del desistimiento
        COALESCE(mo.EXTNATTRIBUTECHAR012,'')                    AS  TIPO_DESIST,              -- Tipologia del desistimiento
        COALESCE(mo.EXTNATTRIBUTENUMBER019,0)                   AS  VALOR_ARRAS,              -- Penalidad que s ele cobra al cliente
        ( SELECT  SUM(COALESCE(ACRA.ARCASHRECEIPTAMOUNT,0))
          FROM    `amrl-data-prd.RAW.FUSION_HZ_CUST_ACCOUNTS`            AS  HCA 
          INNER JOIN  `amrl-data-prd.RAW.FUSION_HZ_CUST_ACCT_SITES_ALL`  AS  HCASA   ON  HCA.CUSTACCOUNTID                   = HCASA.CUSTACCOUNTID
          INNER JOIN  `amrl-data-prd.RAW.FUSION_HZ_CUST_SITE_USES_ALL`   AS  HCSUA   ON  HCSUA.CUSTACCTSITEID                = HCASA.CUSTOMERACCOUNTSITECUSTACCTSITEID
                                                                              AND HCSUA.SITEUSECODE                   = 'BILL_TO'
          INNER JOIN  `amrl-data-prd.RAW.FUSION_AR_CASH_RECEIPTS_ALL`    AS  ACRA    ON  ACRA.ARCASHRECEIPTCUSTOMERSITEUSEID = HCSUA.SITEUSEID
                                                                              AND ACRA.ARCASHRECEIPTSTATUS IN ('APP','UNAPP')
          WHERE HCSUA.PRIMARYFLAG  =   'Y'
          AND   HCA.PARTYID        =   hp.PARTYID 
          AND   HCA.ACCOUNTNAME    =   mo.EXTNATTRIBUTECHAR064
        )                                                       AS  VALOR_RECAUDADO, -- Es el acumulado de la venta
        COALESCE(zsoh.ORDERTOTAL,0)                             AS  VALOR_VENTA,     -- Valor solamente del inmueble
        ((SELECT  SUM(COALESCE(ACRA.ARCASHRECEIPTAMOUNT,0))
          FROM    `amrl-data-prd.RAW.FUSION_HZ_CUST_ACCOUNTS`            AS  HCA 
          INNER JOIN  `amrl-data-prd.RAW.FUSION_HZ_CUST_ACCT_SITES_ALL`  AS  HCASA   ON  HCA.CUSTACCOUNTID                   = HCASA.CUSTACCOUNTID
          INNER JOIN  `amrl-data-prd.RAW.FUSION_HZ_CUST_SITE_USES_ALL`   AS  HCSUA   ON  HCSUA.CUSTACCTSITEID                = HCASA.CUSTOMERACCOUNTSITECUSTACCTSITEID
                                                                              AND HCSUA.SITEUSECODE                   = 'BILL_TO'
          INNER JOIN  `amrl-data-prd.RAW.FUSION_AR_CASH_RECEIPTS_ALL`    AS  ACRA    ON  ACRA.ARCASHRECEIPTCUSTOMERSITEUSEID = HCSUA.SITEUSEID
                                                                              AND ACRA.ARCASHRECEIPTSTATUS IN ('APP','UNAPP')
          WHERE HCSUA.PRIMARYFLAG  =   'Y'
          AND   HCA.PARTYID        =   hp.PARTYID 
          AND   HCA.ACCOUNTNAME    =   mo.EXTNATTRIBUTECHAR064)
          - COALESCE(mo.EXTNATTRIBUTENUMBER019,0)
        )                                                       AS  VALOR_DEVUELTO,  --  Es el valor de vuelto
        (SELECT DISTINCT COALESCE(hzp.PARTYNAME,'') 
        FROM   `amrl-data-prd.RAW.FUSION_HZ_PARTIES` AS hzp 
        WHERE  hzp.PARTYID = mo.OWNERRESOURCEID
        )                                                       AS  RESPONSABLE,     --  Vendedor
        COALESCE(mo.EXTNATTRIBUTECHAR028,'')                    AS  BK_SUB_ETAPA,    --  Codigo de la sub ETAPA -- viene dela DIM_SUB_ETAPA
        COALESCE(mst.NAME,'')                                   AS  ETAPA_VENTA,     --  Etapa de venta
        COALESCE(mo.EXTNATTRIBUTECHAR024,'')                    AS  INMUEBLE,        --  Identificador del inmueble
        (SELECT DISTINCT CAST(mrex.EXTN_ATTRIBUTE_TIMESTAMP002  AS  DATE)            
					FROM  `amrl-data-prd.RAW.FUSION_MOO_REF_ENTITIES_XMM` AS mrex
					WHERE mrex.SOURCE_ID1             = mo.OPTYID 
          AND   mrex.EXTN_ATTRIBUTE_CHAR001 = 'RVDF'
        )                                                       AS  FECHA_VINCULACION,    --  Fecha en que el cliente se creó en ORACLE
			  CAST(mo.EXTNATTRIBUTETIMESTAMP014 AS DATE)              AS  BK_FECHA_VENTA        --  Fecha de venta     --  Viene de la FACT_VENTAS
FROM    `amrl-data-prd.RAW.FUSION_MOO_OPTY`                  AS mo
LEFT JOIN `amrl-data-prd.RAW.FUSION_MOO_STG_TL`              AS mst  ON  mo.CURRSTGID    =   mst.STGID
                                                              AND mst.NAME        <>  '1. Visita'
                                                              AND mst.NAME        <>  '0. Transición'
                                                              AND mst.LANGUAGE    =   'E'
LEFT JOIN `amrl-data-prd.RAW.FUSION_HZ_PARTIES`              AS hp   ON  COALESCE(mo.PRCONPARTYID,mo.CUSTPARTYID) = hp.PARTYID
LEFT JOIN `amrl-data-prd.RAW.FUSION_ZCA_SALES_ORDER_HEADERS` AS zsoh ON  zsoh.OPTYID     =   mo.OPTYID
                                                              AND zsoh.VERSIONNUMBER           =   1 
                                                              AND COALESCE(zsoh.ORDERTOTAL,0)  <>  0
WHERE 2 = 2
AND   mo.EXTNATTRIBUTECHAR016 = 'Y'
AND   mo.EXTNATTRIBUTECHAR029 = 'Y'
GROUP BY  BK_FECHA_DESISTIMIENTO, hp.PARTYID, hp.PARTYNUMBER, CONCAT(hp.PRIMARYPHONEAREACODE,HP.PRIMARYPHONENUMBER),
          mo.EXTNATTRIBUTECHAR019, mo.EXTNATTRIBUTECHAR010, mo.EXTNATTRIBUTECHAR012, mo.EXTNATTRIBUTENUMBER019,
          hp.PARTYNAME, mo.OPTYNUMBER, mo.EXTNATTRIBUTECHAR048, mo.EXTNATTRIBUTECHAR038, zsoh.ORDERTOTAL,
          mo.OWNERRESOURCEID, mo.EXTNATTRIBUTECHAR022, mo.EXTNATTRIBUTECHAR027, mo.EXTNATTRIBUTECHAR064,
          mo.EXTNATTRIBUTECHAR028, mst.NAME, mo.OPTYID, mo.EXTNATTRIBUTECHAR024, mo.EXTNATTRIBUTETIMESTAMP014,
          mo.CREATEDBY, mo.EXTNATTRIBUTECHAR095
),

INVENTARIOS AS 
(
SELECT  mr.OPTYID                                   AS  ID,                   --  ID de sistema de la oportunidad
		    COALESCE(hre.RECORD_NAME,'')                AS  BK_SUB_ETAPA,         --  Código de la SUB_ETAPA
        esit.ITEMTRANSLATIONPEOINVENTORYITEMID      AS  BK_PRODUCTO,
        COALESCE(eieb.ATTRIBUTE_CHAR4,'')           AS  INMUEBLE,
				CASE WHEN hre.EXTN_ATTRIBUTE_TIMESTAMP004 IS NULL THEN 'PREVENTA'
             ELSE 'PATRIMONIO' END                  AS  PUNTO_DE_EQUILIBRIO,  --  Tipo de unidades vendidas que permite sostener el proyecto, con esto la obra se empieza a construir (Descartado)
				COALESCE(mr.UNITPRICE,0)     	              AS  VALOR_INM             --  Valor unitario del inmueble (Descartado)
FROM  `amrl-data-prd.RAW.FUSION_MOO_REVN`                AS  mr
LEFT JOIN `amrl-data-prd.RAW.FUSION_EGP_SYSTEM_ITEMS_B`  AS  esib  ON  mr.INVENTORYITEMID      								= esib.ITEMBASEPEOINVENTORYITEMID
                                                            AND mr.INVENTORYORGID       								= esib.ITEMBASEPEOORGANIZATIONID
LEFT JOIN `amrl-data-prd.RAW.FUSION_EGP_SYSTEM_ITEMS_TL` AS  esit  ON  esib.ITEMBASEPEOINVENTORYITEMID  				= esit.ITEMTRANSLATIONPEOINVENTORYITEMID
                                                            AND esit.ITEMTRANSLATIONPEOLANGUAGE   			= 'E'
LEFT JOIN `amrl-data-prd.RAW.FUSION_EGO_ITEM_EFF_B`      AS  eieb  ON  esit.ITEMTRANSLATIONPEOINVENTORYITEMID  = eieb.INVENTORY_ITEM_ID
LEFT JOIN `amrl-data-prd.RAW.FUSION_HZ_REF_ENTITIES`     AS  hre   ON  eieb.ATTRIBUTE_CHAR1    								= hre.RECORD_NAME
WHERE (eieb.CONTEXT_CODE  = 'Inmuebles' OR eieb.CONTEXT_CODE  = 'INMUEBLES')
AND   mr.INVENTORYITEMID IS NOT NULL
),

DESISTIMIENTO AS
(
SELECT DISTINCT inv.BK_SUB_ETAPA                                            AS  BK_SUB_ETAPA,
                inv.BK_PRODUCTO                                             AS  BK_PRODUCTO,
                des.BK_FECHA_DESISTIMIENTO                                  AS  BK_FECHA_DESISTIMIENTO,
                des.BK_COMPRADOR_PRINCIPAL                                  AS  BK_COMPRADOR_PRINCIPAL,
                des.OPORTUNIDAD                                             AS  CODIGO_OPORTUNIDAD,
                UPPER(des.DESTINO)                                          AS  DESTINO,
                UPPER(des.APROBACION_DIRECTOR)                              AS  APROBACION_DIRECTOR,
                COALESCE(des.ENCARGO_FIDUCIARIO,'')                         AS  ENCARGO_FIDUCIARIO,
                UPPER(des.CONCEPTO)                                         AS  CONCEPTO,
                UPPER(des.TIPO_DESIST)                                      AS  TIPO_DESISTIMIENTO,
                UPPER(des.ETAPA_VENTA)                                      AS  ETAPA_VENTA,
                UPPER(des.RESPONSABLE)                                      AS  RESPONSABLE,
                COALESCE(des.FECHA_VINCULACION,CAST('1900-01-01' AS DATE))  AS  FECHA_VINCULACION,
                des.BK_FECHA_VENTA                                          AS  BK_FECHA_VENTA,
                des.VALOR_ARRAS                                             AS  VALOR_ARRAS,
                COALESCE(des.VALOR_RECAUDADO,0)                             AS  VALOR_RECAUDADO,
                des.VALOR_VENTA                                             AS  VALOR_VENTA,
                CASE WHEN des.VALOR_DEVUELTO < 0 THEN 0
                     ELSE COALESCE(des.VALOR_DEVUELTO,0) END                AS  VALOR_DEVUELTO
FROM DESISTIMIENTOS     AS  des
INNER JOIN INVENTARIOS  AS  inv ON  des.BK_SUB_ETAPA  = inv.BK_SUB_ETAPA
                                AND des.ID            = inv.ID
                                AND des.INMUEBLE      = inv.INMUEBLE
)
SELECT    ds.SK_MACROPROYECTO                             AS  SK_MACROPROYECTO,
          ds.SK_SUB_ETAPA                                 AS  SK_SUB_ETAPA,
          ds.BK_MACROPROYECTO                             AS  BK_MACROPROYECTO,
          ds.MACROPROYECTO                                AS  MACROPROYECTO,
          dp.SK_PRODUCTO                                  AS  SK_PRODUCTO,
          dt.SK_FECHA                                     AS  SK_FECHA,
          dt2.SK_FECHA                                    AS  SK_FECHA_VENTA,
          IFNULL(dsp.SK_PERSONA,SHA256('INDETERMINADO'))  AS  SK_PERSONA_COMPRADOR_PRINCIPAL,
          fd.*
FROM    DESISTIMIENTO AS fd
LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_PRODUCTO`   AS dp   ON  fd.BK_PRODUCTO            = dp.BK_PRODUCTO
LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_TIEMPO`     AS dt   ON  fd.BK_FECHA_DESISTIMIENTO = dt.FECHA
LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_TIEMPO`     AS dt2  ON  fd.BK_FECHA_VENTA         = dt2.FECHA
LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_SUB_ETAPA`  AS ds   ON  fd.BK_SUB_ETAPA           = ds.BK_SUB_ETAPA
LEFT JOIN `amrl-data-prd.PRESENTATION.DIM_PERSONA`    AS dsp  ON  fd.BK_COMPRADOR_PRINCIPAL = dsp.BK_PERSONA
WHERE BK_FECHA_DESISTIMIENTO BETWEEN FECHA_INICIO_SP AND FECHA_FIN_SP;
--WHERE   2 = 2;
END;
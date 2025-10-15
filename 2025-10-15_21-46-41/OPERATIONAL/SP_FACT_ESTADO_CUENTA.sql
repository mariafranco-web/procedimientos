CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FACT_ESTADO_CUENTA()
OPTIONS(
  description="Propósito: Crear y actualizar la tabla OBT_FACT_ESTADO_DE_CUENTA, consolidando todos los conceptos pagados y no pagados de cada cliente . \nAutor: Maria Fernanda Franco\nUsos: Portal privado, reporte de cartera total\nModificaciones: \n2025-05-12: Se incluye CASE en la linea 46 para obtener los datos de los clientes cuando la persona de contacto en la oportunidad solo esta en la pestaña organización de la oportunidad. Este case también es necesario debido a que el campo HZCA.PARTYID no esta llegando con información en todos los casos.\n2025-06-05: se retira CASE de la linea 46 debido a que los datos de la columna HZCA.PARTYID ya llegan completos\n2025-08-01: se incluye la linea 33,34,35,64,84,85 que incluye la conexión a la tabla FUSION_HZ_CUST_SITE_USES_ALL para obtener el NUMERO_DE_CUENTA Y NUMERO_DE_SITIO de cada CONCEPTO en el plan de pagos\n2025-08-15: Se incluye la columna TIPO_DE_TRANSACCION (RCTTA.RACUSTTRXTYPENAME )\n2025-08-21: Se comenta la condición de la linea 88 y se incluye la unión con la tabla RAW.FUSION_RA_BATCH_SOURCES_ALL para mostrar el campo ORIGEN_DE_TRANSACCION y obtener los conceptos con nomenclatura _ESC y origenes PLAN_DE_PAGO_CARTERA.\n2025-09-01: se incluyen las columnas APSA.ARPAYMENTSCHEDULECREATEDBY y APSA.ARPAYMENTSCHEDULECREATEDBY, se ajusta el WHERE de la fila 86 incluyendo el concepto 'OTROS_INGRESOS_%' \n2025-09-01: se incluye la linea 71 a 75 para realizar la extracción de la OP del dato HZPS.PARTYSITENUMBER y no del APSA.ARPAYMENTSCHEDULETRXNUMBER como se encontraba anteriormente\n2025-09-10: Se incluye el campo FECHA_NOTA_INCUMPLIMIENTO_DE_PAGO")
BEGIN
  CREATE OR REPLACE TABLE `amrl-data-prd.PRESENTATION.FACT_ESTADO_CUENTA` PARTITION BY
  DATE(FECHA_ACTUALIZACION_PLAN_DE_PAGOS) CLUSTER BY BK_NUMERO_OP, ID_CONCEPTO AS

       SELECT     T_1.SK_ESTADO_DE_CUENTA                      AS SK_ESTADO_DE_CUENTA,  
            ROW_NUMBER() OVER (PARTITION BY T_1.BK_NUMERO_OP ORDER BY T_1.FECHA_LIMITE_DE_PAGO ASC) AS ID_CONCEPTO,	-- Esta columna es unicamente para organizar las cuotas                                       
            T_1.CONCEPTO  		                           AS CONCEPTO,  
            T_1.ORIGEN_DE_TRANSACCION                    AS ORIGEN_DE_TRANSACCION,
            T_1.TIPO_DE_TRANSACCION                      AS TIPO_DE_TRANSACCION,
            T_1.FECHA_DE_CREACION_TRANSACCION            AS FECHA_DE_CREACION_TRANSACCION,
            T_1.TRANSACCION_CREADA_POR                   AS TRANSACCION_CREADA_POR,
            T_1.NUMERO_DE_TRANSACCION                    AS NUMERO_DE_TRANSACCION,      
            T_1.VALOR_PACTADO		                      AS VALOR_PACTADO,
            COALESCE(T_1.PAGO_REALIZADO,0)               AS PAGO_REALIZADO,
            T_1.FECHA_LIMITE_DE_PAGO	                 AS FECHA_LIMITE_DE_PAGO,
            T_1.VALOR_EN_MORA		                      AS VALOR_EN_MORA,
            T_1.ESTADO_DE_TRANSACCION	                 AS ESTADO_DE_TRANSACCION,
            CASE WHEN T_1.ESTADO_DE_TRANSACCION = 'CL' THEN 0
                 WHEN DATE_DIFF(CURRENT_DATE(), T_1.FECHA_LIMITE_DE_PAGO, DAY) < 0 THEN 0
                 ELSE DATE_DIFF(CURRENT_DATE(), T_1.FECHA_LIMITE_DE_PAGO, DAY)
            END AS CANTIDAD_DIAS_EN_MORA,
            CASE WHEN T_1.ESTADO_DE_TRANSACCION = 'CL' THEN 'AL DÍA'
                 WHEN DATE_DIFF(CURRENT_DATE(), T_1.FECHA_LIMITE_DE_PAGO, DAY) > 1 
                      AND DATE_DIFF(CURRENT_DATE(), T_1.FECHA_LIMITE_DE_PAGO, DAY) <=30  
                 THEN 'MENOS DE 30 DÍAS EN MORA'
                 WHEN DATE_DIFF(CURRENT_DATE(), T_1.FECHA_LIMITE_DE_PAGO, DAY) >30 
                      AND DATE_DIFF(CURRENT_DATE(), T_1.FECHA_LIMITE_DE_PAGO, DAY) <=90  
                 THEN 'MÁS DE 30 DÍAS DE MORA'
                 WHEN DATE_DIFF(CURRENT_DATE(), T_1.FECHA_LIMITE_DE_PAGO, DAY) >90 
                      AND DATE_DIFF(CURRENT_DATE(), T_1.FECHA_LIMITE_DE_PAGO, DAY) <=180 
                 THEN 'MÁS DE 90 DÍAS DE MORA'
                 WHEN DATE_DIFF(CURRENT_DATE(), T_1.FECHA_LIMITE_DE_PAGO, DAY) >180      
                 THEN 'MÁS DE 180 DÍAS DE MORA'				
                 ELSE 'AL DÍA'
             END                                                      AS ESTADO_CARTERA,
             T_1.ENTIDAD_FINANCIERA                                   AS ENTIDAD_FINANCIERA,
             T_2.MEANING                                              AS MOTIVO_DE_INCUMPLIMIENTO_DE_PAGO,
             T_2.NOTETXT                                              AS NOTAS_INCUMPLIMIENTO_DE_PAGO,
             T_2.LASTUPDATEDATE                                       AS FECHA_NOTA_INCUMPLIMIENTO_DE_PAGO,
             T_1.FECHA_ACTUALIZACION_PLAN_DE_PAGOS                    AS FECHA_ACTUALIZACION_PLAN_DE_PAGOS,
             T_1.BK_PERSONA                                           AS BK_PERSONA,
             T_1.BK_NUMERO_OP		                                   AS BK_NUMERO_OP,           
             T_1.BK_COBRO                                             AS BK_COBRO,
             T_1.BK_SITIO_CLIENTE                                     AS BK_SITIO_CLIENTE,
             T_1.NUMERO_DE_SITIO                                      AS NUMERO_DE_SITIO,
             T_1.BK_NUMERO_DE_CUENTA                                  AS BK_NUMERO_DE_CUENTA,              
             CURRENT_DATETIME("America/Bogota")                       AS FECHA_ACTUALIZACION_BQ,
             CURRENT_DATETIME("America/Bogota")                       AS FECHA_CARGUE_BQ
   FROM (
          SELECT RCTLA.RACUSTOMERTRXLINEDESCRIPTION 			AS CONCEPTO,
                RCTTA.RACUSTTRXTYPENAME                          AS TIPO_DE_TRANSACCION,		
                APSA.ARPAYMENTSCHEDULECREATEDBY			  	AS TRANSACCION_CREADA_POR,	
                APSA.ARPAYMENTSCHEDULECREATIONDATE               AS FECHA_DE_CREACION_TRANSACCION,			
                APSA.ARPAYMENTSCHEDULETRXNUMBER 			     AS NUMERO_DE_TRANSACCION,                
                RCTLA.RACUSTOMERTRXLINEEXTENDEDAMOUNT 		     AS VALOR_PACTADO,
                APSA.ARPAYMENTSCHEDULEAMOUNTAPPLIED 			AS PAGO_REALIZADO,
                APSA.ARPAYMENTSCHEDULEDUEDATE 				AS FECHA_LIMITE_DE_PAGO,
                APSA.ARPAYMENTSCHEDULEAMOUNTLINEITEMSREMAINING 	AS VALOR_EN_MORA,
                APSA.ARPAYMENTSCHEDULESTATUS 				AS ESTADO_DE_TRANSACCION,
                APSA.ARPAYMENTSCHEDULEPAYMENTSCHEDULEID		AS SK_ESTADO_DE_CUENTA,                
                CASE 
                   WHEN HZP.PREFERREDCONTACTPERSONID IS NOT NULL THEN HZP.PREFERREDCONTACTPERSONID
                   ELSE HZCA.PARTYID
                END AS BK_PERSONA,			
                APSA.ARPAYMENTSCHEDULELASTUPDATEDATE             AS FECHA_ACTUALIZACION_PLAN_DE_PAGOS,
                RCTA.RACUSTOMERTRXCUSTOMERTRXID                  AS BK_COBRO,
                RCTA.RACUSTOMERTRXATTRIBUTE1  	               AS ENTIDAD_FINANCIERA,
                HZPS.SITEUSEID                                   AS BK_SITIO_CLIENTE,
                HZPS.PARTYSITENUMBER                             AS NUMERO_DE_SITIO,
                    CASE 
                    WHEN ARRAY_LENGTH(SPLIT(HZPS.PARTYSITENUMBER , '.')) >= 2 
                         THEN SPLIT(HZPS.PARTYSITENUMBER   , '.')[OFFSET(1)]
                    ELSE NULL
                    END AS BK_NUMERO_OP,       
                HZPS.CUSTOMERACCOUNTPEOCUSTACCOUNTID             AS BK_NUMERO_DE_CUENTA,
                RBSA.RABATCHSOURCENAME                           AS ORIGEN_DE_TRANSACCION
          FROM `RAW.FUSION_AR_PAYMENT_SCHEDULES_ALL` AS APSA
          LEFT JOIN `RAW.FUSION_RA_CUSTOMER_TRX_LINES_ALL` AS RCTLA ON APSA.ARPAYMENTSCHEDULECUSTOMERTRXID = RCTLA.RACUSTOMERTRXLINECUSTOMERTRXID
          LEFT JOIN `RAW.FUSION_RA_CUSTOMER_TRX_ALL` RCTA ON  RCTLA.RACUSTOMERTRXLINECUSTOMERTRXID = RCTA.RACUSTOMERTRXCUSTOMERTRXID
          LEFT JOIN `RAW.FUSION_HZ_CUST_ACCOUNTS` HZCA ON RCTA.RACUSTOMERTRXBILLTOCUSTOMERID = HZCA.CUSTACCOUNTID --OBTIENE LA CUENTA DEL CLIENTE
          LEFT JOIN `RAW.FUSION_HZ_PARTIES` HZP ON HZP.PARTYID = HZCA.PARTYID --OBTIENE EL CONTACTO ASOCIADO A LA CUENTA DEL CLIENTE
          LEFT JOIN `RAW.FUSION_HZ_CUST_SITE_USES_ALL` HZPS ON APSA.ARPAYMENTSCHEDULECUSTOMERSITEUSEID= HZPS.SITEUSEID
          LEFT JOIN `RAW.FUSION_RA_CUST_TRX_TYPES_ALL` RCTTA ON RCTA.RACUSTOMERTRXCUSTTRXTYPESEQID = RCTTA.RACUSTTRXTYPECUSTTRXTYPESEQID
          LEFT JOIN `RAW.FUSION_RA_BATCH_SOURCES_ALL` RBSA ON RCTA.RACUSTOMERTRXBATCHSOURCESEQID = RBSA.RABATCHSOURCEBATCHSOURCESEQID
          WHERE (RBSA.RABATCHSOURCENAME = 'PLAN DE PAGO' OR RBSA.RABATCHSOURCENAME = 'PLAN_DE_PAGOS_CARTERA')
                AND  RCTTA.RACUSTTRXTYPENAME NOT LIKE 'OTROS_INGRESOS_%'
                --AND RCTLA.RACUSTOMERTRXLINEDESCRIPTION  IS NOT NULL
                --AND  RCTTA.RACUSTTRXTYPENAME  NOT LIKE 'NC%'
                -- AND ARPAYMENTSCHEDULETRXNUMBER like '%190062%' --Condicion para pruebas con numero de OP                
          GROUP BY ARPAYMENTSCHEDULETRXNUMBER
                  ,SUBSTR(REPLACE(SUBSTR(APSA.ARPAYMENTSCHEDULETRXNUMBER,1,INSTR(APSA.ARPAYMENTSCHEDULETRXNUMBER,'.')), '.',''),1,3)
                  ,APSA.ARPAYMENTSCHEDULESTATUS
                  ,APSA.ARPAYMENTSCHEDULEAMOUNTAPPLIED
                  ,APSA.ARPAYMENTSCHEDULEDUEDATE
                  ,APSA.ARPAYMENTSCHEDULEAMOUNTLINEITEMSREMAINING
                  ,RCTLA.RACUSTOMERTRXLINEEXTENDEDAMOUNT
                  ,RCTLA.RACUSTOMERTRXLINEDESCRIPTION			
                  ,APSA.ARPAYMENTSCHEDULEPAYMENTSCHEDULEID                  
                  ,HZP.PREFERREDCONTACTPERSONID 
                  ,HZCA.PARTYID
                  ,APSA.ARPAYMENTSCHEDULELASTUPDATEDATE 
                  ,RCTA.RACUSTOMERTRXCUSTOMERTRXID
                  ,RCTA.RACUSTOMERTRXATTRIBUTE1   
                  ,HZP.IDENADDRPARTYSITEID  
                  ,HZP.IDENADDRPARTYSITEID
                  ,HZPS.PARTYSITENUMBER 
                  ,HZPS.SITEUSEID  
                  ,HZPS.CUSTOMERACCOUNTPEOCUSTACCOUNTID  
                  ,RCTTA.RACUSTTRXTYPENAME
                  ,RBSA.RABATCHSOURCENAME
                  ,APSA.ARPAYMENTSCHEDULECREATEDBY
                  ,APSA.ARPAYMENTSCHEDULECREATIONDATE
          ) AS T_1
          LEFT JOIN (--OBTIENE DEL MODULO DE COBRANZAS DE ORACLE EL ULTIMO SEGUIMIENTO(NOTA), PARA CADA CONCEPTO DEL CLIENTE
                    SELECT *
                    FROM (
                         SELECT aps.ARPAYMENTSCHEDULEPAYMENTSCHEDULEID
                                   ,aps.ARPAYMENTSCHEDULETRXNUMBER
                                   ,d.PARTY_ID
                                   ,d.CUST_ACCOUNT_ID
                                   ,d.CUSTOMER_SITE_USE_ID
                                   ,d.UNPAID_REASON_CODE  
                                   ,ilv.MEANING
                                   ,zn.NOTETXT
                                   ,zn.SOURCEOBJECTCODE
                                   ,zn.LASTUPDATEDBY
                                   ,zn.LASTUPDATEDATE
                                   ,ROW_NUMBER() OVER (PARTITION BY aps.ARPAYMENTSCHEDULETRXNUMBER ORDER BY LASTUPDATEDATE DESC) AS row_num
                         FROM `RAW.FUSION_AR_PAYMENT_SCHEDULES_ALL` aps
                         LEFT JOIN `RAW.FUSION_IEX_DELINQUENCIES_ALL` d ON aps.ARPAYMENTSCHEDULEPAYMENTSCHEDULEID = d.PAYMENT_SCHEDULE_ID
                         LEFT JOIN (
                                        SELECT DISTINCT LOOKUPCODE, MEANING, LOOKUPTYPE
                                        FROM `RAW.FUSION_FND_LOOKUP_VALUES_TL`
                                        WHERE LOOKUPTYPE = 'IEX_UNPAID_REASON' AND LANGUAGE = 'E'
                                        )ilv ON d.UNPAID_REASON_CODE = ilv.LOOKUPCODE  
                         LEFT JOIN `RAW.FUSION_ZMM_NOTES` zn ON zn.SOURCEOBJECTUID = CAST(aps.ARPAYMENTSCHEDULEPAYMENTSCHEDULEID AS STRING) 
                         --WHERE d.CUSTOMER_SITE_USE_ID = 300000690855562
                         ) as T0 
                    WHERE T0.row_num = 1
                    ) AS T_2 ON T_1.NUMERO_DE_TRANSACCION  = T_2.ARPAYMENTSCHEDULETRXNUMBER      
     WHERE   T_1.NUMERO_DE_TRANSACCION NOT LIKE 'NC%'
             AND NOT (T_1.PAGO_REALIZADO IS NULL AND T_1.ESTADO_DE_TRANSACCION = 'CL')-- Condición para excluir las filas que correspondan a versiones anteriores del plan de pagos.
             AND NOT (T_1.PAGO_REALIZADO = 0 AND T_1.ESTADO_DE_TRANSACCION = 'CL')
             AND T_1.CONCEPTO IS NOT NULL; --Excluye transacciones sin un concepto asignado por ejemplo en la OP 237189
            --T_1.BK_NUMERO_OP = '237189' AND
            -- AND T_1.NUMERO_DE_TRANSACCION = 'SUB 2.226652.9FE'
  
END;
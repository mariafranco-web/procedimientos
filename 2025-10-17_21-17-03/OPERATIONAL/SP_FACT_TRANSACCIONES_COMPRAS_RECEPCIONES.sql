CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_FACT_TRANSACCIONES_COMPRAS_RECEPCIONES()
BEGIN

	CREATE OR REPLACE TABLE amrl-data-prd.PRESENTATION.FACT_TRANSACCIONES_COMPRAS_RECEPCIONES PARTITION BY
   DATE(FH_CRE_RECEP)   AS 
  	/**************************** Tabla de hechos del modulo de compras recepciones, aca se relacionan las tablas requeridas de la capa RAW y las llaves necesarias para crear la tabla consolodada en en la capa de presentacion    ***************************/
	
		SELECT ---Informacion transacciones del modulo de Compras - Recepciones
			10096 APPLICATIONID,
			SHA256(CAST(RSH.SHIPMENTHEADERID AS STRING)) AS SK_IDORIGEN,
			RSH.SHIPMENTHEADERID AS  BK_IDORIGEN,
			SHA256(CAST(RSH.VENDORID AS STRING)) AS SK_TERCERO,
			RSH.VENDORID BK_TERCERO,
			RSH.RECEIPTNUM DOCUEMENTO,
			RSH.CREATIONDATE FH_CRE_RECEP
	FROM  amrl-data-prd.RAW.FUSION_RCV_SHIPMENT_HEADERS RSH;
END;
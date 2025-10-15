CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_DIM_INMUEBLES()
OPTIONS(
  description="Propósito: Crear y actualizar la tabla DIM_INVENTARIO, consolidando todos los atributos que corresponden al estado y características de un articulo(garaje, apto, bicicletero, local, oficina o casa) en el inventario se incluye los artículos de centro de acabados, No incluye materiales o servicios . \nAutor: Maria Fernanda Franco\nUsos: Tablero de Indicadores\nModificaciones: \n2025-01-28: Se incluye el campo OPTYID para identificar de forma precisa el valor de precio unitario de cada articulo\n2025-01-28: Se excluye en la clausula where la condición AND EIEB.ATTRIBUTE_CHAR4 <> 'CDA' \n2025-03-11: Se incluyen los campos ,ATTRIBUTE_DATE1 (FECHA_PROYECTADA_ESCRITURACION) , ATTRIBUTE_DATE2 (FECHA_PROYECTADA_CTO), ATTRIBUTE_DATE3 (FECHA_REAL_CTO) ,ATTRIBUTE_DATE7(HABILITADO_PARA_ENTREGA)\n2025-03-11: Se incluye la clausula  DELETED IS NULL para eliminar de la consulta todos los REVNID duplicados en cada actualizacion o visualizacion de la Oportunidad\n2025-07-09: Se incluye CASE en la linea 20 con el objetivo de obtener el PRECIO UNITARIO para los escenarios en los que el articulo nunca se ha vendido y no existe un precio en la tabla MOO_REVN.\n2025-07-09: Se ajusta la condición de la fila 63 para solo excluir el articulo en caso de que no tenga ninguno de los dos precios\n2025-07-09: se incluye la función COALESCE en las columnas ENCARGO_FIDUCIARIO, BK_OP y BK_PRECIO para que el merge funcione con normalidad en casos donde el dato viene nulo\n2025-07-18: Se incluye la columna EIEB.ATTRIBUTE_CHAR5 (TIPO_DE_ARTICULO)\n2025-08-14: SE incluye conversión a float para los campos ATTRIBUTE_NUMBER2 y ATTRIBUTE_NUMBER1\n2025-09-08: Se excluye condión de la linea 64\n2025-09-15: Se incluye la columna ESIB.ITEMBASEPEOLISTPRICEPERUNIT \n2025-10-02: Se ajustan nombres de las columnas ATTRIBUTE_DATE6(Fecha real Poliza Decenal), ATTRIBUTE_DATE7(Fecha Proyectada de habilitación), ATTRIBUTE_DATE5 (Fecha Proyectada de entrega editable), ATTRIBUTE_DATE4(Fecha Proyectada de Entrega) según cambios realizados en Oracle.\n2025-10-02: Se incluyen columnas ATTRIBUTE_DATE8, ATTRIBUTE_DATE9 y ATTRIBUTE_DATE10")
BEGIN

--1. CREANDO TABLA TEMPORAL
CREATE TEMP TABLE TEMP_INVENTARIO AS 
			( 
					SELECT EIEB.EFF_LINE_ID                           AS  SK_ARTICULO
                ,EIEB.ATTRIBUTE_CHAR1 											AS  BK_COD_TRANSACCIONAL
								,EIEB.INVENTORY_ITEM_ID  										AS  BK_ARTICULO
								,ESIB.ITEMBASEPEOITEMNUMBER 								AS  NOMBRE_ARTICULO
								,EIEB.CATEGORY_CODE 											  AS  CATEGORIA
								,EIEB.ATTRIBUTE_CHAR5 											AS  TIPO_DE_ARTICULO
								,EIEB.ATTRIBUTE_CHAR3 											AS  CLASE_DE_ARTICULO
								,EIEB.ATTRIBUTE_CHAR4 											AS  REFERENCIA
								,CAST(EIEB.ATTRIBUTE_NUMBER2 AS FLOAT64)		AS  AREA_CONSTRUIDA
								,CAST(EIEB.ATTRIBUTE_NUMBER1 AS FLOAT64)		AS  AREA_COMUN 
								,COALESCE(T1.REVENUEREVNID,0)							  AS  BK_PRECIO
								--,T1.REVENUEUPSIDEAMT											AS PRECIO_UNITARIO --Precio actual del articulo		
                --,ESIB.ITEMBASEPEOLISTPRICEPERUNIT         AS PRECIO_DE_LISTA --Precio con el que nace el articulo
                ,CASE --Este CASE cubre los escenarios en los que el articulo nunca se ha vendido y no existe un precio en la tabla MOO_REVN. **Es el precio tal como se ve en la OP
                  WHEN T1.REVENUEREVNID	IS NULL AND T1.REVENUEUPSIDEAMT	IS NULL THEN ESIB.ITEMBASEPEOLISTPRICEPERUNIT 
                  ELSE T1.REVENUEUPSIDEAMT	
                END AS PRECIO_UNITARIO			
								,ESIB.ITEMBASEPEOLISTPRICEPERUNIT           AS  PRECIO_DE_LISTA	--Es el mismo precio con el que nace el articulo en CPQ		
								,ESIB.ITEMBASEPEOINVENTORYITEMSTATUSCODE 		AS  ESTADO_DEL_ARTICULO
								,ESIB.ITEMBASEPEOCURRENTPHASECODE 					AS  FASE_CICLO_DE_VIDA
								,EIEB.ATTRIBUTE_CHAR6												AS  MATRICULA_INMOBILIARIA
								,COALESCE(T2.ATTRIBUTE_CHAR1,'0')						AS  ENCARGO_FIDUCIARIO
								,EIEB.ATTRIBUTE_DATE1 											AS  FECHA_PROYECTADA_ESCRITURACION 
								,EIEB.ATTRIBUTE_DATE2 											AS  FECHA_PROYECTADA_CTO  
								,EIEB.ATTRIBUTE_DATE3 											AS  FECHA_REAL_CTO 
								,EIEB.ATTRIBUTE_DATE4												AS  FECHA_PROYECTADA_DE_ENTREGA  
								,EIEB.ATTRIBUTE_DATE5												AS  FECHA_PROYECTADA_DE_ENTREGA_EDITABLE
								,EIEB.ATTRIBUTE_DATE6												AS  FECHA_REAL_POLIZA_DECENAL
								,EIEB.ATTRIBUTE_DATE7												AS  FECHA_PROYECTADA_DE_HABILITACION 
								,EIEB.ATTRIBUTE_DATE8												AS  INMUEBLE_CERRADO_OBRA
								,EIEB.ATTRIBUTE_DATE9												AS  INMUEBLE_HABILITADO_OBRA
								,EIEB.ATTRIBUTE_DATE10											AS  INMUEBLE_ENTREGADO_OBRA
								,COALESCE(T1.REVENUEOPTYID,0)							  AS  BK_OP
								,T1.REVENUEREVNLASTUPDATEDATE								AS  FECHA_ACTUALIZACION_PRECIO
								,CURRENT_DATETIME("America/Bogota")   			AS  FECHA_ACTUALIZACION_BQ
								,CURRENT_DATETIME("America/Bogota")   			AS  FECHA_CARGUE_BQ 								
					FROM `RAW.FUSION_EGO_ITEM_EFF_B` AS EIEB 
					LEFT JOIN `RAW.FUSION_EGP_SYSTEM_ITEMS_B` AS ESIB ON EIEB.INVENTORY_ITEM_ID = ESIB.ITEMBASEPEOINVENTORYITEMID
          LEFT JOIN (--T1 obtiene el precio unitario
										  SELECT 
												REVENUEINVENTORYITEMID    
												,REVENUEREVNID
												,REVENUEREVNLASTUPDATEDATE
												--,REVENUEUNITPRICE
												,REVENUEUPSIDEAMT
										     ,OPPORTUNITIESOPTYID
												,REVENUEOPTYID
											FROM `RAW.FUSION_MOO_REVN`    
											WHERE REVENUEINVENTORYITEMID IS NOT NULL 
														 AND DELETED IS NULL --Excluye duplicados
														 --AND OPPORTUNITIESOPTYID = 300000677454853
											ORDER BY REVENUEINVENTORYITEMID
                    ) AS T1 ON ESIB.ITEMBASEPEOINVENTORYITEMID = T1.REVENUEINVENTORYITEMID 
					LEFT JOIN (--T2 OBTIENE EL ENCARGO FIDUCIARIO (garantiza el llamado del ultimo encargo, incluso cuando el articulo cuenta con 3 encargos activos)
											SELECT *
											FROM (
													SELECT INVENTORY_ITEM_ID, 
																	ATTRIBUTE_CHAR1, 
																	ATTRIBUTE_DATE1 AS FECHA_DESISTIMIENTO_DEL_ENCARGO, 
																	ROW_NUMBER() OVER (PARTITION BY INVENTORY_ITEM_ID  ORDER BY ATTRIBUTE_CHAR1 DESC) AS row_num
													FROM `RAW.FUSION_EGO_ITEM_EFF_B`  
													WHERE CONTEXT_CODE = 'Encargos Fiduciarios' 
																AND ATTRIBUTE_DATE1 IS NULL																
														) AS T0
											WHERE T0.row_num = 1 
										) AS T2 ON EIEB.INVENTORY_ITEM_ID = T2.INVENTORY_ITEM_ID
			    WHERE	EIEB.CATEGORY_CODE = 'INMUEBLE' --Excluye las clases de articulos como: productos, servicios, materiales etc.
					      AND EIEB.CONTEXT_CODE = 'Inmuebles' --Excluye categorias de articulos como: UNIFER, Acabados, proyecto, Encargos fiduciarios
					     -- AND (T1.REVENUEREVNID	IS NOT NULL OR ESIB.ITEMBASEPEOLISTPRICEPERUNIT IS NOT NULL) --Excluye la fila que suma el total de articulo incluidos en la OP
					  	 --AND EIEB.ATTRIBUTE_CHAR4 <> 'CDA'  --Excluye el inventario de centro de acabados.		
							 --AND EIEB.EFF_LINE_ID  = 300000072379887
							 --AND T1.OPPORTUNITIESOPTYID = 300000688524190
							--AND ESIB.ITEMBASEPEOITEMNUMBER 	= 'PRVS20220-3-702-1' 
 			);


--1.1 CREAR TABLA PERMANENTE
--CREATE OR REPLACE TABLE `amrl-data-prd.PRESENTATION.DIM_INMUEBLES` AS
--SELECT * FROM TEMP_INVENTARIO;

--2. ACTUALIZAR TABLA DIM_INVENTARIO
MERGE `PRESENTATION.DIM_INMUEBLES` AS T 
USING (SELECT * FROM TEMP_INVENTARIO) AS S 
ON (T.SK_ARTICULO = S.SK_ARTICULO AND T.BK_PRECIO = S.BK_PRECIO AND T.PRECIO_UNITARIO = S.PRECIO_UNITARIO AND T.BK_OP = S.BK_OP AND T.ENCARGO_FIDUCIARIO = S.ENCARGO_FIDUCIARIO)
		WHEN MATCHED THEN
		UPDATE SET  T.`BK_COD_TRANSACCIONAL`   							= S.`BK_COD_TRANSACCIONAL` 
								,T.`BK_ARTICULO`								 				= S.`BK_ARTICULO`
								,T.`NOMBRE_ARTICULO`			 							= S.`NOMBRE_ARTICULO`
								,T.`CATEGORIA`					  				 			= S.`CATEGORIA`			
								,T.TIPO_DE_ARTICULO											= S.TIPO_DE_ARTICULO					
								,T.`CLASE_DE_ARTICULO`    				 			= S.`CLASE_DE_ARTICULO`
								,T.`REFERENCIA`           				 			= S.REFERENCIA
								,T.`AREA_CONSTRUIDA`							 			= S.`AREA_CONSTRUIDA`
								,T.AREA_COMUN														= S.AREA_COMUN
								,T.`BK_PRECIO`						 				 			= S.`BK_PRECIO`
								,T.`PRECIO_UNITARIO`			 				 			= S.`PRECIO_UNITARIO`
								,T.`PRECIO_DE_LISTA` 										= S.`PRECIO_DE_LISTA`			
								,T.`ESTADO_DEL_ARTICULO`	 				 			= S.`ESTADO_DEL_ARTICULO`
								,T.`FASE_CICLO_DE_VIDA`    				 			= S.`FASE_CICLO_DE_VIDA`
								,T.MATRICULA_INMOBILIARIA  				 			= S.MATRICULA_INMOBILIARIA
								,T.ENCARGO_FIDUCIARIO                   = S.ENCARGO_FIDUCIARIO
								,T.FECHA_PROYECTADA_ESCRITURACION 		  = S.FECHA_PROYECTADA_ESCRITURACION
								,T.FECHA_PROYECTADA_CTO  							  = S.FECHA_PROYECTADA_CTO
								,T.FECHA_REAL_CTO 											= S.FECHA_REAL_CTO
								,T.FECHA_PROYECTADA_DE_ENTREGA  				= S.FECHA_PROYECTADA_DE_ENTREGA
								,T.FECHA_PROYECTADA_DE_ENTREGA_EDITABLE = S.FECHA_PROYECTADA_DE_ENTREGA_EDITABLE
								,T.FECHA_REAL_POLIZA_DECENAL						= S.FECHA_REAL_POLIZA_DECENAL
								,T.FECHA_PROYECTADA_DE_HABILITACION     = S.FECHA_PROYECTADA_DE_HABILITACION
								,T.INMUEBLE_CERRADO_OBRA								= S.INMUEBLE_CERRADO_OBRA
								,T.INMUEBLE_HABILITADO_OBRA							= S.INMUEBLE_HABILITADO_OBRA
								,T.INMUEBLE_ENTREGADO_OBRA							= S.INMUEBLE_ENTREGADO_OBRA								
								,T.BK_OP				      					   			= S.BK_OP
								,T.FECHA_ACTUALIZACION_PRECIO           = S.FECHA_ACTUALIZACION_PRECIO
								,T.FECHA_ACTUALIZACION_BQ  				 			= S.FECHA_ACTUALIZACION_BQ
		WHEN NOT MATCHED THEN
		INSERT(
					SK_ARTICULO
					,BK_COD_TRANSACCIONAL
					,BK_ARTICULO
					,NOMBRE_ARTICULO
					,CATEGORIA
					,TIPO_DE_ARTICULO
					,CLASE_DE_ARTICULO
					,REFERENCIA
					,AREA_CONSTRUIDA
					,AREA_COMUN
					,BK_PRECIO
					,PRECIO_UNITARIO
					,PRECIO_DE_LISTA
					,ESTADO_DEL_ARTICULO
					,FASE_CICLO_DE_VIDA
					,MATRICULA_INMOBILIARIA
					,ENCARGO_FIDUCIARIO
					,FECHA_PROYECTADA_ESCRITURACION 
					,FECHA_PROYECTADA_CTO  
					,FECHA_REAL_CTO 
					,FECHA_PROYECTADA_DE_ENTREGA  
					,FECHA_PROYECTADA_DE_ENTREGA_EDITABLE
					,FECHA_REAL_POLIZA_DECENAL
					,FECHA_PROYECTADA_DE_HABILITACION 
					,INMUEBLE_CERRADO_OBRA
					,INMUEBLE_HABILITADO_OBRA
					,INMUEBLE_ENTREGADO_OBRA
					,BK_OP
					,FECHA_ACTUALIZACION_PRECIO
					,FECHA_ACTUALIZACION_BQ
					,FECHA_CARGUE_BQ
						)
			VALUES(
					S.SK_ARTICULO
					,S.BK_COD_TRANSACCIONAL
					,S.BK_ARTICULO
					,S.NOMBRE_ARTICULO
					,S.CATEGORIA
					,S.TIPO_DE_ARTICULO
					,S.CLASE_DE_ARTICULO
					,S.REFERENCIA
					,S.AREA_CONSTRUIDA
					,S.AREA_COMUN
					,COALESCE(S.BK_PRECIO,0)
					,S.PRECIO_UNITARIO
					,S.PRECIO_DE_LISTA
					,S.ESTADO_DEL_ARTICULO
					,S.FASE_CICLO_DE_VIDA
					,S.MATRICULA_INMOBILIARIA
					,COALESCE(S.ENCARGO_FIDUCIARIO,'0')
					,S.FECHA_PROYECTADA_ESCRITURACION 
					,S.FECHA_PROYECTADA_CTO  
					,S.FECHA_REAL_CTO 
					,S.FECHA_PROYECTADA_DE_ENTREGA  
					,S.FECHA_PROYECTADA_DE_ENTREGA_EDITABLE
					,S.FECHA_REAL_POLIZA_DECENAL
					,S.FECHA_PROYECTADA_DE_HABILITACION 
					,S.INMUEBLE_CERRADO_OBRA
					,S.INMUEBLE_HABILITADO_OBRA
					,S.INMUEBLE_ENTREGADO_OBRA					
					,COALESCE(S.BK_OP,0)
					,S.FECHA_ACTUALIZACION_PRECIO
					,S.FECHA_ACTUALIZACION_BQ
					,S.FECHA_CARGUE_BQ
		);
END;
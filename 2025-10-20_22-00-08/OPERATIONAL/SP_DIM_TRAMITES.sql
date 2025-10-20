CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_DIM_TRAMITES()
OPTIONS(
  description="Propósito: Crear y actualizar la tabla DIM_TRAMITES, consolidando por cada oportunidad la fecha de cada tramite que se requiere para completar  la escrituración. \nAutor: Maria Fernanda Franco")
BEGIN

    CREATE TEMP TABLE TEMP_TRAMITES AS 
    (
      SELECT ROW_NUMBER() OVER (ORDER BY MREX.ID)              AS ID_DIM_TRAMITES
            ,MREX.EXTN_ATTRIBUTE_NUMBER007                       AS INDICE
            ,MREX.ID                                             AS SK_TRAMITE
            ,MREX.EXTN_ATTRIBUTE_CHAR001                         AS CODIGO_DE_TRAMITE 
            ,MREX.EXTN_ATTRIBUTE_CHAR013                         AS DESCRIPCION_TRAMITE
            ,MREX.EXTN_ATTRIBUTE_CHAR006                         AS EJECUTOR_TRAMITE
            ,MREX.EXTN_ATTRIBUTE_TIMESTAMP002                    AS FECHA_CUMPLIMIENTO_TRAMITE
            ,MREX.EXTN_ATTRIBUTE_CHAR007                         AS OBSERVACIONES_TRAMITE
            ,MREX.ATTRIBUTE_CATEGORY                             AS CATEGORIA_TRAMITE
            ,REPLACE(MRE.EXTN_ATTRIBUTE_CHAR013, '_', ' ')       AS RESPONSABLE_TRAMITE
            ,CAST(MREX.SOURCE_ID1 AS INT64)                      AS BK_OP
            ,MREX.CREATED_BY 
            ,MREX.CREATION_DATE
            ,MREX.LAST_UPDATED_BY    
            ,CURRENT_DATETIME("America/Bogota")                  AS FECHA_ACTUALIZACION_BQ
            ,CURRENT_DATETIME("America/Bogota")                  AS FECHA_CARGUE_BQ
      FROM `RAW.FUSION_MOO_REF_ENTITIES_XMM` MREX
      LEFT JOIN `RAW.FUSION_MOT_REF_ENTITIES` MRE ON MREX.EXTN_ATTRIBUTE_CHAR001 = MRE.EXTN_ATTRIBUTE_CHAR014
      WHERE 	MREX.ATTRIBUTE_CATEGORY = 'OCS_Opty_Tramite_c'
      ORDER BY MREX.SOURCE_ID1, MREX.EXTN_ATTRIBUTE_NUMBER007 
    );


  --CREATE OR REPLACE TABLE PRESENTATION.DIM_TRAMITES AS 
  --SELECT * FROM TEMP_TRAMITES 

    MERGE   PRESENTATION.DIM_TRAMITES       AS T
    USING   (SELECT * FROM TEMP_TRAMITES)   AS S
    ON      (T.ID_DIM_TRAMITES = S.ID_DIM_TRAMITES)
    WHEN MATCHED THEN
    UPDATE SET  T.INDICE = S.INDICE,
                T.CODIGO_DE_TRAMITE = S.CODIGO_DE_TRAMITE,
                T.DESCRIPCION_TRAMITE = S.DESCRIPCION_TRAMITE,
                T.EJECUTOR_TRAMITE = S.EJECUTOR_TRAMITE,
                T.FECHA_CUMPLIMIENTO_TRAMITE = S.FECHA_CUMPLIMIENTO_TRAMITE,
                T.OBSERVACIONES_TRAMITE = S.OBSERVACIONES_TRAMITE,
                T.CATEGORIA_TRAMITE = S.CATEGORIA_TRAMITE,
                T.RESPONSABLE_TRAMITE = S.RESPONSABLE_TRAMITE,
                T.BK_OP = S.BK_OP,
                T.CREATION_DATE = S.CREATION_DATE,
                T.LAST_UPDATED_BY = S.LAST_UPDATED_BY,
                T.FECHA_ACTUALIZACION_BQ = CURRENT_DATETIME("America/Bogota")
    WHEN NOT MATCHED THEN
    INSERT  (   ID_DIM_TRAMITES,
                INDICE,
                SK_TRAMITE,
                CODIGO_DE_TRAMITE,
                DESCRIPCION_TRAMITE,
                EJECUTOR_TRAMITE,
                FECHA_CUMPLIMIENTO_TRAMITE,
                OBSERVACIONES_TRAMITE,
                CATEGORIA_TRAMITE,
                RESPONSABLE_TRAMITE,
                BK_OP,
                CREATED_BY,
                CREATION_DATE,
                LAST_UPDATED_BY,
                FECHA_ACTUALIZACION_BQ,
                FECHA_CARGUE_BQ
            )
    VALUES (    S.ID_DIM_TRAMITES,
                S.INDICE,
                S.SK_TRAMITE,
                S.CODIGO_DE_TRAMITE,
                S.DESCRIPCION_TRAMITE,
                S.EJECUTOR_TRAMITE,
                S.FECHA_CUMPLIMIENTO_TRAMITE,
                S.OBSERVACIONES_TRAMITE,
                S.CATEGORIA_TRAMITE,
                S.RESPONSABLE_TRAMITE,
                S.BK_OP,
                S.CREATED_BY,
                S.CREATION_DATE,
                S.LAST_UPDATED_BY,
                CURRENT_DATETIME("America/Bogota"),
                CURRENT_DATETIME("America/Bogota")
            );
END;
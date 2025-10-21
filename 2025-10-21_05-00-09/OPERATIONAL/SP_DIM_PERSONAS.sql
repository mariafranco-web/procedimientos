CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_DIM_PERSONAS()
OPTIONS(
  description="\nPropósito: Crear y actualizar la tabla DIM_PERSONAS, consolidando todos los atributos que corresponden a una persona con el perfil de vendedor, comprador, empleado o accionista. No incluye empresas pero si a sus representantes y accionistas.\nAutor: Maria Fernanda Franco\n26-03-2025: Se ajusta la condición para la columna teléfono, Antes: COALESCE(CONCAT(HZP.PRIMARYPHONEAREACODE,\" \",HZP.PRIMARYPHONENUMBER),'')\n28-03-2025: Se ajusta de Inner Join a Left Join la conexión con la tabla `amrl-data-prd.RAW.FUSION_HZ_PERSON_PROFILES`\n15-08-2025: Se comenta la linea 24 y se incluyen la linea 18 a la 24 para incluir el segundo apellido de los terceros\n16-10-2025: Se modifica el origen del TIPO_DE_DOCUMENTO antes HZPP.ATTRIBUTE3 ahora HZPP.EXTN_ATTRIBUTE_CHAR003\n16-10-2025: Se incluye conexión con la tabla td para homologar el código del tipo de documento con la clasificación del documento.")
BEGIN

CREATE TEMP TABLE TEMP_PERSONA AS (
      SELECT  SHA256(CAST(HZP.PARTYID AS STRING))     AS SK_PERSONA,
              HZP.PARTYID                             AS BK_PERSONA,
              HZP.PARTYTYPE                           AS TIPO_TERCERO,
              HZPP.ATTRIBUTE1                         AS CLASIFICACION_TERCERO,
              UPPER(td.MEANING)                       AS TIPO_IDENTIFICACION,
              CASE
                WHEN HZP.JGZZFISCALCODE IS NOT NULL AND HZPP.ATTRIBUTE2 IS NOT NULL THEN HZP.JGZZFISCALCODE
                WHEN HZP.JGZZFISCALCODE IS NULL     AND HZPP.ATTRIBUTE2 IS NOT NULL THEN HZPP.ATTRIBUTE2
                WHEN HZPP.ATTRIBUTE2    IS NULL     AND HZP.JGZZFISCALCODE IS NOT NULL THEN HZP.JGZZFISCALCODE
              END AS NUMERO_DE_IDENTIFICACION,
              HZPP.ATTRIBUTE4                        AS LUGAR_DE_EXPEDICION,
              CASE 
                  WHEN HZP.PARTYTYPE = 'ORGANIZATION' THEN HZP.PARTYUNIQUENAME 
                  ELSE CONCAT(
                              COALESCE(UPPER(HZPP.PERSONFIRSTNAME),'')," ", 
                              COALESCE(UPPER(HZPP.PERSONMIDDLENAME),'')," ",
                              COALESCE(UPPER(HZPP.PERSONLASTNAME),'')," ",
                              COALESCE(UPPER(HZPP.PERSONSECONDLASTNAME),'')
                            ) 
              END AS NOMBRE_COMPLETO,
              COALESCE(UPPER(HZPP.PERSONFIRSTNAME),'')         AS PRIMER_NOMBRE,
              COALESCE(UPPER(HZPP.PERSONMIDDLENAME),'')        AS SEGUNDO_NOMBRE,
              COALESCE(UPPER(HZPP.PERSONLASTNAME),'')          AS PRIMER_APELLIDO,
              COALESCE(UPPER(HZPP.PERSONSECONDLASTNAME),'')    AS SEGUNDO_APELLIDO,
              HZP.CREATIONDATE                                 AS FECHA_VINCULACION,
              HZPP.EXTNATTRIBUTETIMESTAMP001                   AS FECHA_ACTUALIZACION_SAGRILAFT,
              HZP.DATEOFBIRTH                                  AS FECHA_DE_NACIMIENTO,	          
              fl.MEANING                                       AS PAIS_DE_RESIDENCIA,
              HZP.CITY                                         AS CIUDAD_DE_RESIDENCIA,
              HZP.STATE                                        AS DEPARTAMENTO_DE_RESIDENCIA,
              HZP.STATUS                                       AS ESTADO_ACTIVACION,
              HZP.ADDRESS1                                     AS DIRECCION_RESIDENCIA,
              COALESCE(UPPER(HZP.EMAILADDRESS),'')                                                    AS EMAIL,
              CONCAT(COALESCE(PRIMARYPHONEAREACODE,'') ," ",COALESCE(PRIMARYPHONENUMBER),'')          AS TELEFONO, 
              HZPP.EXTNATTRIBUTECHAR017                                                               AS AUTORIZA_MENSAJES_DE_TEXTO,
              HZPP.EXTNATTRIBUTECHAR018                                                               AS AUTORIZA_CORREOS_ELECTRONICOS,
              HZP.GENDER                                                                              AS GENERO,
              fm.MEANING                                                                              AS ESTADO_CIVIL,
              HZPP.EXTNATTRIBUTECHAR020                                                               AS PROFESION,
              fo.MEANING                                                                              AS OCUPACION,
              HZPP.JOBTITLE                                                                           AS CARGO,
              HZPP.EXTNATTRIBUTECHAR021                                                               AS DONDE_LABORA,
              HZPP.EXTNATTRIBUTECHAR011                                                               AS TIEMPO_SERVICIO,
              HZPP.EXTNATTRIBUTENUMBER006                                                             AS INGRESO_MENSUAL,        
              HZPP.EXTNATTRIBUTENUMBER004                                                             AS EGRESOS_MENSUALES,
              HZPP.EXTNATTRIBUTENUMBER018                                                             AS TOTAL_ACTIVOS,
              HZPP.EXTNATTRIBUTENUMBER020                                                              AS TOTAL_PASIVOS,
              CASE
                --Para los accionistas y RL el atributo PEP se almacena en HZPP.ATTRIBUTE5 y no en HZPP.EXTN_ATTRIBUTE_CHAR009
                WHEN HZPP.EXTNATTRIBUTECHAR009 = 'N' AND HZPP.ATTRIBUTE5 = 'SI' THEN  'Y'
                ELSE HZPP.EXTNATTRIBUTECHAR009 
              END AS PEP,   
              HZPP.EXTNATTRIBUTECHAR010                                                     AS PUBLICAMENTE_EXPUESTO,
              HZPP.EXTNATTRIBUTECHAR014                                                     AS BANCO,
              HZPP.EXTNATTRIBUTECHAR015                                                     AS TIPO_DE_CUENTA,
              HZPP.EXTNATTRIBUTECHAR016                                                     AS CUENTA,
              HZPP.PRIMARYCUSTOMERID                                                        AS BK_PROVEEDOR,
              HZP.PREFERREDCONTACTPERSONID                                                  AS BK_COMPRADOR2,
              CURRENT_DATETIME("America/Bogota")                                            AS FECHA_ACTUALIZACION_BQ,
              CURRENT_DATETIME("America/Bogota")                                            AS FECHA_CARGUE_BQ
    FROM `amrl-data-prd.RAW.FUSION_HZ_PARTIES` AS HZP
    LEFT JOIN `amrl-data-prd.RAW.FUSION_HZ_PERSON_PROFILES` AS HZPP ON HZP.PARTYID = HZPP.PARTYID
    LEFT JOIN `amrl-data-prd.RAW.FUSION_FND_LOOKUP_VALUES_TL` fm ON fm.LOOKUPCODE = HZPP.MARITALSTATUS AND fm.SOURCELANG = 'US' and fm.LOOKUPTYPE = 'MARITAL_STATUS' and fm.language = 'E'
    LEFT JOIN `amrl-data-prd.RAW.FUSION_FND_LOOKUP_VALUES_TL` fo ON fo.LOOKUPCODE = HZPP.EXTNATTRIBUTECHAR023  AND fo.SOURCELANG = 'US' and fo.LOOKUPTYPE = 'OCS_OCUPACION' and fo.language = 'E'
    LEFT JOIN `amrl-data-prd.RAW.FUSION_FND_LOOKUP_VALUES_TL` fl ON fl.LOOKUPCODE = HZP.COUNTRY AND fl.LOOKUPTYPE = 'JEES_EURO_COUNTRY_CODES' AND fl.language = 'E'    
    LEFT JOIN `amrl-data-prd.RAW.FUSION_FND_LOOKUP_VALUES_TL` td ON HZPP.EXTNATTRIBUTECHAR003 = td.LOOKUPCODE AND td.LOOKUPTYPE = 'OCS_DOCUMNTTYPE' AND td.language = 'E' 
    --WHERE HZP.PARTYID IN (300000070584678,300000528541415,300000132264316) 
    ORDER BY HZPP.ATTRIBUTE1 DESC    

);

--CREANDO TABLA X PRIMERA VEZ
--CREATE OR REPLACE TABLE `PRESENTATION.DIM_PERSONA` AS 
--SELECT * FROM TEMP_PERSONA;

--MERGE
MERGE  PRESENTATION.DIM_PERSONA AS T
USING (SELECT * FROM TEMP_PERSONA) AS S
  ON (T.BK_PERSONA = S.BK_PERSONA)
WHEN MATCHED THEN
  UPDATE SET 
    T.TIPO_TERCERO = S.TIPO_TERCERO, 
    T.CLASIFICACION_TERCERO = S.CLASIFICACION_TERCERO, 
    T.TIPO_IDENTIFICACION = S.TIPO_IDENTIFICACION, 
    T.NUMERO_DE_IDENTIFICACION = S.NUMERO_DE_IDENTIFICACION, 
    T.LUGAR_DE_EXPEDICION= S.LUGAR_DE_EXPEDICION, 
    T.NOMBRE_COMPLETO = S.NOMBRE_COMPLETO, 
    T.PRIMER_NOMBRE = S.PRIMER_NOMBRE, 
    T.SEGUNDO_NOMBRE = S.SEGUNDO_NOMBRE, 
    T.PRIMER_APELLIDO = S.PRIMER_APELLIDO, 
    T.SEGUNDO_APELLIDO = S.SEGUNDO_APELLIDO,
    T.FECHA_VINCULACION = S.FECHA_VINCULACION, 
    T.FECHA_ACTUALIZACION_SAGRILAFT = S.FECHA_ACTUALIZACION_SAGRILAFT, 
    T.FECHA_DE_NACIMIENTO = S.FECHA_DE_NACIMIENTO, 
    T.PAIS_DE_RESIDENCIA = S.PAIS_DE_RESIDENCIA, 
    T.CIUDAD_DE_RESIDENCIA = S.CIUDAD_DE_RESIDENCIA,
    T.DEPARTAMENTO_DE_RESIDENCIA = S.DEPARTAMENTO_DE_RESIDENCIA,
    T.ESTADO_ACTIVACION = S.ESTADO_ACTIVACION,
    T.DIRECCION_RESIDENCIA = S.DIRECCION_RESIDENCIA,
    T.EMAIL = S.EMAIL,
    T.TELEFONO = S.TELEFONO,
    T.AUTORIZA_MENSAJES_DE_TEXTO = S.AUTORIZA_MENSAJES_DE_TEXTO,
    T.AUTORIZA_CORREOS_ELECTRONICOS = S.AUTORIZA_CORREOS_ELECTRONICOS,
    T.ESTADO_CIVIL = S.ESTADO_CIVIL,
    T.PROFESION = S.PROFESION,
    T.OCUPACION = S.OCUPACION,
    T.CARGO = S.CARGO,
    T.DONDE_LABORA = S.DONDE_LABORA,
    T.TIEMPO_SERVICIO = S.TIEMPO_SERVICIO,
    T.INGRESO_MENSUAL = S.INGRESO_MENSUAL,
    T.EGRESOS_MENSUALES = S.EGRESOS_MENSUALES,
    T.TOTAL_ACTIVOS = S.TOTAL_ACTIVOS,
    T.TOTAL_PASIVOS = S.TOTAL_PASIVOS,
    T.PUBLICAMENTE_EXPUESTO = S.PUBLICAMENTE_EXPUESTO,
    T.BANCO = S.BANCO,
    T.TIPO_DE_CUENTA = S.TIPO_DE_CUENTA,
    T.CUENTA = S.CUENTA,
    T.BK_PROVEEDOR = S.BK_PROVEEDOR,   
    T.BK_COMPRADOR2 = S.BK_COMPRADOR2,
    T.FECHA_ACTUALIZACION_BQ = CURRENT_DATETIME("America/Bogota")
WHEN NOT MATCHED THEN
  INSERT (
          SK_PERSONA,
          BK_PERSONA,          
          TIPO_TERCERO,
          CLASIFICACION_TERCERO,
          TIPO_IDENTIFICACION,
          NUMERO_DE_IDENTIFICACION,
          LUGAR_DE_EXPEDICION,
          NOMBRE_COMPLETO,
          PRIMER_NOMBRE,
          SEGUNDO_NOMBRE,
          PRIMER_APELLIDO,
          SEGUNDO_APELLIDO,
          FECHA_VINCULACION,
          FECHA_ACTUALIZACION_SAGRILAFT,
          FECHA_DE_NACIMIENTO,
          PAIS_DE_RESIDENCIA,
          CIUDAD_DE_RESIDENCIA,
          DEPARTAMENTO_DE_RESIDENCIA,
          ESTADO_ACTIVACION,
          DIRECCION_RESIDENCIA,
          EMAIL,
          TELEFONO, 
          AUTORIZA_MENSAJES_DE_TEXTO,
          AUTORIZA_CORREOS_ELECTRONICOS,
          GENERO,ESTADO_CIVIL,
          PROFESION,
          OCUPACION,
          CARGO,
          DONDE_LABORA,
          TIEMPO_SERVICIO,
          INGRESO_MENSUAL,
          EGRESOS_MENSUALES,
          TOTAL_ACTIVOS,
          TOTAL_PASIVOS,PEP,
          PUBLICAMENTE_EXPUESTO,
          BANCO,TIPO_DE_CUENTA,
          CUENTA,
          BK_PROVEEDOR,
          BK_COMPRADOR2,
          FECHA_ACTUALIZACION_BQ,
          FECHA_CARGUE_BQ
)
  VALUES (
          S.SK_PERSONA,
          S.BK_PERSONA,
          S.TIPO_TERCERO,
          S.CLASIFICACION_TERCERO,
          S.TIPO_IDENTIFICACION,
          S.NUMERO_DE_IDENTIFICACION,
          S.LUGAR_DE_EXPEDICION,
          S.NOMBRE_COMPLETO,
          S.PRIMER_NOMBRE,
          S.SEGUNDO_NOMBRE,
          S.PRIMER_APELLIDO,
          S.SEGUNDO_APELLIDO,
          S.FECHA_VINCULACION,
          S.FECHA_ACTUALIZACION_SAGRILAFT,
          S.FECHA_DE_NACIMIENTO,
          S.PAIS_DE_RESIDENCIA,
          S.CIUDAD_DE_RESIDENCIA,
          S.DEPARTAMENTO_DE_RESIDENCIA,
          S.ESTADO_ACTIVACION,
          S.DIRECCION_RESIDENCIA,
          S.EMAIL,
          S.TELEFONO, 
          S.AUTORIZA_MENSAJES_DE_TEXTO,
          S.AUTORIZA_CORREOS_ELECTRONICOS,
          S.GENERO,
          S.ESTADO_CIVIL,
          S.PROFESION,
          S.OCUPACION,
          S.CARGO,
          S.DONDE_LABORA,
          S.TIEMPO_SERVICIO,
          S.INGRESO_MENSUAL,
          S.EGRESOS_MENSUALES,
          S.TOTAL_ACTIVOS,
          S.TOTAL_PASIVOS,PEP,
          S.PUBLICAMENTE_EXPUESTO,
          S.BANCO,TIPO_DE_CUENTA,
          S.CUENTA,
          S.BK_PROVEEDOR,
          S.BK_COMPRADOR2,
          S.FECHA_ACTUALIZACION_BQ,
          S.FECHA_CARGUE_BQ
);

END;
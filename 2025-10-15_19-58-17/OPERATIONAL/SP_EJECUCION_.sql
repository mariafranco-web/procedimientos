CREATE PROCEDURE `amrl-data-prd`.OPERATIONAL.SP_EJECUCION_()
BEGIN
    -- Asegurar que la tabla de logs existe
   /* CREATE OR REPLACE TABLE `amrl-data-prd.OPERATIONAL.LOGS_SP_EJECUCION` (
        log_id STRING,  -- Usaremos UUID para ID único
        sp_name STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        duration_seconds FLOAT64,
        status STRING,
        message STRING
    );*/

    -- Lista de procedimientos a ejecutar
    DECLARE sp_list ARRAY<STRING>;
    DECLARE start_time TIMESTAMP;
    DECLARE end_time TIMESTAMP;
    DECLARE duration FLOAT64;
    DECLARE error_message STRING;
 
    SET sp_list = [
        
        '`amrl-data-prd.OPERATIONAL.SP_DIM_COBRO`();',
        'CALL `amrl-data-prd.OPERATIONAL.SP_DIM_TIEMPO`();'
    ];

    -- Iterar sobre cada procedimiento
    FOR i IN (SELECT * FROM UNNEST(sp_list)) DO
      
        SELECT i;
        SET start_time = CURRENT_TIMESTAMP();
    

        BEGIN
            -- Ejecutar procedimiento
            EXECUTE  IMMEDIATE FORMAT('''
BEGIN
  CALL `amrl-data-prd.OPERATIONAL.%s`();
END;
''', i.f0_);


            -- Calcular fin y duración
            SET end_time = CURRENT_TIMESTAMP();
            SET duration = TIMESTAMP_DIFF(end_time, start_time, SECOND);

            -- Insertar log de éxito
            INSERT INTO `amrl-data-prd.OPERATIONAL.LOGS_SP_EJECUCION` (log_id, sp_name, start_time, end_time, duration_seconds, status, message)
            VALUES (GENERATE_UUID(), i, start_time, end_time, duration, 'ÉXITO', 'Procedimiento ejecutado correctamente');

        EXCEPTION WHEN ERROR THEN
            -- Capturar fin y duración en caso de error
            SET end_time = CURRENT_TIMESTAMP();
            SET duration = TIMESTAMP_DIFF(end_time, start_time, SECOND);
            SET error_message = (
                                    SELECT ARRAY_TO_STRING(
                                        ARRAY(SELECT TO_JSON_STRING(t) FROM UNNEST(@@error.stack_trace) AS t),
                                        '; '
                                    )
                                );

            SELECT error_message;    
            

            -- Insertar log de error
            INSERT INTO `amrl-data-prd.OPERATIONAL.LOGS_SP_EJECUCION` (log_id, sp_name, start_time, end_time, duration_seconds, status, message)
            VALUES (GENERATE_UUID(), i.f0_, start_time, end_time, duration, 'ERROR', error_message);
        END;
    END FOR;
END;
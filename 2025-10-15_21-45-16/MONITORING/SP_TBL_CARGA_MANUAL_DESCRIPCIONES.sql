CREATE PROCEDURE `amrl-data-prd`.MONITORING.SP_TBL_CARGA_MANUAL_DESCRIPCIONES()
BEGIN

CREATE OR REPLACE TABLE `amrl-data-prd.MONITORING.TBL_CARGA_MANUAL_DESCRIPCIONES`
(

DATASET STRING,
TABLA STRING,
COLUMNA STRING,
DESCRIPCION STRING
);



INSERT INTO `amrl-data-prd.MONITORING.TBL_CARGA_MANUAL_DESCRIPCIONES` (DATASET, TABLA, COLUMNA, DESCRIPCION)
VALUES
('CONSUMPTION','OBT_GESTION_INMUEBLES','FECHA_PROYECTADA_DE_ENTREGA','FECHA_PROYECTADA_DE_ENTREGA: Corresponde a la fecha planeada por el equipo de obra para la entrega del inmueble'),
('CONSUMPTION','OBT_GESTION_INMUEBLES','FECHA_PROYECTADA_DE_ENTREGA_EDITABLE','FECHA_PROYECTADA_DE_ENTREGA_EDITABLE: Es la nueva fecha de entega del inmueble modificada, por cambios, retrasos en la planeacion de obra'),
('CONSUMPTION','OBT_GESTION_INMUEBLES','FECHA_PROYECTADA_DE_HABILITACION','FECHA_PROYECTADA_DE_HABILITACION: Corresponde a la fecha planeada por el equipo de obra para la habilitación del inmueble'),
('CONSUMPTION','OBT_GESTION_INMUEBLES','INMUEBLE_CERRADO_OBRA','INMUEBLE_CERRADO_OBRA: Fecha en la que el inmueble finalizo el proceso constructivo'),
('CONSUMPTION','OBT_GESTION_INMUEBLES','INMUEBLE_HABILITADO_OBRA','INMUEBLE_HABILITADO_OBRA: Fecha en la que el equipo de obra confirma que el inmueble tiene areas comunes listas, ha pasado por control de calidad y se encuentra limpio y listo para entregar'),
('CONSUMPTION','OBT_GESTION_INMUEBLES','INMUEBLE_ENTREGADO_OBRA','INMUEBLE_ENTREGADO_OBRA: Fecha en la que el equipo de obra confirma la entrega oficial del inmueble')

;

END;
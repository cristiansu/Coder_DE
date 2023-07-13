# Coder_DE

## Objetivo

ETL que extrae datos desde API pública en la web, luego los carga, primero en una bd de datos local Postgresql para posteriormente cargar en Redshift.

### Primera Parte

* Extracción de data desde API web.
* Carga de data extraída en bd local postgres

### Segunda Parte

* Transformación tabla, selección de columnas. Luego carga de la tabla transformada en redshift
* Transformación tabla para generar una segunda tabla, reemplazo de comas por puntos en columnas de precios, uso de CAST para convertir object a float, agregación COUNT y Average, agrupación group by. Carga de esta segunda tabla transformada en redshift.

### Tercera Parte

* Uso de Docker y Airflow

#### Cuarta Parte

* Envío aviso notificación por email proceso ETL ok

## Etapas e imágenes

* Extracción data desde API web. Se convierte data a dataframe con pandas y luego se pasa a SQL postres local. El código está en archivo Etl1.py
![My Image](codigoPy_carga_en_postgre.png)

* Carga de la data en bd local Postgresql
![My Image](carga_en_postgre.png)

* Cargar data desde bd local postresql a Redshift, tabla transformada seleccionando sólo columnas de interés. El código está en archivo Etl2.py
![My Image](etl_cargaRedshift_ok.png)

* Primera tabla cargada en redshift. El código está en archivo Etl2.py
![My Image](redshift_img_datosCargados.png)

* Segunda tabla cargada en redshift, complementando también se incluye imágen de la segunda tabla en consola postgres. El código está en archivo Etl2.py
![My Image](tabla_transform_redshift.png)

![My Image](tabla_transform_postgre.png)

* Uso de Docker y Airflow

Primer intento (arrojó el error el primer intento ---- debo corregir )
![My Image](DCompose_fail.png)

Segundo intento corrección del fail
![My Image](img_22jun/AirflowDags.png)

Imagenes tareas y log de Airflow

![My Image](img_22jun/af_dag1.png)
![My Image](img_22jun/af_dag1_log.png)
![My Image](img_22jun/af_dag2.png)
![My Image](img_22jun/af_dag2_log.png)

* Envio Notificación Email

![My Image](fotos_proy/airflow-menu.png)

![My Image](fotos_proy/envio_mail_flujo.png)

![My Image](fotos_proy/log_envio_mail_ok.png)

![My Image](fotos_proy/img_correo_recibido.png)

![My Image](fotos_proy/log_etl_2.png)
  

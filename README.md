# Coder_DE

## Objetivo

ETL que extrae datos desde API, los carga primero en una bd de datos local Postgresql para posteriormente cargar en Redshift.

## Etapas

* Extracción data desde API. Se convierte data a dataframe con pandas y luego se pasa a SQL. El código está en archivo Etl1.py
![My Image](codigoPy_carga_en_postgre.png)

* Carga de la data en bd local Postgresql
![My Image](carga_en_postgre.png)

* Cargar copia de la data desde bd local postresql a Redshift (en desarrollo, falta hacer corrección). El código está en archivo Etl2.py
![My Image](error_redshift_int.png)

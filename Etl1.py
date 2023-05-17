import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import pyodbc
import os


def extraer_api():
    try:
        #api entrega detalle de estaciones de servicio combustibles en españa
        url_api="https://geoportalgasolineras.es/resources/files/preciosEESS_es.xls"
        df=pd.read_excel(url_api, skiprows=3)
        cargar_postgres(df)
    except Exception as e:
        print(f'Error de extracción datos: {str(e)}')

#datos bd postgres
bd_postgre='Combustibles_Esp'
user='postgres'
password='Wallace709***'
host='127.0.0.1'
port=5432
con_string=f'postgresql://{user}:{password}@{host}:5432/{bd_postgre}'
tabla='Combustibles'

def cargar_postgres(df):
    try:
        rows_imported = 0
        engine = create_engine(con_string)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tabla}')
        # guardar df en postgres
        df.to_sql(f'stg_{tabla}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Datos cargados en postgres")
    except Exception as e:
        print("Error en carga de datos: " + str(e))

try:
    #llamar función extraer desde api
    extraer_api()
except Exception as e:
    print("Error durante la extracción de la data: " + str(e))

import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import pyodbc
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime


# argumentos por defecto para el DAG
default_args = {
    'owner': 'CristianSU',
    'start_date': datetime(2023,6,20),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}


BC_dag = DAG(
    dag_id='Combustibles_ETL',
    default_args=default_args,
    description='Información combustibles España',
    schedule_interval="@daily",
    catchup=False
)


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

# try:
#     #llamar función extraer desde api
#     extraer_api()
# except Exception as e:
#     print("Error durante la extracción de la data: " + str(e))


# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data_api',
    python_callable=extraer_api,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

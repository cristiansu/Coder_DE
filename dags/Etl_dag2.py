import pandas as pd
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import psycopg2
import pyodbc
import os
import redshift_connector
from psycopg2.extras import execute_values
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

#para email
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.encoders import encode_base64
from email.mime.text import MIMEText
import smtplib
from datetime import date




# argumentos por defecto para el DAG
default_args = {
    'owner': 'CristianSU',
    'start_date': datetime(2023,7,13),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}


BC_dag = DAG(
    dag_id='Combustibles_ETL_Rsf',
    default_args=default_args,
    description='Información combustibles España en Redshift',
    schedule_interval="@daily",
    catchup=False
)





#datos bd postgres
bd_postgre='Combustibles_Esp'
user_pg='postgres'
password_pg='Wallace709***'
host_pg='127.0.0.1'
port_pg=5432
#con_string=f'postgresql://{user}:{password}@{host}:5432/{bd_postgre}'
#tabla='stg_Combustibles' es la tabla original, la primera cargada
tabla='stg_Combustibles_gby_localidad'

def extraer_desde_postgres():
    try:
        #extracción de datos desde postgresql
        # engine = create_engine(con_string)
        # conex=engine.connect()
        # df=pd.read_sql("SELECT * FROM \"stg_Combustibles\"",conex)

        conn_pg = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host_pg, port_pg, bd_postgre, user_pg, password_pg))
        #sql = 'SELECT "Provincia","Localidad","Longitud","Latitud","Precio gasolina 98 E5" as Bencina98,"Precio gasóleo A" as Diesel,"Rótulo" as Concesionario  FROM public."stg_Combustibles"'
        sql2= '''SELECT t."Provincia",AVG(t.Bencina98) as "PromBencina98",AVG(t.Diesel) as "PromDiesel" 
        from (SELECT "Provincia","Localidad","Longitud","Latitud",CAST(REPLACE("Precio gasolina 98 E5",',','.') AS float) as Bencina98, CAST(REPLACE("Precio gasóleo A",',','.') AS float) as Diesel,"Rótulo" as Concesionario from public."stg_Combustibles" where "Precio gasolina 98 E5" is not null and "Precio gasóleo A" is not null) as t 
        group by t."Provincia"'''
        df = sqlio.read_sql_query(sql2, conn_pg)
        

        connx=conexion_rf()
        cargar_en_redshift(connx,tabla,df)
        conn = None
    except Exception as e:
        print(f'Error de extracción datos postgres: {str(e)}')


#redshift

host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user="saavedra_cristian_coderhouse"
pwd="rHojN35Xd0"

def conexion_rf():
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Conectado a redshift ok!")
        
    except Exception as e:
        print(f"Error de conexión a Redshift. Tipo error : {str(e)}")
    
    return conn


def cargar_en_redshift(conn, table_name, dataframe):
    try:
        dtypes= dataframe.dtypes
        cols= list(dtypes.index )
        tipos= list(dtypes.values)
        #type_map = {'integer':'INT','int64': 'INT','int32': 'INT','float64': 'FLOAT','object': 'VARCHAR(300)','bool':'BOOLEAN'}
        type_map = {'object': 'VARCHAR(300)','int64':'INT','integer':'INT','float64': 'FLOAT'}
        sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
        # Definir formato SQL VARIABLE TIPO_DATO
        column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
        # Combine column definitions into the CREATE TABLE statement
        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(column_defs)}
            );
            """
        # Crear la tabla
        cur = conn.cursor()
        cur.execute(table_schema)
        # Generar los valores a insertar
        values = [tuple(x) for x in dataframe.to_numpy()]
        # Definir el INSERT
        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
        # Execute the transaction to insert the data
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        print('Proceso terminado, carga redshift ok')
    except Exception as e:
        print(f'Error dato, tipo: {str(e)}')


def envia_mail():
	hoy = date.today()
	remitente='testingcgc@gmail.com'
	asunto='Aviso Proceso ETL'+'  '+str(hoy)
	texto_correo='''Aviso Proceso ETL Ok!'''
	
	mensaje=MIMEMultipart()
	mensaje['From']=remitente
	mensaje['Subject']=asunto
	mensaje.attach(MIMEText(texto_correo, 'plain'))
	
	destinatario=['casu709@gmail.com']
	mensaje['To']=', '.join(destinatario)
		
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.starttls()
	server.login('testingcgc@gmail.com', 'cwiekjerevntmgef')
	texto=mensaje.as_string()
	server.sendmail('testingcgc@gmail.com', destinatario, texto)
	server.quit()

# try:
#     #llamar función extraer desde postgres que luego activará la carga en redshift
#     #conexion_rf()
#     extraer_desde_postgres()
    
# except Exception as e:
#     print(f"Error proceso Etl : {str(e)}")



# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data_api',
    python_callable=extraer_desde_postgres,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

##2. Enviar mail
task_2 = PythonOperator(
    task_id='enviar_mail',
    python_callable=envia_mail,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

task_1 >> task_2
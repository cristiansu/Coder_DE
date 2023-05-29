import pandas as pd
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import psycopg2
import pyodbc
import os
import redshift_connector
from psycopg2.extras import execute_values


#datos bd postgres
bd_postgre='Combustibles_Esp'
user_pg='postgres'
password_pg='Wallace709***'
host_pg='127.0.0.1'
port_pg=5432
#con_string=f'postgresql://{user}:{password}@{host}:5432/{bd_postgre}'
tabla='stg_Combustibles'



conn_pg = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host_pg, port_pg, bd_postgre, user_pg, password_pg))
#sql = 'SELECT * FROM public."stg_Combustibles"'
#df = sqlio.read_sql_query(sql, conn_pg)

sql = 'SELECT public.Provincia,public.Municipio,public.Localidad FROM public."stg_Combustibles"'
df = sqlio.read_sql_query(sql, conn_pg)

print(df.head())
print(df.info())
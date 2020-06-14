from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import codecs
import os
import logging
import pymssql
from airflow.hooks.base_hook import BaseHook

def create_connection():
    c= BaseHook.get_connection('mssql_pi') 
    return c

def   read_csv(location):
     import pandas as pd
     #Leo el archivo CSV
     df = pd.read_csv(location)    

     #Seteo fecha y hora a la columna FECHA_COPIA
     df["FECHA_COPIA"] = pd.to_datetime('now')

     #Elimino posibles espacios 
     df["ID"] = df['ID'].str.strip()
     df["RESULTADO"] = df['RESULTADO'].str.strip()
    
     #Muestro las 10 primeras filas del Archivo
     row = df.head()
     print('Muestro las primeras lineas del archivo ')
     print(row)

     # Imprimo cantidad filas del archivos
     row = df.shape
     print('La cantidad de filas es de: ',row[0],' y la cantidad de columnas es de: ', row[1]   )

     #Creo archivo de parametros sql a insertar
     data = [tuple(x) for x in df.values]
     return data

def execute_insert():
    #Creo Connexión con mssql
    c = create_connection()
    conn = pymssql.connect(c.host,c.login,c.password,c.schema)
    cursor = conn.cursor()

    #Creo tabla temporal en mssql
    cursor.execute ('SELECT * INTO #TEMPORAL FROM UNIFICADO WHERE 1=2')

    #Inserto el archivo en la tabla temporal
    sql = "insert into #TEMPORAL values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"      
    data =  read_csv('/usr/local/airflow/csv/nuevas_filas.csv')
    cursor.executemany(sql, data)

    #Verifico la cantidad de filas insertadas
    cursor.execute('select COUNT(*) from #TEMPORAL')
    row = cursor.fetchone()
    print('La cantidad de registros en la tabla "ŦEMPORAL" es de: ',row[0])

    #Verifico la cantidad de filas de la tabla UNIFICADO
    cursor.execute('select COUNT(*) from UNIFICADO')
    row = cursor.fetchone()
    print('La cantidad de registros en la tabla "UNIFICADO" es de: ',row[0])

    #Cuento los registros duplicados de la tabla UNIFICADO
    cursor.execute('select COUNT(*) from #TEMPORAL t, UNIFICADO u where t.id = u.id and t.muestra = u.muestra and t.resultado = u.resultado')
    row = cursor.fetchone()
    print('La cantidad de registros duplicados es de: ',row[0])

    #Elimino los duplicados de la tabla Unificado
    cursor.execute('delete UNIFICADO from Unificado u inner JOIN #TEMPORAL t on  t.id = u.ID and t.muestra = u.MUESTRA and t.resultado = u.RESULTADO')
    print('Eliminó')
    
    #Cuento la cantidad de registros en la tabla Unificado (Post delete)    
    cursor.execute('select COUNT(*) from UNIFICADO')
    row = cursor.fetchone()
    print('La cantidad de registros de la tabla "UNIFICADO" luego de eliminar repetidos es de: ',row[0])
    #Inserto los valores de la tabla TEMPORAL a la Tabla UNIFICADO    
    cursor.execute('insert into UNIFICADO select * from #TEMPORAL')
    print('Insertó')
    
    #Cuento la cantidad de registro final
    cursor.execute('select COUNT(*) from UNIFICADO')
    row = cursor.fetchone()
    print('La cantidad de registros en la tabla "UNIFICADO" es de: ',row[0])

    #Commiteo
    conn.commit()

    #Cierro la conexión
    conn.close()
    return row

default_dag_args = {
    'start_date': datetime(2020, 6, 8,5),
    'email': ['gastonfortuny@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'project_id' : 'pi_challenge',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('pi_ETL',
    schedule_interval = timedelta(days=7),
    catchup = False,
    default_args=default_dag_args) as dag:

    t_start = DummyOperator(task_id='start')
    t_execute_insert = PythonOperator(task_id='execute_insert', python_callable=execute_insert, dag=dag)
    t_end = DummyOperator(task_id='end')
    t_start >> t_execute_insert >> t_end


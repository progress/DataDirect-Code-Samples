from datetime import timedelta
 
import airflow
import os
import csv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.jdbc_hook import JdbcHook

#Creating JDBC connection using Conn ID
JdbcConn = JdbcHook(jdbc_conn_id='Redshift')
def getconnection():
    JdbcConn.get_connection('Redshift')
    print("connected")
def writerrecords():
    id=JdbcConn.get_records(sql="SELECT * FROM customer")
    with open('records.csv', 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(id)
def displyrecords():
    with open('records.csv', 'rt')as f:
        data = csv.reader(f)
        for row in data:
            print(row)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
 
dag = DAG(
    'datadirect_sample', default_args=default_args,
    schedule_interval="@daily")
 
t1 = PythonOperator(
    task_id='getconnection',
    python_callable=getconnection,
    dag=dag,
)
t2 = PythonOperator(
    task_id='WriteRecords',
    python_callable=writerrecords,
    dag=dag,
)
 
t3 = PythonOperator(
    task_id='DisplayRecords',
    python_callable=displyrecords,
    dag=dag,
)
 
t1 >> t2 >> t3
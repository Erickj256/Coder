from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from clima import climaCDMX

default_args={
    'owner': 'Erick',
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    default_args=default_args,
    dag_id='CargaDatos',
    description= 'Se carga la información a RS',
    start_date=datetime(2023,8,19,2),
    schedule_interval='@daily'
    ) as dag:

    task1 = PythonOperator(
        task_id='climaID',
        python_callable=climaCDMX,)

task1 

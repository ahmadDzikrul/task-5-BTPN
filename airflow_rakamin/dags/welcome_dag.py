from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import os

# import subprocess
# from uji import run_ssis
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
# Tentukan parameter alur kerja
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inisialisasi DAG
dag = DAG(
    'example_ssis_dag',
    default_args=default_args,
    description='Contoh Alur Kerja SSIS',
    schedule_interval=timedelta(days=1),  # Atur interval sesuai kebutuhan
)

# Tentukan fungsi untuk menjalankan paket SSIS
def transform_kolom():
    
    df = pd.read_csv(AIRFLOW_HOME+'/dags/final2.csv')
    def create_is_attrited(row):
        if row['status'] == 'Existing Customer':
            return 0
        else:
            return 1

    df['is_attrited'] = df.apply(create_is_attrited, axis=1)
    df.to_csv(AIRFLOW_HOME+'/dags/final3.csv')

# Tentukan tugas Python Operator
run_transform_task = PythonOperator(
    task_id='transform_kolom',
    python_callable=transform_kolom,
    provide_context=True,
    dag=dag,
)

# Tentukan urutan tugas
run_transform_task
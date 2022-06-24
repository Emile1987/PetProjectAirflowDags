import os
import datetime as dt
import json
import pandas as pd
import pyarrow as pa
from datetime import date
from airflow.models import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.hive.operators.hive import HiveOperator


default_args = {
    'owner': 'Emil_Egiazarov',
    'start_date': dt.datetime(2022, 6, 6),
    'retries': 1,
}


def check_completed_DAG():
    fs = pa.hdfs.connect()
    path = '/user/hduser/bored/bored_final/'
    obj_list = fs.ls(path, True)
    for i in obj_list:
        if i['name'] == '/user/hduser/bored/bored_final/_SUCCESS.txt':
            return True
    return False


def on_failure_callback(context):
    ti = context['task_instance']
    print(f'task {ti.task_id} failed in dag {ti.dag_id}')


with DAG(dag_id='create_dataset_final_version', schedule_interval=None, default_args=default_args, catchup=False) as dag:

    check_DAG = PythonSensor(
        task_id='hdfs_check_DAG',
        poke_interval=120,
        timeout=30,
        mode='reschedule',
        python_callable=check_completed_DAG,
        on_failure_callback=on_failure_callback,
        soft_fail=True
    )

    create_dataset_inlocal = BashOperator(
        task_id='create_dataset_inlocal',
        bash_command='''beeline -u jdbc:hive2://localhost:10000 \
        -e 'select * from bored WHERE id=1;' | sed 's/[[:space:]]\+/,/g' > /home/hduser/dataset.csv && \
        echo /home/hduser/dataset.csv'''
    )

    send_result_telegram = TelegramOperator(
        task_id='send_info_telegram',
        telegram_conn_id='telegram_conn_id',
        chat_id='1289818711',
        text='''DAG create_dataset was completed:
        {{ ti.xcom_pull(task_ids='create_dataset_inlocal', key='return_value') }}''',
    )

    check_DAG >> create_dataset_inlocal >> send_result_telegram
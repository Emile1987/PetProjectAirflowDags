import os
import datetime as dt
import json
import pandas as pd
import pyarrow as pa
from datetime import date
from airflow.models import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.decorators import dag, task


default_args = {
    'owner': 'Emil_Egiazarov',
    'start_date': dt.datetime(2022, 3, 3),
    'retries': 1,
}


def send_telegram_ms():
    tg_hook = TelegramHook(telegram_conn_id='telegram_conn_id', chat_id='1289818711')
    tg_hook.send_message({'text': 'DAG collect_data_dug was completed earlier'})


def send_telegram_finally_ms():
    tg_hook = TelegramHook(telegram_conn_id='telegram_conn_id', chat_id='1289818711')
    tg_hook.send_message({'text': 'DAG collect_data_dug was completed'})


def load_data_to_hive(**kwargs):
    print({kwargs['ti'].xcom_pull(task_ids='move_csv_data_inhadoop', key='return_value')})
    hive_hook = HiveCliHook()
    hive_hook.run_cli(f'''LOAD DATA INPATH '{kwargs['ti'].xcom_pull(task_ids='move_csv_data_inhadoop', key='return_value')}' INTO TABLE bored;''')


def create_hive_table():
    hive_hook = HiveCliHook()
    hive_hook.run_cli('''
            CREATE TABLE IF NOT EXISTS bored ( ID INT, activity STRING, type STRING, participants INT, price DOUBLE, 
            link STRING, key STRING, accessibility DOUBLE)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            TBLPROPERTIES('skip.header.line.count'='1');''')


def drop_table():
    hive_hook = HiveCliHook()
    hive_hook.run_cli('DROP TABLE bored;')


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


def download_data(**kwargs):
    path_dir = kwargs['ti'].xcom_pull(task_ids='move_tmpdata_tolocal', key='return_value')
    print(path_dir)
    name = date.today()
    tmp_list = []
    for file_name in os.listdir(path_dir):
        with open(os.path.join(path_dir, file_name), 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            tmp_list.append(json_data)
    df = pd.DataFrame(tmp_list)
    df.to_csv(get_path(f'{name}.csv'))
    file_csv = f'{name}.csv'
    kwargs['ti'].xcom_push(key='path_file', value=file_csv)


def check_count_files():
    fs = pa.hdfs.connect()
    path = '/user/hduser/bored/bored_tmp'
    obj_list = fs.ls(path, True)
    count = len(obj_list)
    return count >= 0


def on_failure_callback(context):
    ti = context['task_instance']
    print(f'task {ti.task_id} failed in dag {ti.dag_id}')


def check_completed_dag():
    fs = pa.hdfs.connect()
    path = '/user/hduser/bored/bored_final/'
    obj_list = fs.ls(path, True)
    for i in obj_list:
        if i['name'] == '/user/hduser/bored/bored_final/_SUCCESS.txt':
            return 'send_info_telegram'
    return 'move_tmpdata_tolocal'


with DAG(dag_id='collect_data_dug_version_API_1_hooks', schedule_interval=None, default_args=default_args,
         catchup=False) as dag:

    hdfs_check = PythonSensor(
        task_id='hdfs_check_task',
        poke_interval=120,
        timeout=30,
        mode='reschedule',
        python_callable=check_count_files,
        on_failure_callback=on_failure_callback,
        soft_fail=True
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=check_completed_dag,
    )

    move_tmpdata_tolocal = BashOperator(
        task_id='move_tmpdata_tolocal',
        bash_command='''
            hdfs dfs -get /user/hduser/bored/bored_tmp/* /home/hduser/bored/ && \
            hdfs dfs -rm /user/hduser/bored/bored_tmp/* && \
            echo "/home/hduser/bored/"
            '''
    )

    collect_data_tofile_inlocal = PythonOperator(
        task_id="collect_data_tofile_inlocal",
        python_callable=download_data,
    )

    move_csv_data_inhadoop = BashOperator(
        task_id='move_csv_data_inhadoop',
        bash_command='''BORED_FILE='{{ ti.xcom_pull(task_ids='collect_data_tofile_inlocal', key='path_file') }}' && \
                    hdfs dfs -put /home/hduser/$BORED_FILE /user/hduser/bored/bored_final/ && \
                    rm /home/hduser/bored/* && \
                    rm /home/hduser/$BORED_FILE && \
                    echo "/user/hduser/bored/bored_final/$BORED_FILE"
                    '''
    )

    with TaskGroup("prepare_table") as prepare_table:
        drop_hive_table = PythonOperator(
            task_id='drop_hive_table',
            python_callable=drop_table,
        )

        create_hive_table = PythonOperator(
            task_id='create_hive_table',
            python_callable=create_hive_table,
        )

        drop_hive_table >> create_hive_table

    load_data_hive = PythonOperator(
        task_id='load_data_to_hive',
        python_callable=load_data_to_hive,
    )

    create_file_SUCCESS = BashOperator(
        task_id='create_file_SUCCESS',
        bash_command='''hdfs dfs -touchz  /user/hduser/bored/bored_final/_SUCCESS.txt'''
    )

    send_finally_result_telegram = PythonOperator(
        task_id='send_finally_info_telegram',
        python_callable=send_telegram_finally_ms,
    )

    send_result_telegram = PythonOperator(
        task_id='send_info_telegram',
        python_callable=send_telegram_ms,
    )

hdfs_check >> branch_task >> move_tmpdata_tolocal >> collect_data_tofile_inlocal >> prepare_table >> \
move_csv_data_inhadoop >> load_data_hive >> create_file_SUCCESS >> send_finally_result_telegram

hdfs_check >> branch_task >> send_result_telegram

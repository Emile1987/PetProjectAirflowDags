import datetime as dt
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator


default_args = {
    'owner': 'Emil_Egiazarov',
    'start_date': dt.datetime(2022, 6, 8),
    'retries': 1,
}


with DAG(dag_id='download_data_dug_final_version', schedule_interval=None, default_args=default_args, catchup=False) as dag:

    download_data_tohive = BashOperator(
        task_id='download_data_tohive',
        bash_command='''BORED_FILE="{{ execution_date.int_timestamp }}.json" && \
        wget -q http://www.boredapi.com/api/activity/ -O $BORED_FILE && \
        hdfs dfs -put $BORED_FILE /user/hduser/bored/bored_tmp && \
        rm $BORED_FILE
        ''',
    )

    send_result_telegram = TelegramOperator(
        task_id='send_info_telegram',
        telegram_conn_id='telegram_conn_id',
        chat_id='1289818711',
        text='''DAG download_data_dug was completed''',
    )


download_data_tohive >> send_result_telegram
"""Даг прогрузки слоя DDS в DWH."""
import pendulum
from airflow.providers.vertica.operators.vertica import VerticaOperator

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from airflow.decorators import task


connection_dwh = 'vertica_dwh'
sql_dir = Variable.get('sql_load_cdm')


args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        'load-cdm',
        catchup=True,
        default_args=args,
        description='Load cdm from staging layer.',
        is_paused_upon_creation=True,
        start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
        end_date=pendulum.datetime(2022, 11, 1, tz="UTC"),
        schedule_interval='@daily',
        tags=['load', 'cdm']
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    files = (
        'dm_global_metrics.sql',
    )

    tasks = []
    for file in files:
        t_query = VerticaOperator(
            task_id=file[:-4],
            sql=open(f"{sql_dir}/{file}", encoding='utf8').read(),
            vertica_conn_id=connection_dwh,
            dag=dag
        )
        tasks.append(t_query)

    start >> tasks[0] >> end

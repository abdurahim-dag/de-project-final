"""Даг прогрузки слоя DDS в DWH."""
import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator


connection_dwh = 'vertica_dwh'
sql_dir = Variable.get('sql_load_dds')
sql_files = Variable.get('sql_load_dds_files', deserialize_json=True)

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        'load-dds',
        catchup=True,
        default_args=args,
        description='Load dds from staging layer.',
        is_paused_upon_creation=True,
        start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
        end_date=pendulum.datetime(2022, 11, 1, tz="UTC"),
        schedule_interval='@daily',
        tags=['load', 'dds']
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    tasks = []
    for file in sql_files:
        with open(f"{sql_dir}/{file}", encoding='utf8') as f:
            sql = f.read()
            t_query = VerticaOperator(
                task_id=file[:-4],
                sql=sql,
                vertica_conn_id=connection_dwh,
                dag=dag
            )
            tasks.append(t_query)

    start >> tasks >> end

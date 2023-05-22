import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from staging.loader import Loader
from staging.models import Currency
from staging.models import Transaction


CONNECTION_SRC = 'postgres'
CONNECTION_DST = 'vertica_dwh'

# Переменная пути откуда будем брать шаблон запроса.
VARIABLE_SQL_PATH = 'sql_load_staging'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        'load-messages-to-staging',
        catchup=True,
        default_args=args,
        description='Load messages from postgres to vertica.',
        is_paused_upon_creation=True,
        schedule_interval='@daily',
        start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
        end_date=pendulum.datetime(2022, 11, 1, tz="UTC"),
        tags=['load', 'staging']
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task()
    def staging_load(model, src_sql, dst_sql, object_type, dst_table_name, ds) -> None:
        """Функция загрузки."""
        sql_dir = Variable.get(VARIABLE_SQL_PATH)
        dfmt = pendulum.from_format(ds, 'YYYY-MM-DD')

        loader = Loader(
            src_con_id=CONNECTION_SRC,
            src_query_path=f"{sql_dir}/{src_sql}",
            dst_con_id=CONNECTION_DST,
            dst_query_path=f"{sql_dir}/{dst_sql}",
            model=model,
            object_type=object_type,
            dst_table_name=dst_table_name,
            date_from=dfmt
        )
        loader.load()


    parameters = [
        ('transactions', Transaction, 'stg_src_pg.sql', 'stg_dst_vertica.sql', 'TRANSACTION', 'transactions'),
        ('currencies', Currency, 'stg_src_pg.sql', 'stg_dst_vertica.sql', 'CURRENCY', 'currencies'),
    ]

    groups = []
    for name, model, src_sql, dst_sql, object_type, dst_table_name in parameters:

        with TaskGroup(group_id=name) as tg:

            t_staging_load = staging_load(
                model, src_sql, dst_sql, object_type, dst_table_name, ds='{{ds}}'
            )

        groups.append(tg)

    start >> [tg for tg in groups] >> end
"""Класс исполнитель загрузки данных из PG в Vertica."""
import dataclasses
import io
import logging
from contextlib import contextmanager
from typing import Generator
from typing import Type
from typing import Union

import pendulum
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import vertica_python
from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook

from .backoff import on_exception
from .models import Currency
from .models import Transaction
from .storage import WorkflowStorage


class Loader:
    LAST_LOADED_KEY = "last_loaded_offset"
    BATCH_LIMIT = 100000

    def __init__(
            self,
            src_con_id: str,
            dst_con_id: str,
            src_query_path: str,
            dst_query_path: str,
            model: Union[Type[Transaction], Type[Currency]],
            object_type: str,
            dst_table_name: str,
            date_from: pendulum.DateTime
    ) -> None:
        """ Инициализация стартовых параметров.
        :param src_con_id: Название соединения источника.
        :param src_query_path: Название переменной, где лежит запрос на выгрузку данных.
        :param dst_con_id: Название соединения DWH.
        :param dst_query_path: Название переменной, где лежит запрос на загрузку данных.
        :param object_type: Тип выгружаемого объекта.
        :param dst_table_name: Название таблицы в получателе.
        :param date_from: Дата за которую выгружаем запись из источника.
        :param model: Модель, которой должна соответствовать запись из источника.
        """
        self._src_con_id = src_con_id
        self._src_query_path = src_query_path
        self._dst_con_id = dst_con_id
        self._dst_query_path = dst_query_path
        self._model = model
        self._object_type = object_type
        self._dst_table_name = dst_table_name
        self._date_from = date_from

        # Объект, для доступа к сохранению и извлечению из хранилища состояния процесса выгрузки.
        self.wf_storage = WorkflowStorage(
            conn_id=src_con_id, # название соединения в Airflow БД состояния
            etl_key=f"{object_type}-{date_from.to_date_string()}", # Ключ идентификации ETL процесса в БД прогресса
            workflow_settings={ # Значение по умолчанию начального состояния прогресса.
                self.LAST_LOADED_KEY: 0,
            },
            schema='public' # Схема в БД, где хранится состояние
        )

    @on_exception(
        exception=psycopg2.DatabaseError,
        start_sleep_time=1,
        factor=2,
        border_sleep_time=15,
        max_retries=15,
    )
    @contextmanager
    def _get_src_conn(
            self,
    ) -> Generator[psycopg2.extensions.connection, None, None]:
        logging.info('Using connection ID %s for source.', self._src_con_id)
        hook = PostgresHook(postgres_conn_id=self._src_con_id)
        conn = hook.get_conn()
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
        try:
            conn.autocommit = False
            yield conn
        finally:
            conn.close()

    @on_exception(
        exception=vertica_python.Error,
        start_sleep_time=1,
        factor=2,
        border_sleep_time=15,
        max_retries=15,
    )
    @contextmanager
    def _get_dst_conn(
            self,
    ) -> Generator[vertica_python.Connection, None, None]:
        logging.info('Using connection ID %s for destination.', self._dst_con_id)
        conn = Connection.get_connection_from_secrets(self._dst_con_id)
        conn_info = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "database": conn.schema,
            'autocommit': False,
        }

        if not conn.port:
            conn_info["port"] = 5433
        else:
            conn_info["port"] = int(conn.port)

        yield vertica_python.connect(**conn_info)

    def load(self):
        src_query = open(self._src_query_path, encoding='utf-8').read()
        dst_query = open(self._dst_query_path, encoding='utf-8').read()

        wf_setting = self.wf_storage.retrieve_state()

        while True:
            # Считываем последний прогресс.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_KEY]
            logging.info(f"Last loaded - {last_loaded}")

            # Параметры к шаблону запроса.
            parameters={
                'threshold': last_loaded,
                'limit': self.BATCH_LIMIT,
                'object_type': self._object_type,
                'date_from': self._date_from.to_date_string()
            }
            # Считываем данные из источника.
            with self._get_src_conn() as src_conn:
                src_curs: psycopg2.extensions.cursor
                with src_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as src_curs:
                    src_curs.execute(src_query, parameters)
                    load_queue = src_curs.fetchall()

            try:
                i = 0
                file_in_memory = io.BytesIO()
                for obj in load_queue:

                    try:
                        m = self._model(**obj['payload'])
                    except TypeError as e:
                        logging.info(f"Объект не соответствует модели {obj}")
                        continue

                    line = tuple(getattr(m, field.name) for field in dataclasses.fields(m))
                    file_in_memory.writelines([bytes(','.join(line) + '\n', 'utf-8')])
                    i += 1

                if i > 0:
                    file_in_memory.seek(0)

                    with self._get_dst_conn() as dst_conn:
                        dst_curs = dst_conn.cursor()
                        sql = dst_query % {
                            'table_name': self._dst_table_name,
                            'column_names': ','.join((field.name for field in dataclasses.fields(self._model)))
                        }
                        dst_curs.execute(sql, copy_stdin=file_in_memory, buffer_size=65536)
                        dst_conn.commit()

                    logging.info('Rows loaded: %s', i)

                    wf_setting.workflow_settings[self.LAST_LOADED_KEY] += len(load_queue)
                    self.wf_storage.save_state(wf_setting)

            finally:
                file_in_memory.close()

            if not load_queue:
                break

        logging.info("Quitting.")

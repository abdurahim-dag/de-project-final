import json

import psycopg
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg.rows import class_row

from .utils import MyEncoder
from .base import BaseStorage
from .model import Workflow


class WorkflowStorage(BaseStorage):
    """Класс извлечения и сохранения состояния прогресса."""
    def __init__(
            self,
            conn_id: str,
            etl_key: str,
            workflow_settings: dict,
            schema: str
    ):
        self.conn_id = conn_id
        self.etl_key = etl_key
        self.workflow_settings = workflow_settings
        self.schema = schema

    def save_state(self, state: Workflow) -> None:
        hook = PostgresHook(self.conn_id)
        uri = hook.get_uri()
        conn: psycopg.Connection
        curs: psycopg.Cursor
        with psycopg.connect(uri) as conn:
            with conn.cursor() as curs:
                curs.execute(
                    f"""
                        INSERT INTO {self.schema}.srv_wf_settings(workflow_key, workflow_settings)
                        VALUES (%(etl_key)s, %(etl_setting)s)
                        ON CONFLICT (workflow_key) DO UPDATE
                        SET workflow_settings = EXCLUDED.workflow_settings;
                    """,
                    {
                        "etl_key": self.etl_key,
                        "etl_setting": json.dumps(
                            state.workflow_settings,
                            cls=MyEncoder,
                            sort_keys=True,
                            ensure_ascii=False
                        )

                    },
                )
                conn.commit()

    def retrieve_state(self) -> Workflow:
        hook = PostgresHook(self.conn_id)
        uri = hook.get_uri()
        conn: psycopg.Connection
        curs: psycopg.Cursor
        obj = None
        with psycopg.connect(uri) as conn:
            with conn.cursor(row_factory=class_row(Workflow)) as curs:
                curs.execute(
                    f"""
                        SELECT
                            id,
                            workflow_key,
                            workflow_settings
                        FROM {self.schema}.srv_wf_settings
                        WHERE workflow_key = %(etl_key)s;
                    """,
                    {"etl_key": self.etl_key},
                )
                obj = curs.fetchone()

        if obj is None:
            obj = Workflow(id=0, workflow_key=self.etl_key, workflow_settings=self.workflow_settings )
        return obj

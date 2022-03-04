from operator import itemgetter
from typing import List

import pyodbc
import pandas as pd

from dek.utils import Timer, LogMixin
from dek.models import SQLExecutor


class HiveExecutor(SQLExecutor, LogMixin):
    """
    A wrapper class that manages Hive SQL creation/execution
    """

    def __init__(self, connection: pyodbc.Connection):
        self.connection = connection

    def __call__(self, sql: str):
        self.sql = sql
        return self

    @property
    def statements(self) -> List[str]:
        try:
            return self.sql.split(';')
        except AttributeError:
            raise AttributeError('"sql" attribute is not set. Please call HiveExecutor instance with sql statements.')

    def execute(self, return_cursor=False) -> pyodbc.Cursor:
        with self.connection.cursor() as cursor:
            for statement in self.statements:
                if not statement:
                    continue

                with Timer(logger=self.logger.info, text=f"Elapsed time: {{:0.4f}} seconds"):
                    statement = statement.strip()
                    self.logger.info(f"Executing statement:")
                    self.logger.info(f'\n\n{statement}\n')

                    cursor.execute(statement)

            if return_cursor:
                return cursor

    def fetch_df(self) -> pd.DataFrame:
        with self.execute(return_cursor=True) as cursor:
            data = cursor.fetchall()
            col_names = [itemgetter(0)(col_def).split('.', 1)[-1] for col_def in cursor.description]

            self.logger.info(f'cursor description: {cursor.description}')
            self.logger.info(f'column names: {col_names}')

            if data:
                return pd.DataFrame([tuple(r) for r in data], columns=col_names)
            else:
                return pd.DataFrame(columns=col_names)

    def fetch_result(self, callback):
        callback(self.fetch_df())

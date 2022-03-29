import logging
from typing import List, Optional
from functools import reduce
import operator

import pandas as pd
from pyspark.sql import SparkSession, DataFrame

from dek.utils import Timer, LogMixin
from dek.models.sql import SQLExecutor

__all__ = (
    'SparkSQLExecutor',
    'execute_sql',
)

_logger = logging.getLogger(__name__)


def _indented(snippet: str) -> str:
    raw_lines = snippet.split('\n')
    indented_lines = map(lambda line: f'\n\t{line}', raw_lines)

    return reduce(operator.add, indented_lines)


class SparkSQLExecutor(SQLExecutor, LogMixin):
    def __init__(self, spark: SparkSession, sql: str):
        self.spark = spark
        self.sql = sql

    @property
    def statements(self) -> List[str]:
        return self.sql.split(';')

    def execute(self) -> Optional[pd.DataFrame]:
        for statement in self.statements:
            _statement = statement.strip()

            if not _statement:
                continue

            with Timer(
                log=self.logger.info,
                text=f"Elapsed time: {{:0.4f}} seconds",  # noqa: F541
            ):
                self.logger.info(
                    f"Executing statement:\n{_indented(_statement)}\n"
                )
                result_df = self.spark.sql(sqlQuery=_statement)

        if result_df:
            result_df.printSchema()
            return result_df.toPandas()

    def fetch_df(self) -> pd.DataFrame:
        return self.execute()


def execute_sql(
    spark: SparkSession, sql: str, logger=_logger.info
) -> DataFrame:
    if logger:
        logger(f"Execute sql:\n{_indented(sql)}")

    return spark.sql(sql)

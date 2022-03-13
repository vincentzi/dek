from typing import Dict

from pyspark.sql import SparkSession, DataFrame

from dek.utils.log import LogMixin


class CsvReadable(LogMixin):
    """
    options: Spark csv read options
    """

    options: Dict[str, str] = dict()
    
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def Path(self, path: str) -> 'CsvReadable':
        self._path = path
        return self

    def read(self) -> DataFrame:
        self.logger.info(f"Reading csv files from {self.path}")
        return self.spark.read \
            .options(**self.options) \
            .csv(path=self._path)


class JsonReadable(LogMixin):
    """
    options: Spark json read options
    """

    options: Dict[str, str] = dict()

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def Path(self, path: str) -> 'JsonReadable':
        self._path = path
        return self

    def read(self) -> DataFrame:
        self.logger.info(f"Reading json files from {self.path}")
        return self.spark.read \
            .options(**self.options) \
            .json(path=self._path)

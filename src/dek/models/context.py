import reprlib
from typing import Optional
from pyspark.sql import SparkSession

from .generics import GenericContextTracker


class Context:
    """
    An base class that wraps SparkSession and other configurations.

    The actual application context should inherit this class and provide configuration details specific to its domain.
    """

    def __init__(self, spark: Optional[SparkSession] = None):
        self._spark = spark

    def __repr__(self):
        return f'{self.__class__.__name__}(spark={reprlib.repr(self._spark)}, settings={reprlib.repr(self.settings)})'

    def __enter__(self):
        ContextTracker.push_context(context=self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ContextTracker.pop_context()

    @property
    def spark(self) -> SparkSession:
        return self._spark

    @spark.setter
    def spark(self, other: SparkSession):
        self._spark = other

    @property
    def settings(self):
        """
        Subclass should provide implementation that returns an object to work with global/env configurations.

        Such object should support access notation such as:
            settings.WEB_SERVER_URL
            settings.HIVE_DEFAULT_DB
            settings.HIVE_DEFAULT_DB_HDFS_ROOT
            ...
        """
        raise NotImplementedError

    @property
    def hive_table_prefix(self) -> str:
        raise NotImplementedError

    @property
    def hive_default_db(self) -> str:
        raise NotImplementedError

    @property
    def hive_db_hdfs_dir(self) -> str:
        raise NotImplementedError

    @property
    def default_file_format(self) -> str:
        raise NotImplementedError


class ContextTracker(GenericContextTracker[Context]):
    """ Context injection utility """

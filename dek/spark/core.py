import os
import reprlib
from typing import Callable, Iterable, List


from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import col
from pyspark import SparkConf

from dek.utils import LogMixin

from .config import SparkConfig, DEFAULT_SPARK_CONFIG

__all__ = (
    'get_spark_session',
    'SparkMixin',
)


def get_hadoop_spark_session(app_name='triangletask', spark_config: SparkConfig = DEFAULT_SPARK_CONFIG) -> SparkSession:
    os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf/"

    spark_conf = SparkConf()

    spark_conf.setAppName(app_name)
    spark_conf.set("spark.shuffle.service.enabled", True)
    spark_conf.set("spark.dynamicAllocation.enabled", True)
    spark_conf.set("spark.dynamicAllocation.minExecutors", spark_config.min_executors)

    spark_conf.set("spark.driver.memory", spark_config.driver_memory)
    spark_conf.set("spark.driver.cores", spark_config.driver_cores)
    spark_conf.set("spark.executor.memory", spark_config.executor_memory)
    spark_conf.set("spark.executor.cores", spark_config.executor_cores)
    spark_conf.set("spark.executor.instances", spark_config.executor_instances)

    spark_conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark_conf.set("hive.exec.dynamic.partition", "true")
    spark_conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark_conf.set("spark.sql.parquet.compression.codec", "snappy")

    spark_conf.set("spark.sql.execution.arrow.enabled", "true")

    spark_conf.set("spark.sql.shuffle.partitions", spark_config.shuffle_partitions)

    # Enable Spark to read Hive subdirectory
    spark_conf.set("spark.sql.hive.convertMetastoreParquet", False)
    spark_conf.set("spark.sql.hive.convertMetastoreOrc", False)
    spark_conf.set("mapred.input.dir.recursive", True)
    spark_conf.set("spark.sql.parquet.binaryAsString", True)

    spark_conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark


get_spark_session = get_hadoop_spark_session


class SparkMixin(LogMixin):
    app_name = None

    def __init__(self, spark: SparkSession = None, spark_config: SparkConfig = None, **kwargs):
        super().__init__(**kwargs)

        if spark:
            self.logger.info(f'Using an existing SparkSession {reprlib.repr(spark)}.')
            self.spark = spark
        else:
            self._spark_config = spark_config or DEFAULT_SPARK_CONFIG
            self._spark = self._get_spark_session(self._spark_config)
            self.logger.info(f'Created a new SparkSession {reprlib.repr(self._spark)}.')

        self._patch_transform()
        self._patch_withCustomColumn()

    @property
    def spark(self) -> SparkSession:
        return self._spark

    @spark.setter
    def spark(self, val: SparkSession):
        self._spark = val

    @staticmethod
    def inspect_df(df: DataFrame):
        df.printSchema()
        df.show(truncate=False)

    def _get_spark_session(self, spark_config: SparkConfig) -> SparkSession:
        _app_name = self.__class__.__name__
        return get_spark_session(app_name=_app_name, spark_config=spark_config)

    def _patch_transform(self):
        """
        Monkey patch .transform function for Spark 2. It's not needed for Spark 3

        Example usage:
            def with_greeting(df):
                return df.withColumn("greeting", lit("hi"))
            def with_something(df, something):
                return df.withColumn("something", lit(something))

            data = [("jose", 1), ("li", 2), ("liz", 3)]
            source_df = spark.createDataFrame(data, ["name", "age"])
            actual_df = (source_df
                .transform(lambda df: with_greeting(df))
                .transform(lambda df: with_something(df, "crazy")))

            print(actual_df.show())
            +----+---+--------+---------+
            |name|age|greeting|something|
            +----+---+--------+---------+
            |jose|  1|      hi|    crazy|
            |  li|  2|      hi|    crazy|
            | liz|  3|      hi|    crazy|
            +----+---+--------+---------+

        """
        _spark = self.spark

        def transform(input_df: DataFrame, f: Callable[[DataFrame], DataFrame], **kwargs):
            return f(input_df, spark=_spark, **kwargs)

        DataFrame.transform = transform

    @staticmethod
    def _patch_withCustomColumn():
        """
        Example usage:

            df.withCustomColumn('rank', non_builtin_spark_sql_function(*args, **kwargs))

        """
        def withCustomColumn(input_df: DataFrame, column_name: str, f: Callable[[DataFrame, str], DataFrame]):
            return f(input_df, column_name)

        DataFrame.withCustomColumn = withCustomColumn

    @staticmethod
    def map_cols(column_names: Iterable[str], alias: str = None) -> List[Column]:
        if alias:
            return [col(f'{alias}.{column_name}') for column_name in column_names]
        else:
            return [col(column_name) for column_name in column_names]

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from dek.utils import LogMixin
from dek.spark.sql import execute_sql
from dek.hadoop import remove_from_hdfs, hql_create_external_table_template
from dek.models import Context, ContextTracker


# **********************************************************************************************************************
# Base Class
# **********************************************************************************************************************


class HiveReadable(LogMixin):
    """
    Required instance/class attributes from children that are not explicitly defined here:
        - resolve_table_name
        - column_def

    Limitation: unable to switch db during runtime
    """

    db = None

    def __init__(self, context: Optional[Context] = None, **kwargs):
        super().__init__(**kwargs)
        self._context = context or ContextTracker.get_current_context()

    @property
    def context(self) -> Context:
        """
        This attr needs to be available to subclasses
        """
        return self._context

    @property
    def db_table_name(self) -> str:
        return f'{self.db}.{self.resolve_table_name}'

    @property
    def resolve_table_name(self) -> str:
        return self.table_name

    def read(self) -> DataFrame:
        return self._read_all_cols()

    def _read_all_cols(self) -> DataFrame:
        columns = self.column_def
        if hasattr(self, 'partition_def'):
            columns.update(**self.partition_def)

        cols = ',\n\t\t'.join([col for col in columns.keys()])

        sql = f"""
            SELECT
                {cols}
            FROM {self.db_table_name}
        """

        return execute_sql(
            spark=self._context.spark, sql=sql, logger=self.logger.info
        )


class HiveWritable(LogMixin):
    """
    A base class that resolves output metadata such as fully-qualified table name, path etc.
    Subclass needs to provide specific writing implementation.

    Required instance/class attributes from children:
    - resolve_table_name
    """

    def __init__(self, context: Optional[Context] = None, **kwargs):
        super().__init__(**kwargs)
        self._context = context or ContextTracker.get_current_context()

    @property
    def context(self) -> Context:
        """
        This attr needs to be available to subclasses
        """
        return self._context

    @property
    def db(self) -> str:
        return self._context.hive_default_db

    @property
    def table_prefix(self) -> str:
        return self._context.hive_table_prefix

    @property
    def file_format(self) -> str:
        return self._context.default_file_format

    @property
    def base_path(self) -> str:
        return f"{self._context.hive_db_hdfs_dir}/{self.prefix_table_name}"

    def path(self, **partition) -> str:
        path = self.base_path

        if partition:
            for key, value in partition.items():
                path = f"{path}/{key}={value}"

        return path

    @property
    def prefix_table_name(self) -> str:
        return f"{self.table_prefix}{self.resolve_table_name}"

    @property
    def db_table_name(self) -> str:
        return f"{self.db}.{self.prefix_table_name}"


# **********************************************************************************************************************
# Derived Class
# **********************************************************************************************************************


class HiveManagedTableWritable(HiveWritable):
    def save(self, df: DataFrame):
        self.logger.info(
            f"Writing to table {self.db_table_name} in {self.file_format} format."
        )

        df.printSchema()
        df.write.mode('overwrite').saveAsTable(
            name=self.db_table_name,
            format=self.file_format,
            path=self.base_path,
        )

        execute_sql(
            spark=self.context.spark,
            sql=f"ALTER TABLE {self.db_table_name} SET tblproperties('external.table.purge'='true')",
        )


class HiveExternalTableWritable(HiveWritable):
    """
    Required instance/class attributes from children that are not explicitly defined here:
        - column_def
    """

    def write(self, df: DataFrame):
        self.logger.info(
            f"Writing to table {self.db_table_name} in {self.file_format} format."
        )

        df.printSchema()
        df.write.mode('overwrite').saveAsTable(
            name=self.db_table_name,
            format=self.file_format,
            path=self.base_path,
        )

    def define_external_table(self):
        self.logger.info("Re-creating hive external table")
        execute_sql(
            spark=self.context.spark,
            sql=f"DROP TABLE IF EXISTS {self.db_table_name}",
            logger=self.logger.info,
        )

        if hasattr(self, 'partition_def'):
            _partition_def = self.partition_def
        else:
            _partition_def = None

        hql_create_external_table = hql_create_external_table_template(
            table_name=self.db_table_name,
            path=self.base_path,
            column_def=self.column_def,
            partition_def=_partition_def,
        )

        execute_sql(
            spark=self.context.spark,
            sql=hql_create_external_table,
            logger=self.logger.info,
        )

        if _partition_def:
            execute_sql(
                spark=self.context.spark,
                sql=f"MSCK REPAIR TABLE {self.db_table_name}",
                logger=self.logger.info,
            )

    def save(self, df: DataFrame):
        self.write(df)
        self.define_external_table()


class HiveExternalPartitionedTableWritable(HiveExternalTableWritable):
    """
    Required instance/class attributes from children that are not explicitly defined here:
        - column_def
        - partition_def
        - base_path
        - db_table_name


    Required instance method:
        - path
    """

    def remove_partition(self, **partition):
        remove_from_hdfs(sink=self.path(**partition))

    def write_single_partition(self, df: DataFrame, **partition):
        if len(self.partition_def) > 1:
            self.logger.error("Nested partition level is not supported!")
            exit(1)

        p_col = next(iter(self.partition_def.keys()))
        self.logger.info(
            f"Writing {self.path(**partition)} in {self.file_format} format."
        )

        df.printSchema()
        df.repartition(1, p_col).write.mode('append').partitionBy(
            p_col
        ).parquet(self.base_path)

    def write_dynamic_partitions(self, df: DataFrame):
        p_cols = self.partition_def.keys()
        self.logger.info(
            f"Writing to {self.base_path}. Table: {self.db_table_name}. Partitioned on {p_cols}. Format: {self.file_format}"
        )

        df.printSchema()
        df.repartition(1, *p_cols).write.insertInto(
            self.db_table_name, overwrite=True
        )


class HiveExternalTableReadWritable(HiveExternalTableWritable, HiveReadable):
    """Enable hive external table read/write"""


class HiveExternalPartitionedTableReadWritable(
    HiveExternalPartitionedTableWritable, HiveReadable
):
    """Enable hive external partitioned table read/write"""


class HiveManagedTableReadWritable(HiveManagedTableWritable, HiveReadable):
    """Enable hive managed table read/write"""


class HiveExternalMonthPartitionedTableReadWritable(
    HiveExternalPartitionedTableReadWritable
):
    def remove(self, month_id):
        self.remove_partition(p_monthid=month_id)

    def append(self, df, month_id):
        self.write_single_partition(df, p_monthid=month_id)

    def finalize(self):
        self.define_external_table()

    def save(self, df, month_id):
        self.remove(month_id)
        self.append(df, month_id)
        self.finalize()

    def add(self, delta_df, month_id):
        current_df = self.read().filter(col('p_monthid') == lit(month_id))

        updated_df = current_df.unionByName(delta_df)
        self.append(df=updated_df, month_id=month_id)


class HiveTextFileWritable(HiveWritable):
    def save(self, df: DataFrame):
        if len(df.columns) > 1:
            raise ValueError('Input df should contain only one column')

        self.logger.info(f'Writing to {self.base_path} in csv format')

        df.write.mode('overwrite').format('csv').option('sep', '|').option(
            'header', 'false'
        ).option('ignoreLeadingWhiteSpace', 'false').option(
            'ignoreTrailingWhiteSpace', 'false'
        ).save(
            path=self.base_path
        )


# **********************************************************************************************************************
# Utility class
# **********************************************************************************************************************


class HiveDb:
    """
    Decorator factory that maps hive db
    """

    def __init__(self, name):
        self._db = name

    def __call__(self, class_):
        class_.db = self._db

        return class_


OverridingDb = HiveDb


class OverridingTablePrefix:
    """
    Decorator factory to force usage of hard-coded table prefix
    """

    def __init__(self, prefix):
        self.table_prefix = prefix

    def __call__(self, class_):
        class_.table_prefix = self.table_prefix

        return class_

from typing import Mapping, Optional
import subprocess
import logging

logger_ = logging.getLogger(__name__)


def remove_from_hdfs(sink):
    logger_.info(f"Removing from {sink}")
    subprocess.run(['hdfs', 'dfs', '-rm', '-r', sink])


def hql_create_external_table_template(
        table_name: str,
        path: str,
        column_def: Mapping[str, str],
        partition_def: Optional[Mapping[str, str]] = None,
        format: str = 'PARQUET'
):
    columns = ', '.join([f"{column_name} {data_type}" for column_name, data_type in column_def.items()])

    if partition_def:
        partitions = ', '.join([f"{partition_col} {data_type}" for partition_col, data_type in partition_def.items()])
        partition_clause = f"PARTITIONED BY ({partitions})"
    else:
        partition_clause = ''

    return f"""
        CREATE EXTERNAL TABLE {table_name} (
            {columns}
        ) {partition_clause}
        STORED AS {format}
        LOCATION '{path}'
    """

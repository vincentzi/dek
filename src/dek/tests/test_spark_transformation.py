from pyspark.sql import SparkSession, Row
from chispa import assert_df_equality

from dek.spark.transformation import union_dfs


def test_union_dfs(spark: SparkSession):
    df1 = spark.createDataFrame(
        [
            (1, 'john'),
            (2, 'alice'),
        ],
        ['id', 'name'],
    )

    df2 = spark.createDataFrame(
        [
            ('bob', 3),
            ('mary', 4),
        ],
        ['name', 'id'],
    )

    actual_df = union_dfs(dfs=[df1, df2])

    expected_df = spark.createDataFrame(
        [
            (1, 'john'),
            (2, 'alice'),
            (3, 'bob'),
            (4, 'mary'),
        ],
        ['id', 'name'],
    )

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    MapType,
)

from testfixtures import log_capture

from dek.spark.extension import patch_StructType, patch_SparkSession


# fmt: off
def test_schema_treeString():
    patch_StructType()

    schema = StructType([
        StructField("customer", StringType(), True),
        StructField("date", StringType(), True),
        StructField("itemList", ArrayType(
            StructType([
                StructField("item", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("quantity", DoubleType(), True),
                StructField("categoryTags", ArrayType(
                    MapType(StringType(), StructType([
                        StructField("tagId", IntegerType(), False),
                        StructField("tagDesc", StringType(), False),
                    ]), False),
                    False
                ), True),
                StructField("label", MapType(StringType(), StringType(), False), False),
            ]), True),
            True,
        ),
        StructField("store", StringType(), True),
        StructField("address", StructType([
            StructField('city', StringType(), False),
            StructField('province', StructType([
                StructField('province_code', StringType(), False),
                StructField('province_name', StringType(), False),
            ]), False),
            StructField('postal_code', StringType(), False),
        ]), True)
    ])

    expected = """\
root
 |-- customer: string (nullable = true)
 |-- date: string (nullable = true)
 |-- itemList: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- item: string (nullable = true)
 |    |    |-- price: double (nullable = true)
 |    |    |-- quantity: double (nullable = true)
 |    |    |-- categoryTags: array (nullable = true)
 |    |    |    |-- element: map (containsNull = false)
 |    |    |    |    |-- key: string
 |    |    |    |    |-- value: struct (valueContainsNull = false)
 |    |    |    |    |    |-- tagId: integer (nullable = false)
 |    |    |    |    |    |-- tagDesc: string (nullable = false)
 |    |    |-- label: map (nullable = false)
 |    |    |    |-- key: string
 |    |    |    |-- value: string (valueContainsNull = false)
 |-- store: string (nullable = true)
 |-- address: struct (nullable = true)
 |    |-- city: string (nullable = false)
 |    |-- province: struct (nullable = false)
 |    |    |-- province_code: string (nullable = false)
 |    |    |-- province_name: string (nullable = false)
 |    |-- postal_code: string (nullable = false)
"""
    assert repr(schema.treeString()) == repr(expected)
# fmt: on


@log_capture()
def test_spark_sql_extension(capture, spark: SparkSession):
    patch_SparkSession()

    _ = spark.sql('SELECT 1')

    capture.check(
        ('dek.spark.extension', 'INFO', 'Executing Spark SQL\nSELECT 1'),
    )

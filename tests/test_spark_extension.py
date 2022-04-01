import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    MapType,
)
from dek.spark.extension import patch_spark_extensions


# fmt: off
@pytest.mark.skip(reason="Not sure how to assert multiline string in this one")
def test_schema_treeString():
    patch_spark_extensions()

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
            ]), True),
            True
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
 |-- store: string (nullable = true)
 |-- address: struct (nullable = true)
 |    |-- city: string (nullable = false)
 |    |-- province: struct (nullable = false)
 |    |    |-- province_code: string (nullable = false)
 |    |    |-- province_name: string (nullable = false)
 |    |-- postal_code: string (nullable = false
"""

    # expected = (
    #     "root\n"
    #     " |-- customer: string (nullable = true)\n"
    #     " |-- date: string (nullable = true)\n"
    #     " |-- itemList: array (nullable = true)\n"
    #     " |    |-- element: struct (containsNull = true)\n"
    #     " |    |    |-- item: string (nullable = true)\n"
    #     " |    |    |-- price: double (nullable = true)\n"
    #     " |    |    |-- quantity: double (nullable = true)\n"
    #     " |    |    |-- categoryTags: array (nullable = true)\n"
    #     " |    |    |    |-- element: map (containsNull = false)\n"
    #     " |    |    |    |    |-- key: string\n"
    #     " |    |    |    |    |-- value: struct (valueContainsNull = false)\n"
    #     " |    |    |    |    |    |-- tagId: integer (nullable = false)\n"
    #     " |    |    |    |    |    |-- tagDesc: string (nullable = false)\n"
    #     " |-- store: string (nullable = true)\n"
    #     " |-- address: struct (nullable = true)\n"
    #     " |    |-- city: string (nullable = false)\n"
    #     " |    |-- province: struct (nullable = false)\n"
    #     " |    |    |-- province_code: string (nullable = false)\n"
    #     " |    |    |-- province_name: string (nullable = false)\n"
    #     " |    |-- postal_code: string (nullable = false\n"
    # )
    assert repr(schema.treeString()) == expected
# fmt: on

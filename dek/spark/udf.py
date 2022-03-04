import json

from pyspark.sql.types import StructType, StructField, ArrayType, StringType, MapType, BooleanType
from pyspark.sql.functions import udf

__all__ = (
    'udf_parse_json',
)


def parse_json(array_str):
    json_obj = json.loads(array_str)
    for item in json_obj:
        yield item['key'], item['value']


user_properties_schema = ArrayType(
    StructType([
        StructField('key', StringType(), True),
        StructField('value', MapType(StringType(), StringType()), True)
    ])
)


@udf(BooleanType())
def udf_is_subset(s1: list, s2: list):
    return set(s1).issubset(set(s2))


@udf(user_properties_schema)
def udf_parse_json(array_str):
    json_obj = json.loads(array_str)
    for item in json_obj:
        yield item['key'], item['value']

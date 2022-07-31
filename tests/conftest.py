import pytest
import logging
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    _spark = (
        SparkSession.builder.master('local').appName('example').getOrCreate()
    )
    _spark.sparkContext.setLogLevel("OFF")

    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)

    yield _spark

    _spark.stop()

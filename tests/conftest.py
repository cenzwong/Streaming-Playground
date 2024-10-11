import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark():
    yield SparkSession.builder.appName("Testing PySpark Example").getOrCreate()

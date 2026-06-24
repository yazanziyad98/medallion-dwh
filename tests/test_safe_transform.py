import pytest
from pyspark.sql import SparkSession
from safe_transform import safe_long, safe_int


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()


def test_safe_long_converts_empty_string_to_null(spark):
    df = spark.createDataFrame([("",)], ["val"])
    result = df.withColumn("val", safe_long("val")).collect()
    assert result[0]["val"] is None


def test_safe_long_casts_valid_string_to_long(spark):
    df = spark.createDataFrame([("123456789012",)], ["val"])
    result = df.withColumn("val", safe_long("val")).collect()
    assert result[0]["val"] == 123456789012


def test_safe_int_converts_empty_string_to_null(spark):
    df = spark.createDataFrame([("",)], ["val"])
    result = df.withColumn("val", safe_int("val")).collect()
    assert result[0]["val"] is None


def test_safe_int_casts_valid_string_to_int(spark):
    df = spark.createDataFrame([("42",)], ["val"])
    result = df.withColumn("val", safe_int("val")).collect()
    assert result[0]["val"] == 42
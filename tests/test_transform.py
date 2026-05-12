import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, length, trim

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test_silver_transform") \
        .master("local[*]") \
        .getOrCreate()

def make_df(spark, data, schema):
    return spark.createDataFrame(data, schema)

SCHEMA = StructType([
    StructField("prescriber_npi",   StringType(), True),
    StructField("state",            StringType(), True),
    StructField("drug_generic",     StringType(), True),
    StructField("total_claims",     IntegerType(), True),
    StructField("total_cost_usd",   DoubleType(), True),
    StructField("data_year",        IntegerType(), True),
])

def test_no_null_npi(spark):
    data = [("1234567890", "CA", "Aspirin", 10, 100.0, 2021),
            (None,         "TX", "Aspirin",  5,  50.0, 2021)]
    df = make_df(spark, data, SCHEMA)
    df_clean = df.filter(col("prescriber_npi").isNotNull())
    assert df_clean.count() == 1

def test_npi_must_be_10_digits(spark):
    data = [("1234567890", "CA", "Aspirin", 10, 100.0, 2021),
            ("123",        "TX", "Aspirin",  5,  50.0, 2021)]
    df = make_df(spark, data, SCHEMA)
    df_valid = df.filter(length(trim(col("prescriber_npi"))) == 10)
    assert df_valid.count() == 1

def test_claims_must_be_positive(spark):
    data = [("1234567890", "CA", "Aspirin", 10, 100.0, 2021),
            ("9876543210", "TX", "Aspirin",  0,  50.0, 2021)]
    df = make_df(spark, data, SCHEMA)
    df_valid = df.filter(col("total_claims") >= 1)
    assert df_valid.count() == 1

def test_no_negative_cost(spark):
    data = [("1234567890", "CA", "Aspirin", 10,  100.0, 2021),
            ("9876543210", "TX", "Aspirin",  5,   -1.0, 2021)]
    df = make_df(spark, data, SCHEMA)
    df_valid = df.filter(col("total_cost_usd") >= 0)
    assert df_valid.count() == 1

import sys
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Add the path to app.py to the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import standardize_name, standardize_name_udf

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("TestETL") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_standardize_name():
    assert standardize_name("First Company Limited") == "first company"
    assert standardize_name("Company Limited LTD") == "company"
    assert standardize_name("My Company Inc.") == "my company"
    assert standardize_name("Another Co.") == "another"
    assert standardize_name("Example GmbH") == "example"

def test_standardize_name_udf(spark):
    data = [("First Company Limited",), ("Company Limited LTD",), ("My Company Inc.",), ("Another Co.",), ("Example GmbH",)]
    df = spark.createDataFrame(data, ["company_name"])
    df = df.withColumn("standardized_company_name", standardize_name_udf(col("company_name")))
    
    expected_data = [("First Company Limited", "first company"), ("Company Limited LTD", "company"), ("My Company Inc.", "my company"), ("Another Co.", "another"), ("Example GmbH", "example")]
    expected_df = spark.createDataFrame(expected_data, ["company_name", "standardized_company_name"])
    
    result = df.collect()
    expected = expected_df.collect()
    
    assert result == expected

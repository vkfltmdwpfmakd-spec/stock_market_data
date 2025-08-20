# tests/test_spark_analyzer.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta

# Import the function to be tested
from scripts.spark_batch_analyzer import analyze_finnhub_data

@pytest.fixture(scope="session")
def spark():
    """ Pytest fixture to create a SparkSession for testing. """
    return SparkSession.builder \
        .master("local[2]") \
        .appName("PytestSparkAnalyzer") \
        .getOrCreate()

def test_moving_average_calculation(spark):
    """
    Tests if the 7-day moving average is calculated correctly.
    """
    # 1. Prepare sample data
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", TimestampType()),
    ])
    
    # Create 10 days of data for one stock
    data = [('TEST', 100.0 + i, datetime(2024, 1, 1) + timedelta(days=i)) for i in range(10)]
    df = spark.createDataFrame(data, schema)
    
    # Mock HDFS paths for local testing
    input_path = "/tmp/test_input_data"
    output_path = "/tmp/test_output_data"
    df.write.mode("overwrite").parquet(input_path)

    # 2. Run the analysis function (we need to temporarily override the path constants)
    # This is a simplified way. A better way would be to pass paths as arguments.
    from scripts import spark_batch_analyzer
    original_input_path = spark_batch_analyzer.HDFS_INPUT_PATH
    original_output_path = spark_batch_analyzer.HDFS_OUTPUT_PATH
    spark_batch_analyzer.HDFS_INPUT_PATH = input_path
    spark_batch_analyzer.HDFS_OUTPUT_PATH = output_path

    analyze_finnhub_data(spark)

    # 3. Read the result and assert
    result_df = spark.read.parquet(output_path)
    result_df.show()

    # The 7th day (index 6) should have a moving average of the first 7 days (100 to 106)
    # Average of (100, 101, 102, 103, 104, 105, 106) is 103.
    seventh_day_row = result_df.filter(col("trade_date") == "2024-01-07").first()
    assert seventh_day_row is not None
    assert seventh_day_row["7_day_moving_average"] == pytest.approx(103.0)

    # The 10th day (index 9) should have a moving average of days 4 to 10 (103 to 109)
    # Average of (103, 104, 105, 106, 107, 108, 109) is 106.
    tenth_day_row = result_df.filter(col("trade_date") == "2024-01-10").first()
    assert tenth_day_row is not None
    assert tenth_day_row["7_day_moving_average"] == pytest.approx(106.0)

    # Restore original paths
    spark_batch_analyzer.HDFS_INPUT_PATH = original_input_path
    spark_batch_analyzer.HDFS_OUTPUT_PATH = original_output_path

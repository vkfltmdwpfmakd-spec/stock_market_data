# finnhub_batch_analyzer.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, to_date

APP_NAME = "FinnhubBatchAnalyzer"
# 입력 경로를 finnhub_consumer가 저장한 경로로 변경
HDFS_INPUT_PATH = "hdfs://namenode:8020/user/spark/finnhub_market_data"
# 출력 경로도 새로운 경로로 변경
HDFS_OUTPUT_PATH = "hdfs://namenode:8020/user/spark/analyzed_finnhub_data"

def create_spark_session():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("[Finnhub] SparkSession이 성공적으로 생성되었습니다.")
    return spark

def analyze_stock_data(spark):
    print(f"[Finnhub] HDFS 경로 '{HDFS_INPUT_PATH}'에서 데이터를 읽어옵니다...")

    try:
        df = spark.read.parquet(HDFS_INPUT_PATH)
        print(f"[Finnhub] 총 {df.count()}개의 레코드를 읽었습니다.")

        # timestamp 컬럼을 날짜 형식으로 변환하여 그룹화
        analyzed_df = df.withColumn("trade_date", to_date(col("timestamp"))) \
                        .groupBy("symbol", "trade_date") \
                        .agg(
                            avg("price").alias("average_price"),
                            avg("previous_close").alias("average_previous_close")
                        ) \
                        .orderBy(col("trade_date").desc(), col("symbol"))

        print("[Finnhub] 분석 결과:")
        analyzed_df.show(truncate=False)

        analyzed_df.write \
            .mode("overwrite") \
            .partitionBy("trade_date") \
            .parquet(HDFS_OUTPUT_PATH)

        print(f"[Finnhub] 분석 결과가 HDFS 경로 '{HDFS_OUTPUT_PATH}'에 성공적으로 저장되었습니다.")

    except Exception as e:
        print(f"[Finnhub] 데이터 분석 및 저장 중 오류 발생: {e}")
        if "Path does not exist" in str(e):
            print("[Finnhub] 경고: HDFS 입력 경로에 데이터가 없습니다.")
        sys.exit(1)

if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        analyze_stock_data(spark)
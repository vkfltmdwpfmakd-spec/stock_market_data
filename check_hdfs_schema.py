#!/usr/bin/env python3
from pyspark.sql import SparkSession

def check_hdfs_data():
    spark = SparkSession.builder \
        .appName("CheckHDFSSchema") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()
    
    print("=== 원본 데이터 스키마 확인 ===")
    try:
        raw_df = spark.read.parquet("hdfs://namenode:8020/user/spark/finnhub_market_data")
        print("원본 데이터 컬럼:")
        raw_df.printSchema()
        print(f"원본 데이터 레코드 수: {raw_df.count()}")
        print("\n샘플 데이터:")
        raw_df.show(5, truncate=False)
    except Exception as e:
        print(f"원본 데이터 읽기 실패: {e}")
    
    print("\n=== 분석 데이터 스키마 확인 ===")
    try:
        analyzed_df = spark.read.parquet("hdfs://namenode:8020/user/spark/analyzed_finnhub_data")
        print("분석 데이터 컬럼:")
        analyzed_df.printSchema()
        print(f"분석 데이터 레코드 수: {analyzed_df.count()}")
        print("\n샘플 데이터:")
        analyzed_df.show(5, truncate=False)
    except Exception as e:
        print(f"분석 데이터 읽기 실패: {e}")
    
    spark.stop()

if __name__ == "__main__":
    check_hdfs_data()
# cleanup_duplicates.py - 중복된 분석 결과 정리

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

APP_NAME = "DuplicateCleanup"
HDFS_ANALYZED_PATH = "hdfs://namenode:8020/user/spark/analyzed_finnhub_data"

def create_spark_session():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logging.info("SparkSession이 성공적으로 생성되었습니다.")
    return spark

def cleanup_duplicates(spark):
    logging.info(f"중복 데이터 정리를 시작합니다: '{HDFS_ANALYZED_PATH}'")
    
    try:
        # 기존 분석된 데이터 읽기
        df = spark.read.parquet(HDFS_ANALYZED_PATH)
        original_count = df.count()
        logging.info(f"전체 레코드 수: {original_count}")
        
        if df.rdd.isEmpty():
            logging.warning("분석된 데이터가 없습니다.")
            return
            
        # 중복 확인
        df.show(20, truncate=False)
        
        # 중복 제거: 각 종목의 각 날짜에 대해 하나의 레코드만 유지
        # 가장 완전한 데이터를 우선으로 선택 (null이 적은 것)
        window = Window.partitionBy("symbol", "trade_date").orderBy(
            desc("moving_avg_7_days"), 
            desc("average_price"),
            desc("data_points_per_day")
        )
        
        deduped_df = df.withColumn("row_num", row_number().over(window)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num")
        
        deduped_count = deduped_df.count()
        removed_count = original_count - deduped_count
        
        logging.info(f"중복 제거 후 레코드 수: {deduped_count}")
        logging.info(f"제거된 중복 레코드 수: {removed_count}")
        
        if removed_count > 0:
            # 정리된 데이터로 덮어쓰기
            logging.info("중복 제거된 데이터를 저장합니다...")
            deduped_df.write \
                .mode("overwrite") \
                .partitionBy("trade_date") \
                .parquet(HDFS_ANALYZED_PATH)
            
            logging.info("중복 데이터 정리가 완료되었습니다!")
        else:
            logging.info("중복 데이터가 발견되지 않았습니다.")
            
    except Exception as e:
        logging.error(f"중복 데이터 정리 중 오류 발생: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        cleanup_duplicates(spark)
        spark.stop()
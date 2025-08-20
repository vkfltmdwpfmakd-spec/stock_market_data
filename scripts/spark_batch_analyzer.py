# spark_batch_analyzer.py (로깅 적용)

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date
from pyspark.sql.window import Window

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# --- Spark Session 초기화 및 설정 ---
APP_NAME = "FinnhubBatchAnalyzer"
HDFS_INPUT_PATH = "hdfs://namenode:8020/user/spark/finnhub_market_data"
HDFS_OUTPUT_PATH = "hdfs://namenode:8020/user/spark/analyzed_finnhub_data"

def create_spark_session():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logging.info(f"[{APP_NAME}] SparkSession이 성공적으로 생성되었습니다.")
    return spark

def analyze_finnhub_data(spark):
    logging.info(f"HDFS 경로 '{HDFS_INPUT_PATH}'에서 데이터를 읽어옵니다...")
    try:
        df = spark.read.parquet(HDFS_INPUT_PATH)
        if df.isEmpty():
            logging.warning(f"입력 경로 '{HDFS_INPUT_PATH}'에 데이터가 없습니다. 작업을 종료합니다.")
            return
        logging.info(f"총 {df.count()}개의 레코드를 읽었습니다.")

        daily_avg_price_df = df.withColumn("trade_date", to_date(col("timestamp")))
                               .groupBy("symbol", "trade_date")
                               .agg(avg("price").alias("daily_avg_price"))
                               .orderBy("symbol", "trade_date")
        logging.info("일별 평균 가격 계산 완료.")

        window_spec = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-6, 0)
        final_df = daily_avg_price_df.withColumn(
            "7_day_moving_average",
            avg(col("daily_avg_price")).over(window_spec)
        )
        logging.info("7일 이동평균선 계산 완료. 최종 결과 (일부):")
        final_df.show(10, truncate=False)

        final_df.write \
            .mode("overwrite") \
            .partitionBy("trade_date") \
            .parquet(HDFS_OUTPUT_PATH)
        logging.info(f"최종 분석 결과가 HDFS 경로 '{HDFS_OUTPUT_PATH}'에 성공적으로 저장되었습니다.")

    except Exception as e:
        logging.error("데이터 분석 및 저장 중 오류 발생", exc_info=True)
        if "Path does not exist" in str(e):
            logging.warning(f"경고: HDFS 입력 경로에 데이터가 없습니다. finnhub_consumer가 먼저 실행되었는지 확인하세요.")
        sys.exit(1)

if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        analyze_finnhub_data(spark)
        spark.stop()
    else:
        logging.error("SparkSession 생성에 실패하여 프로그램을 종료합니다.")

# finnhub_batch_analyzer.py

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date
from pyspark.sql.window import Window

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

APP_NAME = "FinnhubBatchAnalyzer"
HDFS_INPUT_PATH = "hdfs://namenode:8020/user/spark/finnhub_market_data"
HDFS_OUTPUT_PATH = "hdfs://namenode:8020/user/spark/analyzed_finnhub_data"

def create_spark_session():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logging.info("[Finnhub] SparkSession이 성공적으로 생성되었습니다.")
    return spark

def analyze_stock_data(spark):
    logging.info(f"[Finnhub] HDFS 경로 '{HDFS_INPUT_PATH}'에서 데이터를 읽어옵니다...")

    try:
        df = spark.read.parquet(HDFS_INPUT_PATH)
        record_count = df.count()
        logging.info(f"[Finnhub] 총 {record_count}개의 레코드를 읽었습니다.")

        # 데이터가 비어있으면 분석 중단
        if df.rdd.isEmpty():
            logging.warning("[Finnhub] 경고: 입력 데이터가 비어있어 분석을 건너뜁니다.")
            return

        # 1. 데이터를 날짜별로 집계하여 일별 평균 가격 계산
        daily_avg_df = (
            df.withColumn("trade_date", to_date(col("timestamp")))
            .groupBy("symbol", "trade_date")
            .agg(
                avg("current_price").alias("average_price"),
                avg("market_cap").alias("avg_market_cap"),
                avg("pe_ratio").alias("avg_pe_ratio"),
                avg("volatility").alias("avg_volatility"),
                avg("daily_return").alias("avg_daily_return")
            )
        )

        # 2. 7일 이동평균 계산 (Window Function)
        # - 주식 종목(symbol)별로 파티션을 나눕니다.
        # - 각 파티션 내에서 날짜(trade_date)순으로 정렬합니다.
        # - 현재 행(오늘)을 기준으로 이전 6개 행(6일 전)과 현재 행을 포함하여 7일간의 창(window)을 만듭니다.
        windowSpec = Window.partitionBy("symbol") \
                           .orderBy("trade_date") \
                           .rowsBetween(-6, 0)

        # 3. 위에서 정의한 '창'을 기준으로 평균 가격(average_price)의 평균을 계산하여 이동평균을 구합니다.
        analyzed_df = (
            daily_avg_df.withColumn("moving_avg_7_days", avg("average_price").over(windowSpec))
                        .orderBy(col("trade_date").desc(), col("symbol"))
        )

        logging.info("[Finnhub] 분석 결과 (7일 이동평균 포함):")
        analyzed_df.show(truncate=False)

        analyzed_df.write \
            .mode("overwrite") \
            .partitionBy("trade_date") \
            .parquet(HDFS_OUTPUT_PATH)

        logging.info(f"[Finnhub] 분석 결과가 HDFS 경로 '{HDFS_OUTPUT_PATH}'에 성공적으로 저장되었습니다.")

    except Exception as e:
        logging.error(f"[Finnhub] 데이터 분석 및 저장 중 오류 발생: {e}", exc_info=True)
        if "Path does not exist" in str(e):
            logging.warning("[Finnhub] 경고: HDFS 입력 경로에 데이터가 없습니다.")
        sys.exit(1)



if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        analyze_stock_data(spark)
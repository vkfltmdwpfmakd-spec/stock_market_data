# finnhub_batch_analyzer.py

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date, to_timestamp, max as spark_max, when, isnan, isnull, count, desc, row_number, first
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

        # 중복 제거: 같은 날짜, 시간, 종목의 최신 데이터만 유지
        # 실제 수집 시간을 유지하면서 날짜별 분석용 trade_date도 생성
        dedup_window = Window.partitionBy("symbol", "trade_date", "batch_hour").orderBy(desc("batch_timestamp"))
        deduped_df = df.withColumn("trade_date", to_date(col("processed_timestamp"))) \
                      .withColumn("collection_time", to_timestamp(col("processed_timestamp"))) \
                      .withColumn("row_num", row_number().over(dedup_window)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")

        logging.info(f"[Finnhub] 중복 제거 후 {deduped_df.count()}개의 레코드가 남았습니다.")

        # 이미 분석된 날짜 확인 (기존 결과와의 중복 방지)
        try:
            existing_df = spark.read.parquet(HDFS_OUTPUT_PATH)
            existing_dates = existing_df.select("trade_date").distinct().rdd.map(lambda x: x[0]).collect()
            existing_dates_set = set(existing_dates)
            
            # 새로운 데이터에서 이미 처리된 날짜는 제외
            new_dates = deduped_df.select("trade_date").distinct().rdd.map(lambda x: x[0]).collect()
            dates_to_process = [date for date in new_dates if date not in existing_dates_set]
            
            if not dates_to_process:
                logging.info("[Finnhub] 새로 처리할 데이터가 없습니다. 모든 날짜가 이미 분석되었습니다.")
                return
                
            # 새로운 날짜의 데이터만 필터링
            deduped_df = deduped_df.filter(col("trade_date").isin(dates_to_process))
            logging.info(f"[Finnhub] 새로 처리할 날짜: {dates_to_process}")
            
        except Exception as e:
            if "Path does not exist" in str(e):
                logging.info("[Finnhub] 기존 분석 결과가 없어 모든 데이터를 처리합니다.")
            else:
                logging.warning(f"[Finnhub] 기존 데이터 확인 중 오류: {e}")

        # 1. 데이터를 날짜별로 집계하여 일별 평균 가격 계산
        daily_avg_df = (
            deduped_df.groupBy("symbol", "trade_date")
            .agg(
                avg("current_price").alias("average_price"),
                avg("market_cap").alias("avg_market_cap"),
                avg("pe_ratio").alias("avg_pe_ratio"),
                avg("volatility").alias("avg_volatility"),
                avg("daily_return").alias("avg_daily_return"),
                count("*").alias("data_points_per_day"),
                first("collection_time").alias("collection_time")  # 실제 수집 시간 보존
            )
            # null 값 처리
            .filter(col("average_price").isNotNull())
        )

        # 2. 올바른 7일 이동평균 계산 (Window Function)
        # - 주식 종목별로 파티션하고 날짜순 정렬
        # - 현재일 포함 이전 7일간의 데이터로 이동평균 계산
        windowSpec = Window.partitionBy("symbol") \
                           .orderBy("trade_date") \
                           .rowsBetween(-6, 0)

        # 3. 이동평균 및 추가 지표 계산
        analyzed_df = (
            daily_avg_df
            .withColumn("moving_avg_7_days", avg("average_price").over(windowSpec))
            .withColumn("days_of_data", count("average_price").over(windowSpec))
            # 최신 데이터부터 정렬
            .orderBy(col("trade_date").desc(), col("symbol"))
        )

        # 4. 데이터 품질 검증
        total_symbols = analyzed_df.select("symbol").distinct().count()
        latest_date = analyzed_df.agg(spark_max("trade_date")).collect()[0][0]
        
        logging.info(f"[Finnhub] 분석 완료: {total_symbols}개 종목, 최신 데이터: {latest_date}")

        logging.info("[Finnhub] 분석 결과 (7일 이동평균 포함):")
        analyzed_df.show(truncate=False)

        analyzed_df.write \
            .mode("append") \
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
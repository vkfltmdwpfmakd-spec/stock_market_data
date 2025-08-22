# Finnhub 데이터 배치 분석기 
# Consumer가 HDFS에 저장한 원시 데이터를 읽어서 의미있는 지표로 변환
# 7일 이동평균 계산하고 중복 제거도 여기서 처리해야 해서 좀 복잡함

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date, to_timestamp, hour, minute, max as spark_max, when, isnan, isnull, count, desc, row_number, first, concat, lit
from pyspark.sql.window import Window

# 로그는 스파크 로그들 때문에 시끄러워서 INFO 레벨로만
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# 설정들 - Consumer가 저장한 원시 데이터를 읽어서 분석 후 다시 저장
APP_NAME = "FinnhubBatchAnalyzer"
HDFS_INPUT_PATH = "hdfs://namenode:8020/user/spark/finnhub_market_data"  # Consumer가 저장한 곳
HDFS_OUTPUT_PATH = "hdfs://namenode:8020/user/spark/analyzed_finnhub_data"  # 분석 결과 저장할 곳

def create_spark_session():
    """스파크 세션 생성 - HDFS 연결하고 성능 최적화 설정들 적용"""
    # 적응형 쿼리 실행, 파티션 자동 조정, 데이터 불균형 처리 등 성능 최적화
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
    spark.sparkContext.setLogLevel("WARN")  # 스파크 로그 줄이기
    logging.info("[Finnhub] SparkSession이 성공적으로 생성되었습니다.")
    return spark

def analyze_stock_data(spark):
    """실제 분석 함수 - Consumer가 저장한 원시 데이터를 읽어서 7일 이동평균 같은 지표 계산"""
    logging.info(f"[Finnhub] HDFS 경로 '{HDFS_INPUT_PATH}'에서 데이터를 읽어옵니다...")

    try:
        # Consumer가 저장한 parquet 파일들 읽기
        df = spark.read.parquet(HDFS_INPUT_PATH)
        record_count = df.count()
        logging.info(f"[Finnhub] 총 {record_count}개의 레코드를 읽었습니다.")

        # 빈 데이터면 분석할 게 없으니까 그냥 종료
        if df.rdd.isEmpty():
            logging.warning("[Finnhub] 경고: 입력 데이터가 비어있어 분석을 건너뜁니다.")
            return

        # 중복 제거 로직 - 같은 종목, 같은 날짜, 같은 시간, 같은 분의 중복 데이터만 제거
        # 30초마다 수집되므로 같은 분 안에 여러 데이터가 있을 수 있음
        deduped_df = df.withColumn("trade_date", to_date(col("processed_timestamp"))) \
                      .withColumn("collection_time", to_timestamp(col("processed_timestamp"))) \
                      .withColumn("collection_hour", hour(col("processed_timestamp"))) \
                      .withColumn("collection_minute", minute(col("processed_timestamp")))
        
        # 같은 종목, 같은 날짜, 같은 시간, 같은 분의 데이터에서 최신 것만 남기기
        dedup_window = Window.partitionBy("symbol", "trade_date", "collection_hour", "collection_minute").orderBy(desc("batch_timestamp"))
        deduped_df = deduped_df.withColumn("row_num", row_number().over(dedup_window)) \
                              .filter(col("row_num") == 1) \
                              .drop("row_num")

        logging.info(f"[Finnhub] 중복 제거 후 {deduped_df.count()}개의 레코드가 남았습니다.")

        # 이미 처리된 시간대 데이터는 다시 처리하지 않기 - 중복 방지
        try:
            existing_df = spark.read.parquet(HDFS_OUTPUT_PATH)
            # 기존 파일이 새로운 스키마를 지원하는지 확인
            existing_columns = existing_df.columns
            
            # 기존 데이터에 collection_hour, collection_minute이 있는지 확인
            if "collection_hour" in existing_columns and "collection_minute" in existing_columns:
                # 새 스키마 - 시분 단위로 중복 방지
                existing_keys = existing_df.select("symbol", "trade_date", "collection_hour", "collection_minute").distinct()
                existing_keys_df = existing_keys.withColumn("processed_key", 
                    concat(col("symbol"), lit("_"), col("trade_date").cast("string"), 
                           lit("_"), col("collection_hour").cast("string"), 
                           lit("_"), col("collection_minute").cast("string")))
                
                new_keys_df = deduped_df.withColumn("processed_key",
                    concat(col("symbol"), lit("_"), col("trade_date").cast("string"), 
                           lit("_"), col("collection_hour").cast("string"), 
                           lit("_"), col("collection_minute").cast("string")))
                
                unprocessed_df = new_keys_df.join(existing_keys_df.select("processed_key"), ["processed_key"], "left_anti")
                deduped_df = unprocessed_df.drop("processed_key")
            else:
                # 구 스키마 발견 - 새 스키마로 전환하기 위해 모든 새 데이터 처리
                logging.info("[Finnhub] 기존 분석 결과가 구 스키마입니다. 새 스키마(시분 단위)로 전환하여 모든 새 데이터를 처리합니다.")
                # 구 스키마 파일이 있어도 새로운 시분 단위 데이터는 모두 처리하도록 함
            
            if deduped_df.count() == 0:
                logging.info("[Finnhub] 새로 처리할 데이터가 없습니다. 모든 데이터가 이미 분석되었습니다.")
                return
                
            logging.info(f"[Finnhub] {deduped_df.count()}개의 새로운 데이터를 처리합니다.")
            
        except Exception as e:
            if "Path does not exist" in str(e):
                logging.info("[Finnhub] 기존 분석 결과가 없어 모든 데이터를 처리합니다.")
            else:
                logging.warning(f"[Finnhub] 기존 데이터 확인 중 오류: {e}")
                # 오류 발생시 모든 데이터 처리

        # 1단계: 시간별 데이터 준비 - 중복 제거된 원본 데이터 그대로 사용 (집계 안 함!)
        # 15분마다 실행되니까 각 시간대별 가격을 그대로 보존해야 함
        time_based_df = (
            deduped_df
            .withColumn("average_price", col("current_price"))  # 컬럼명 통일
            .withColumn("avg_market_cap", col("market_cap"))
            .withColumn("avg_pe_ratio", col("pe_ratio"))
            .withColumn("avg_volatility", col("volatility"))
            .withColumn("avg_daily_return", col("daily_return"))
            .withColumn("data_points_per_minute", lit(1))  # 분당 1개 데이터포인트
            .filter(col("current_price").isNotNull())
        )

        # 2단계: 7일 이동평균 계산 - 시간순으로 정렬해서 계산
        # 각 종목별로 시간순 정렬해서 최근 데이터들의 이동평균
        windowSpec = Window.partitionBy("symbol") \
                           .orderBy("collection_time") \
                           .rowsBetween(-167, 0)  # 대략 7일치 데이터 (24시간*7일=168개, 최근 168개)

        # 3단계: 최종 분석 결과 생성 - 각 시간대별로 이동평균 계산
        analyzed_df = (
            time_based_df
            .withColumn("moving_avg_7_days", avg("average_price").over(windowSpec))
            .withColumn("data_count_for_avg", count("average_price").over(windowSpec))
            .orderBy(col("collection_time").desc(), col("symbol"))
        )

        # 4단계: 결과 검증 및 로깅
        total_symbols = analyzed_df.select("symbol").distinct().count()
        latest_date = analyzed_df.agg(spark_max("trade_date")).collect()[0][0]
        
        logging.info(f"[Finnhub] 분석 완료: {total_symbols}개 종목, 최신 데이터: {latest_date}")

        logging.info("[Finnhub] 분석 결과 미리보기 (7일 이동평균 포함):")
        analyzed_df.show(truncate=False)

        # 5단계: 분석 결과를 HDFS에 저장 - 시간대별 데이터 누적 저장
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
    # 메인 실행부 - Airflow에서 이 스크립트를 직접 호출
    spark = create_spark_session()
    if spark:
        analyze_stock_data(spark)
        spark.stop()
# finnhub_consumer.py (로깅 및 멱등성 적용)

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# --- 설정 ---
APP_NAME = "FinnhubBatchConsumer"
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "finnhub_stock_data"
HDFS_PATH = "hdfs://namenode:8020/user/spark/finnhub_market_data"

json_schema = StructType([
    # 기본 필드들
    StructField("symbol", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("previous_close", DoubleType(), True),
    StructField("price_change", DoubleType(), True),
    StructField("percent_change", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    
    # 확장 필드들 (선택적)
    StructField("company_name", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("ipo_date", StringType(), True),
    
    # 재무 지표들 (선택적)
    StructField("pe_ratio", DoubleType(), True),
    StructField("pb_ratio", DoubleType(), True),
    StructField("eps_ttm", DoubleType(), True),
    StructField("dividend_yield", DoubleType(), True),
    StructField("beta", DoubleType(), True),
    StructField("week_52_high", DoubleType(), True),
    StructField("week_52_low", DoubleType(), True),
    StructField("volume_10day_avg", DoubleType(), True),
    
    # 계산된 지표들
    StructField("daily_return", DoubleType(), True),
    StructField("volatility", DoubleType(), True),
    StructField("price_range", DoubleType(), True),
    StructField("price_position", DoubleType(), True)
])

def create_spark_session():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logging.info("SparkSession이 성공적으로 생성되었습니다.")
    return spark

def process_kafka_batch(spark):
    logging.info(f"Kafka 토픽 '{KAFKA_TOPIC}'에서 데이터를 배치로 읽어옵니다...")
    try:
        kafka_df = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        if kafka_df.isEmpty():
            logging.warning("Kafka 토픽에 새로운 데이터가 없습니다. 작업을 종료합니다.")
            return

        logging.info(f"{kafka_df.count()}개의 레코드를 읽었습니다.")

        parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_data") \
            .select(from_json(col("json_data"), json_schema).alias("data")) \
            .select("data.*")

        logging.info(f"데이터를 HDFS 경로 '{HDFS_PATH}'에 저장합니다. (mode: overwrite)")

        # 멱등성 보장을 위해 매번 데이터를 덮어쓰기(overwrite)합니다.
        # 이는 파이프라인을 여러 번 재실행해도 결과가 항상 동일하게 유지되도록 합니다.
        parsed_df.write \
            .mode("overwrite") \
            .partitionBy("symbol") \
            .parquet(HDFS_PATH)

        logging.info("데이터 저장을 완료하고 작업을 종료합니다.")

    except Exception as e:
        logging.error("배치 처리 중 오류 발생", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        process_kafka_batch(spark)
        spark.stop()

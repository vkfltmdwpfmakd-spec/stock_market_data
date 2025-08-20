# scripts/data_quality_check.py
# 데이터 품질 검증 스크립트

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, isnan, when, isnull, sum as spark_sum
from datetime import datetime, timedelta

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def create_spark_session():
    """Spark 세션 생성"""
    return SparkSession.builder \
        .appName("DataQualityCheck") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()

def check_data_quality(spark, hdfs_path):
    """
    데이터 품질 검증
    - 널값 검사
    - 중복 데이터 검사
    - 데이터 무결성 검사
    """
    try:
        df = spark.read.parquet(hdfs_path)
        total_records = df.count()
        
        if total_records == 0:
            logging.warning(f"경고: {hdfs_path}에 데이터가 없습니다.")
            return False
        
        logging.info(f"총 {total_records}개 레코드 검증 중...")
        
        # 1. 널값 검사
        null_counts = df.select([
            spark_sum(when(col(c).isNull() | isnan(c), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]).collect()[0].asDict()
        
        # 2. 중복 검사
        unique_records = df.distinct().count()
        duplicate_count = total_records - unique_records
        
        # 3. 가격 데이터 무결성 (음수 가격 검사)
        invalid_prices = df.filter(col("price") <= 0).count() if "price" in df.columns else 0
        
        # 결과 리포트
        logging.info("=== 데이터 품질 리포트 ===")
        logging.info(f"총 레코드: {total_records}")
        logging.info(f"중복 레코드: {duplicate_count}")
        logging.info(f"유효하지 않은 가격: {invalid_prices}")
        
        for col_name, null_count in null_counts.items():
            if null_count > 0:
                logging.warning(f"널값 발견 - {col_name}: {null_count}개")
        
        # 품질 임계값 검사
        quality_passed = (
            duplicate_count < total_records * 0.05 and  # 중복 5% 미만
            invalid_prices == 0 and  # 유효하지 않은 가격 0개
            sum(null_counts.values()) < total_records * 0.1  # 널값 10% 미만
        )
        
        if quality_passed:
            logging.info("✅ 데이터 품질 검증 통과")
        else:
            logging.error("❌ 데이터 품질 검증 실패")
            
        return quality_passed
        
    except Exception as e:
        logging.error(f"데이터 품질 검증 중 오류: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    spark = create_spark_session()
    
    # 원본 데이터 및 분석 결과 검증
    raw_data_path = "hdfs://namenode:8020/user/spark/finnhub_market_data"
    analyzed_data_path = "hdfs://namenode:8020/user/spark/analyzed_finnhub_data"
    
    logging.info("원본 데이터 품질 검증...")
    raw_quality_ok = check_data_quality(spark, raw_data_path)
    
    logging.info("분석 결과 데이터 품질 검증...")
    analyzed_quality_ok = check_data_quality(spark, analyzed_data_path)
    
    if raw_quality_ok and analyzed_quality_ok:
        logging.info("🎉 전체 데이터 품질 검증 완료")
        sys.exit(0)
    else:
        logging.error("💥 데이터 품질 문제 발견")
        sys.exit(1)
# HDFS 분석 데이터 스키마 확인 디버깅 스크립트
from pyspark.sql import SparkSession

def check_hdfs_data_schema():
    """HDFS에 저장된 분석 데이터의 실제 스키마와 샘플 데이터 확인"""
    spark = SparkSession.builder \
        .appName("HDFSSchemaChecker") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()
    
    try:
        # 분석된 데이터 읽기
        df = spark.read.parquet("hdfs://namenode:8020/user/spark/analyzed_finnhub_data")
        
        print("=== 분석된 데이터 스키마 ===")
        df.printSchema()
        
        print(f"\n=== 총 레코드 수: {df.count()} ===")
        
        print("\n=== 컬럼 리스트 ===")
        print(df.columns)
        
        print("\n=== 시간 관련 컬럼들 확인 ===")
        time_cols = [col for col in df.columns if any(word in col.lower() for word in ['time', 'hour', 'minute', 'date'])]
        print(f"시간 관련 컬럼들: {time_cols}")
        
        if time_cols:
            print("\n=== 시간 관련 컬럼 샘플 데이터 ===")
            df.select(time_cols + ['symbol']).show(10, truncate=False)
        
        print("\n=== 최신 5개 레코드 전체 데이터 ===")
        df.orderBy(df.columns[0]).show(5, truncate=False)
        
    except Exception as e:
        print(f"오류 발생: {e}")
        if "Path does not exist" in str(e):
            print("분석된 데이터가 아직 없습니다. Analyzer를 먼저 실행하세요.")
    finally:
        spark.stop()

if __name__ == "__main__":
    check_hdfs_data_schema()
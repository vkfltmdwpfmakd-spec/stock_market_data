# read_parquet.py

import sys
from pyspark.sql import SparkSession

# 실행할 때 터미널에서 경로를 입력받습니다.
if len(sys.argv) < 2:
    print("Usage: spark-submit read_parquet.py <path_to_parquet_directory>")
    sys.exit(-1)

path = sys.argv[1]

# Spark 세션을 만들고 Parquet 파일을 읽습니다.
spark = SparkSession.builder.appName("ParquetReader").getOrCreate()
df = spark.read.parquet(path)

print(f"'{path}' 경로의 데이터를 읽어옵니다.")
# 읽어온 데이터를 표 형태로 출력합니다.
df.show(truncate=False)

spark.stop()



# docker exec spark-master /opt/bitnami/spark/bin/spark-submit /opt/bitnami/spark/scripts/read_parquet.py hdfs://namenode:8020/user/spark/analyzed_finnhub_data/
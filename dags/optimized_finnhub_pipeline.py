# 고도화된 Finnhub 파이프라인 DAG
# 기본 파이프라인보다 똑똑함 - 시장시간 체크하고, 메모리 최적화하고, 데이터 품질도 검증
# 2시간마다 실행하되 시장 열 때만 진짜 실행함

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import logging

# 최적화된 DAG 설정 - 기본보다 더 견고하게
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # 2번까지 재시도 (더 안정적)
    'retry_delay': timedelta(minutes=3),  # 3분 간격으로 재시도
    'execution_timeout': timedelta(minutes=30),  # 30분 넘으면 강제 종료
}

def check_market_hours(**context):
    """시장 시간 체크 함수 - 미국 장 시간에만 데이터 수집하도록"""
    execution_date = context['execution_date']
    hour = execution_date.hour
    weekday = execution_date.weekday()  # 0=월요일, 6=일요일
    
    # 주말이거나 시장 외 시간이면 스킵
    # 미국 장시간: 월-금 9:30-16:00 EST = 한국시간 23:30-06:00
    if weekday >= 5 or not (23 <= hour or hour <= 6):
        logging.info(f"시장 외 시간 또는 주말: {execution_date}. 파이프라인 스킵")
        return 'skip_pipeline'
    
    logging.info(f"시장 시간 내: {execution_date}. 파이프라인 실행")
    return 'run_finnhub_consumer'

# 최적화된 DAG 정의 - 리소스 효율적이고 똑똑한 실행
with DAG(
    dag_id='optimized_finnhub_pipeline',
    default_args=default_args,
    description='최적화된 Finnhub 데이터 파이프라인 - 스마트 스케줄링',
    schedule_interval='0 */2 * * *',  # 2시간마다 실행 체크
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stock', 'finnhub', 'optimized', 'smart_scheduling'],
    max_active_runs=1,  # 동시 실행 막기 (리소스 보호)
) as dag:

    # 1단계: 시장 시간인지 먼저 체크
    market_check = BranchPythonOperator(
        task_id='check_market_hours',
        python_callable=check_market_hours,
        provide_context=True,
    )

    # 스킵 태스크 - 시장 외 시간일 때 실행됨
    skip_task = BashOperator(
        task_id='skip_pipeline',
        bash_command='echo "시장 외 시간 - 파이프라인 스킵"',
    )

    # 2단계: 메모리 최적화된 Consumer 실행
    finnhub_consumer_task = BashOperator(
        task_id='run_finnhub_consumer',
        bash_command="""
            docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --driver-memory 1g \
                --executor-memory 1g \
                --conf spark.sql.adaptive.enabled=true \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-client-runtime:3.3.4 \
                /opt/bitnami/spark/scripts/finnhub_consumer.py
        """,
        # pool='spark_pool',  # 리소스 풀이 있으면 활성화
    )

    # 3단계: 고성능 분석 실행 (메모리 더 많이 사용)
    finnhub_analyzer_task = BashOperator(
        task_id='run_finnhub_analyzer',
        bash_command="""
            docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --driver-memory 2g \
                --executor-memory 2g \
                --conf spark.sql.adaptive.enabled=true \
                --conf spark.sql.adaptive.coalescePartitions.enabled=true \
                /opt/bitnami/spark/scripts/finnhub_batch_analyzer.py
        """,
        # pool='spark_pool',  # 리소스 풀이 있으면 활성화
    )

    # 4단계: 데이터 품질 검증 (분석 결과가 이상한지 체크)
    data_quality_check = BashOperator(
        task_id='data_quality_check',
        bash_command="""
            docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --driver-memory 1g \
                /opt/bitnami/spark/scripts/data_quality_check.py
        """,
    )

    # 실행 순서: 시장시간체크 → [스킵 또는 Consumer] → Analyzer → 품질체크
    market_check >> [skip_task, finnhub_consumer_task]
    finnhub_consumer_task >> finnhub_analyzer_task >> data_quality_check
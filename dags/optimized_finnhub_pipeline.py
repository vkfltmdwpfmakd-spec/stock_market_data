# dags/optimized_finnhub_pipeline.py
# 최적화된 버전: 스마트 스케줄링 및 조건부 실행

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=30),
}

def check_market_hours(**context):
    """
    주식 시장 운영 시간 확인
    미국 주식 시장: 월-금 9:30-16:00 EST (한국시간 23:30-06:00)
    """
    execution_date = context['execution_date']
    # 간단한 예시: 평일 23시-06시에만 실행
    hour = execution_date.hour
    weekday = execution_date.weekday()
    
    # 주말 및 시장 외 시간에는 스킵
    if weekday >= 5 or not (23 <= hour or hour <= 6):
        logging.info(f"시장 외 시간 또는 주말: {execution_date}. 파이프라인 스킵")
        return 'skip_pipeline'
    
    logging.info(f"시장 시간 내: {execution_date}. 파이프라인 실행")
    return 'run_finnhub_consumer'

with DAG(
    dag_id='optimized_finnhub_pipeline',
    default_args=default_args,
    description='최적화된 Finnhub 데이터 파이프라인 - 스마트 스케줄링',
    schedule_interval='0 */2 * * *',  # 2시간마다 실행 (기본)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stock', 'finnhub', 'optimized', 'smart_scheduling'],
    max_active_runs=1,  # 동시 실행 방지
) as dag:

    # 1. 시장 시간 확인
    market_check = BranchPythonOperator(
        task_id='check_market_hours',
        python_callable=check_market_hours,
        provide_context=True,
    )

    # 2. 파이프라인 스킵 태스크
    skip_task = BashOperator(
        task_id='skip_pipeline',
        bash_command='echo "시장 외 시간 - 파이프라인 스킵"',
    )

    # 3. 조건부 데이터 수집 (메모리 최적화)
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
        # pool='spark_pool',  # 리소스 풀 사용 (비활성화 - 풀이 없으면 에러)
    )

    # 4. 증분 데이터 분석 (시간대별 파티셔닝)
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
        # pool='spark_pool',  # 리소스 풀 사용 (비활성화 - 풀이 없으면 에러)
    )

    # 5. 데이터 품질 검증
    data_quality_check = BashOperator(
        task_id='data_quality_check',
        bash_command="""
            docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --driver-memory 1g \
                /opt/bitnami/spark/scripts/data_quality_check.py
        """,
    )

    # 태스크 의존성
    market_check >> [skip_task, finnhub_consumer_task]
    finnhub_consumer_task >> finnhub_analyzer_task >> data_quality_check
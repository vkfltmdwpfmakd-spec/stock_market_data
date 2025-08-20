# dags/finnhub_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='finnhub_stock_pipeline',
    default_args=default_args,
    description='Finnhub API를 이용한 주식 데이터 수집, 저장, 분석 파이프라인',
    schedule_interval='@hourly', # 1시간마다 실행
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stock', 'finnhub', 'data_pipeline'],
) as dag:

    # 1. Finnhub 데이터를 HDFS로 저장 (배치 컨슈머 실행)
    finnhub_consumer_task = BashOperator(
        task_id='run_finnhub_batch_consumer',
        bash_command=f"""
            docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-client-runtime:3.3.4 \
                /opt/bitnami/spark/scripts/finnhub_consumer.py
        """,
        dag=dag,
    )

    # 2. HDFS에 저장된 Finnhub 데이터 분석
    finnhub_analyzer_task = BashOperator(
        task_id='run_finnhub_batch_analyzer',
        bash_command=f"""
            docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                /opt/bitnami/spark/scripts/finnhub_batch_analyzer.py
        """,
        dag=dag,
    )

    # --- 태스크 의존성 설정 ---
    finnhub_consumer_task >> finnhub_analyzer_task

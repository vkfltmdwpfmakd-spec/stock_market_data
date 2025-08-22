# Finnhub 데이터 파이프라인 DAG
# Producer는 docker-compose에서 무한 루프로 돌고, 여기서는 15분마다 배치로 데이터 처리
# Consumer → Analyzer 순서로 실행해서 Kafka 데이터를 HDFS로 보내고 분석함

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG 기본 설정 - 실패해도 1번은 재시도하고 5분 기다림
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 이전 실행 실패해도 다음 실행 가능
    'email_on_failure': False,  # 이메일 알림 안함 (개발 환경)
    'email_on_retry': False,
    'retries': 1,  # 1번 재시도
    'retry_delay': timedelta(minutes=5),  # 5분 후 재시도
}

# DAG 정의 - 15분마다 실행되는 주식 데이터 처리 파이프라인
with DAG(
    dag_id='finnhub_stock_pipeline',
    default_args=default_args,
    description='Finnhub API를 이용한 주식 데이터 수집, 저장, 분석 파이프라인',
    schedule_interval=timedelta(minutes=15),  # 15분마다 실행 (배치 처리)
    start_date=datetime(2023, 1, 1),  # DAG 시작 날짜
    catchup=False,  # 과거 실행 안함 (실시간 데이터니까)
    tags=['stock', 'finnhub', 'data_pipeline', 'append_mode'],
) as dag:

    # 1단계: Kafka에서 데이터 읽어서 HDFS에 저장
    # Producer가 Kafka에 넣어놓은 데이터를 배치로 읽어서 parquet 파일로 저장
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

    # 2단계: 원시 데이터를 분석해서 7일 이동평균 같은 지표 계산
    # Consumer가 저장한 데이터를 읽어서 투자 지표들 계산하고 다시 저장
    finnhub_analyzer_task = BashOperator(
        task_id='run_finnhub_batch_analyzer',
        bash_command=f"""
            docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                /opt/bitnami/spark/scripts/finnhub_batch_analyzer.py
        """,
        dag=dag,
    )

    # 실행 순서: Consumer 먼저 → 성공하면 Analyzer 실행
    finnhub_consumer_task >> finnhub_analyzer_task

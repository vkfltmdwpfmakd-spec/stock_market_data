# dags/monitoring_pipeline.py

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
    dag_id='monitoring_pipeline',
    default_args=default_args,
    description='데이터 파이프라인의 상태를 모니터링합니다.',
    schedule_interval='0 */3 * * *',  # 3시간마다 실행
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['monitoring', 'data_pipeline'],
) as dag:

    # 1. Kafka 토픽에 데이터가 수신되는지 확인
    # kafka-console-consumer를 30초 동안 실행하여 메시지가 없으면 에러를 발생시킵니다.
    check_kafka_topic_task = BashOperator(
        task_id='check_kafka_topic',
        bash_command="""
            echo "Checking Kafka topic for new messages..."
            timeout 30s docker exec kafka /usr/bin/kafka-console-consumer \
                --bootstrap-server kafka:9092 \
                --topic finnhub_stock_data \
                --from-beginning \
                --max-messages 1 \
                --timeout-ms 30000 | grep -q . || (echo "No message received from Kafka in 30s" && exit 1)
        """,
        dag=dag,
    )

    # 2. HDFS에 분석된 데이터가 존재하는지 확인
    # hdfs dfs -ls 명령어의 결과가 있는지 확인하여 데이터 존재 여부를 판단합니다.
    check_hdfs_data_task = BashOperator(
        task_id='check_hdfs_data',
        bash_command="""
            echo "Checking HDFS for analyzed data..."
            docker exec namenode hdfs dfs -ls /user/spark/analyzed_finnhub_data | grep -q . || (echo "Analyzed data does not exist in HDFS" && exit 1)
        """,
        dag=dag,
    )

    # --- 태스크 의존성 설정 (두 검사는 독립적으로 실행 가능) ---
    # 여기서는 의존성 없이 병렬로 실행되도록 설정합니다.

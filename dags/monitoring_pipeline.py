# 파이프라인 모니터링 DAG
# 3시간마다 실행해서 데이터 파이프라인이 제대로 돌아가는지 체크
# Kafka 토픽에 데이터 들어오는지, HDFS에 분석 결과 잘 저장되는지 확인

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 모니터링 DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 이전 실행 결과와 상관없이 실행
    'email_on_failure': False,  # 실패해도 이메일 안보냄
    'email_on_retry': False,
    'retries': 1,  # 실패하면 1번 재시도
    'retry_delay': timedelta(minutes=5),
}

# 모니터링 DAG 정의 - 3시간마다 시스템 상태 체크
with DAG(
    dag_id='monitoring_pipeline',
    default_args=default_args,
    description='데이터 파이프라인의 상태를 모니터링합니다.',
    schedule_interval='0 */3 * * *',  # 3시간마다 실행 (cron 표현식)
    start_date=datetime(2023, 1, 1),
    catchup=False,  # 과거 실행 건너뛰기
    tags=['monitoring', 'data_pipeline'],
) as dag:

    # 1단계: Kafka 토픽 상태 확인
    # Producer가 데이터를 제대로 보내고 있는지 30초 동안 체크
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

    # 2단계: HDFS 데이터 존재 확인
    # Analyzer가 분석 결과를 제대로 저장했는지 체크
    check_hdfs_data_task = BashOperator(
        task_id='check_hdfs_data',
        bash_command="""
            echo "Checking HDFS for analyzed data..."
            docker exec namenode hdfs dfs -ls /user/spark/analyzed_finnhub_data | grep -q . || (echo "Analyzed data does not exist in HDFS" && exit 1)
        """,
        dag=dag,
    )

    # 두 검사는 독립적으로 병렬 실행 - 서로 연관성 없음

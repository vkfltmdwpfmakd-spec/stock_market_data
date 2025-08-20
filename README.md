📈 Finnhub 주식 시장 데이터 파이프라인
이 프로젝트는 Apache Kafka, Hadoop, Spark, Airflow를 활용하여 Finnhub API로부터 주식 시장 데이터를 수집, 저장, 처리 및 분석하는 End-to-End 데이터 파이프라인을 구축합니다. 모든 서비스는 Docker Compose를 통해 컨테이너화되어 쉽게 배포하고 관리할 수 있습니다.

🚀 프로젝트 구성 요소 및 흐름

1. 데이터 수집 및 전송 (Kafka Producer)

기술: Python (requests, kafka-python), Docker

역할: Finnhub API에서 실시간 주식 시세 데이터를 주기적으로 가져옵니다.

흐름: 수집된 JSON 형식의 주식 데이터를 Kafka의 'stock_market_data' 토픽으로 전송합니다. 이 과정은 Airflow DAG에 의해 스케줄링되어 자동화됩니다. API 키는 보안을 위해 .env 파일을 통해 환경 변수로 관리됩니다.

2. 분산 메시지 큐 (Apache Kafka & Zookeeper)

기술: Apache Kafka, Apache Zookeeper

역할:
- Zookeeper: Kafka 클러스터의 메타데이터를 관리하고 분산 코디네이션을 담당합니다.
- Kafka: Producer로부터 전송된 주식 데이터를 'stock_market_data' 토픽에 안정적으로 저장하고, Consumer가 데이터를 실시간으로 읽어갈 수 있도록 하는 고성능 분산 스트리밍 플랫폼입니다.

3. 분산 스토리지 (Hadoop HDFS)

기술: Hadoop HDFS (NameNode, DataNode)

역할: 
- NameNode: HDFS의 마스터 노드로, 파일 시스템의 메타데이터(파일 위치, 블록 정보 등)를 관리합니다.
- DataNode: HDFS의 워커 노드로, 실제 데이터 블록을 저장합니다.

흐름: Kafka에서 소비된 주식 데이터는 Spark를 통해 HDFS의 '/user/spark/stock_data' 경로에 Parquet 형식으로 저장됩니다. 분석된 최종 결과 또한 HDFS의 '/user/spark/analyzed_stock_data' 경로에 저장됩니다.

4. 배치 데이터 처리 (Apache Spark)

기술: Apache Spark (PySpark)

역할:
- Spark Master: Spark 클러스터의 리소스 관리 및 작업을 스케줄링합니다.
- Spark Worker: 실제 데이터 처리 작업을 실행합니다.

흐름:
- Spark Consumer (finnhub_consumer.py): Kafka의 'stock_market_data' 토픽에서 데이터를 읽어와 HDFS에 Parquet 형식으로 저장합니다. `overwrite` 모드를 사용하여 데이터의 멱등성을 보장합니다.
- Spark 배치 분석 (finnhub_batch_analyzer.py): HDFS에 저장된 원시 데이터를 읽어와 일별 평균 가격과 7일 이동평균선을 계산하고, 그 결과를 다시 HDFS에 저장합니다.

5. 워크플로우 오케스트레이션 (Apache Airflow)

기술: Apache Airflow

역할: 데이터 파이프라인의 모든 작업을 프로그래밍 방식으로 정의하고, 스케줄링하며, 모니터링하는 플랫폼입니다.

흐름:
`finnhub_pipeline.py` DAG 파일에 정의된 대로, `run_finnhub_producer`, `run_finnhub_batch_consumer`, `run_finnhub_batch_analyzer` 태스크를 순차적으로 실행합니다. 이를 통해 주식 데이터 수집부터 HDFS 저장, Spark 분석까지의 모든 과정이 자동화됩니다.

🛠️ 프로젝트 구조
.
├── dags/
│   └── finnhub_pipeline.py         # Airflow DAG 정의 파일
├── scripts/
│   ├── finnhub_producer.py         # Finnhub 데이터 수집 및 Kafka 전송 스크립트
│   ├── finnhub_consumer.py         # Kafka 데이터를 HDFS에 저장하는 스크립트
│   └── finnhub_batch_analyzer.py   # HDFS 데이터 배치 분석 스크립트 (이동평균선 등)
├── tests/
│   ├── test_dag_integrity.py       # Airflow DAG 무결성 테스트
│   └── test_spark_analyzer.py      # Spark 분석 로직 단위 테스트
├── .env                            # API 키 등 환경 변수 파일 (사용자 생성)
├── requirements.txt                # Python 라이브러리 의존성 목록
├── requirements-dev.txt            # 개발 및 테스트용 라이브러리 목록
├── docker-compose.yml              # 모든 Docker 서비스 정의 파일
└── Dockerfile                      # Airflow 컨테이너 빌드 파일

⚙️ 실행 방법
1. Docker 실행:
Docker Desktop을 실행합니다.

2. .env 파일 생성:
프로젝트 루트 폴더에 `.env` 파일을 생성하고, 발급받은 Finnhub API 키를 다음과 같이 추가합니다.
FINNHUB_API_KEY=YOUR_FINNHUB_API_KEY

주의: `YOUR_FINNHUB_API_KEY` 부분을 실제 API 키로 반드시 교체해야 합니다.

3. Docker Compose 실행:
터미널에서 다음 명령어를 실행하여 모든 서비스를 빌드하고 백그라운드에서 실행합니다.
docker-compose up --build -d

4. Airflow UI 접속:
웹 브라우저에서 http://localhost:8082로 접속합니다 (ID: admin, PW: admin). `finnhub_stock_pipeline` DAG를 활성화하고 수동으로 트리거하여 파이프라인을 실행합니다.

5. 결과 확인:
HDFS에 저장된 데이터는 `scripts/read_parquet.py` 스크립트를 사용하거나, Spark 작업을 통해 직접 확인할 수 있습니다.

6. 서비스 종료:
docker-compose down

💡 추가 정보
- 이 프로젝트는 데이터 엔지니어링의 핵심 요소인 데이터 수집, 저장, 처리, 분석의 전 과정을 자동화된 파이프라인으로 구축하는 방법을 보여줍니다.
- 각 컴포넌트는 Docker를 통해 독립적으로 실행되므로, 확장성과 유지보수성이 뛰어납니다.
- Airflow를 통해 복잡한 데이터 워크플로우를 안정적으로 관리할 수 있습니다.
- Spark의 배치 처리 기능을 활용하여 이동평균선과 같은 시계열 분석을 수행합니다.
- Parquet 형식을 사용하여 대용량 데이터 처리 성능을 최적화합니다.

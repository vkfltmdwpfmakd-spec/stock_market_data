# Real-time Stock Data Pipeline

Apache Kafka, Spark, HDFS, Airflow를 활용한 실시간 데이터 파이프라인 구축 프로젝트입니다. Finnhub API로부터 주식 데이터를 수집하여 실시간 스트리밍 처리 후, 시계열 분석을 통해 투자 인사이트를 제공합니다.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Docker](https://img.shields.io/badge/Docker-20.10+-green.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.8-red.svg)

## 프로젝트 개요

### 구현 배경
금융 데이터 분석 업무에서 실시간 데이터 처리의 중요성이 커지고 있습니다. 기존 배치 처리 방식의 한계를 극복하고, 실시간으로 변화하는 시장 데이터를 신속하게 분석할 수 있는 시스템이 필요했습니다.

### 핵심 기능
- **실시간 데이터 수집**: Finnhub API 연동을 통한 주식 시세 실시간 수집
- **스트림 처리**: Apache Kafka를 활용한 대용량 메시지 스트리밍
- **분산 저장**: HDFS 클러스터 기반 확장 가능한 데이터 저장소
- **배치 분석**: Spark를 활용한 시계열 분석 및 기술적 지표 계산
- **실시간 모니터링**: Streamlit 대시보드를 통한 실시간 데이터 시각화

## 시스템 아키텍처

```
┌─────────────┐    ┌─────────┐    ┌───────────┐    ┌─────────────┐
│ Finnhub API │───▶│  Kafka  │───▶│   Spark   │───▶│    HDFS     │
└─────────────┘    └─────────┘    └───────────┘    └─────────────┘
                                         │                   │
                                         ▼                   ▼
┌─────────────┐    ┌─────────────────────┐         ┌─────────────┐
│ Streamlit   │◀───│    Analytics        │◀────────│ Raw Data    │
│ Dashboard   │    │    Engine (Spark)   │         │ Storage     │
└─────────────┘    └─────────────────────┘         └─────────────┘
```

## 기술 스택 및 선택 이유

### 데이터 수집 & 스트리밍
- **Apache Kafka**: 높은 처리량과 내결함성을 제공하는 분산 스트리밍 플랫폼
- **Zookeeper**: Kafka 클러스터 메타데이터 관리 및 리더 선출
- **Python (kafka-python)**: 안정적인 Kafka Producer/Consumer 구현

### 데이터 처리 & 분석  
- **Apache Spark**: 메모리 기반 분산 처리로 빠른 데이터 분석 성능
- **PySpark**: Python 생태계와의 원활한 통합
- **Spark Structured Streaming**: 실시간 스트림 처리를 위한 고수준 API

### 데이터 저장
- **Hadoop HDFS**: 대용량 데이터의 안정적인 분산 저장
- **Apache Parquet**: 컬럼형 저장 방식으로 분석 쿼리 성능 최적화

### 워크플로 관리
- **Apache Airflow**: 복잡한 데이터 파이프라인의 스케줄링 및 모니터링
- **Docker Compose**: 마이크로서비스 아키텍처 기반 컨테이너 오케스트레이션

## 실행 방법

### 준비사항
1. Docker Desktop 설치
2. [Finnhub](https://finnhub.io/)에서 무료 API 키 발급

### 설정
프로젝트 루트에 `.env` 파일 생성:
```
FINNHUB_API_KEY=당신의_API_키
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
AIRFLOW__CORE__FERNET_KEY=임의의_32자리_키
AIRFLOW__WEBSERVER__SECRET_KEY=임의의_시크릿_키
```

### 실행
```bash
# 전체 시스템 시작 (처음엔 시간이 좀 걸려요)
docker-compose up -d --build

# 잘 떴는지 확인
docker-compose ps
```

### 접속 주소
- Airflow: http://localhost:8082 (admin/admin)
- Spark: http://localhost:8080  
- HDFS: http://localhost:9870
- 대시보드: http://localhost:8503

### 파이프라인 실행
1. Airflow 웹에서 `finnhub_stock_pipeline` DAG 찾기
2. 토글 버튼으로 활성화
3. "Trigger DAG" 버튼 클릭
4. 실행 완료되면 대시보드에서 결과 확인

## 구조

```
├── dags/
│   └── finnhub_pipeline.py        # Airflow DAG 정의
├── scripts/
│   ├── finnhub_producer.py        # API 데이터 수집
│   ├── finnhub_consumer.py        # Kafka → HDFS 저장
│   ├── finnhub_batch_analyzer.py  # 이동평균 분석
│   └── hdfs_init.sh              # HDFS 자동 초기화
├── tests/                        # 테스트 코드
├── dashboard.py                  # Streamlit 대시보드
├── docker-compose.yml           # 전체 서비스 정의
└── requirements.txt            # Python 의존성
```




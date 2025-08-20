# Dockerfile for Airflow
# 공식 Airflow 이미지를 기반으로 합니다.
FROM apache/airflow:2.8.1

# 루트 사용자로 전환하여 패키지를 설치합니다.
USER root

# Airflow 컨테이너가 호스트의 Docker를 제어(docker exec)하기 위해 moby-cli (docker-ce-cli의 Debian 버전)를 설치합니다.
RUN apt-get update && \
    apt-get install -y --no-install-recommends moby-cli && \
    apt-get clean

# 다시 Airflow 사용자로 전환합니다.
USER airflow

# 파이썬 의존성을 설치합니다.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 최종 실행 사용자를 airflow로 명시하여 보안을 강화합니다.
USER airflow
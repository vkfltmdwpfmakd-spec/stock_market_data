#!/bin/bash

echo "==== HDFS 자동 초기화 시작 ====" 

# namenode 연결 대기
echo "namenode 서비스 연결 대기 중..."
until nc -z namenode 8020; do
  echo "⏳ namenode:8020 연결 대기..."
  sleep 3
done
echo "✅ namenode에 연결되었습니다!"

# 추가 대기 시간 (HDFS 완전 초기화)
echo "HDFS 완전 초기화 대기 중..."
sleep 10

echo "==== HDFS 디렉토리 구조 생성 중 ===="

# 필요한 디렉토리들 생성
directories=(
    "/user"
    "/user/spark"
    "/user/spark/finnhub_market_data"
    "/tmp"
)

for dir in "${directories[@]}"; do
    echo "📁 디렉토리 생성: $dir"
    if hdfs dfs -fs hdfs://namenode:8020 -test -d $dir; then
        echo "   ✅ 이미 존재함: $dir"
    else
        hdfs dfs -fs hdfs://namenode:8020 -mkdir -p $dir
        echo "   ✅ 생성 완료: $dir"
    fi
done

echo "==== 권한 설정 중 ===="
hdfs dfs -fs hdfs://namenode:8020 -chown -R spark:spark /user/spark
hdfs dfs -fs hdfs://namenode:8020 -chmod -R 755 /user
hdfs dfs -fs hdfs://namenode:8020 -chmod -R 777 /tmp

echo "==== 초기화 결과 확인 ===="
echo "📋 생성된 디렉토리 목록:"
hdfs dfs -fs hdfs://namenode:8020 -ls -R /user

echo ""
echo "🎉 HDFS 초기화가 성공적으로 완료되었습니다!"
echo "==== 초기화 완료 ===="
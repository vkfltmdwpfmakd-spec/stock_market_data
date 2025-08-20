# finnhub_producer.py (로깅 적용)

import os
import requests
import json
import time
import logging
from kafka import KafkaProducer
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# --- 설정 ---
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'finnhub_stock_data'
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
STOCK_SYMBOLS = ['TSLA', 'NVDA', 'AMD', 'QCOM', 'INTC']
REQUEST_INTERVAL_SECONDS = 30

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info(f"Kafka Producer가 {KAFKA_BROKER}에 성공적으로 연결되었습니다.")
        return producer
    except Exception as e:
        logging.error(f"Kafka Producer 연결 오류: {e}", exc_info=True)
        return None

def get_stock_data_from_finnhub(symbol):
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data and data.get('c') != 0 and data.get('c') is not None:
            stock_data = {
                'symbol': symbol,
                'open': float(data.get('o', 0)),
                'high': float(data.get('h', 0)),
                'low': float(data.get('l', 0)),
                'price': float(data.get('c', 0)),
                'previous_close': float(data.get('pc', 0)),
                'timestamp': datetime.fromtimestamp(data.get('t', 0)).isoformat()
            }
            logging.info(f"'{symbol}' 데이터 가져오기 성공: {stock_data['price']}")
            return stock_data
        else:
            logging.warning(f"'{symbol}'에 대한 유효한 데이터 없음: {data}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"API 요청 오류 for '{symbol}': {e}")
        return None
    except Exception as e:
        logging.error(f"JSON 파싱 또는 데이터 형식 오류 for '{symbol}': {e}", exc_info=True)
        return None

def send_to_kafka(producer, topic, data):
    if producer and data:
        try:
            producer.send(topic, data).get(timeout=10)
            logging.info(f"데이터를 Kafka 토픽 '{topic}'으로 성공적으로 전송했습니다: {data['symbol']}")
        except Exception as e:
            logging.error(f"Kafka 메시지 전송 오류: {e}", exc_info=True)

if __name__ == "__main__":
    if not FINNHUB_API_KEY:
        logging.error("치명적 오류: FINNHUB_API_KEY 환경 변수가 설정되지 않았습니다.")
        exit(1)

    producer = create_kafka_producer()
    if not producer:
        exit(1)

    run_minutes = 1
    total_cycles = int(run_minutes * 60 / REQUEST_INTERVAL_SECONDS)
    logging.info(f"--- Finnhub 데이터 수집 시작 ({run_minutes}분간, 총 {total_cycles} 사이클) ---")

    for i in range(total_cycles):
        logging.info(f"--- 사이클: {i + 1}/{total_cycles} ---")
        for symbol in STOCK_SYMBOLS:
            stock_data = get_stock_data_from_finnhub(symbol)
            send_to_kafka(producer, KAFKA_TOPIC, stock_data)
            time.sleep(1)
        
        remaining_wait_time = REQUEST_INTERVAL_SECONDS - len(STOCK_SYMBOLS)
        if remaining_wait_time > 0:
            logging.info(f"--- 사이클 {i + 1} 완료, 다음 사이클까지 {remaining_wait_time}초 대기... ---")
            time.sleep(remaining_wait_time)

    logging.info("--- Finnhub 데이터 수집 완료 ---")
    producer.close()

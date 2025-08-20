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
# 동적 종목 선택 설정
STOCK_SYMBOLS_CONFIG = os.getenv('STOCK_SYMBOLS', 'TSLA,NVDA,AMD,QCOM,INTC,AAPL,GOOGL,MSFT')
STOCK_SELECTION_METHOD = os.getenv('STOCK_SELECTION_METHOD', 'manual')  # manual, trending, sp500
REQUEST_INTERVAL_SECONDS = 30

def get_trending_stocks():
    """Finnhub에서 트렌딩 종목 가져오기"""
    try:
        url = f"https://finnhub.io/api/v1/stock/recommendation-trends?symbol=AAPL&token={FINNHUB_API_KEY}"
        # 실제로는 여러 종목을 확인하고 recommendation score가 높은 것들 선택
        # 간단한 예시로 tech stocks 반환
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']
    except Exception as e:
        logging.error(f"트렌딩 종목 조회 오류: {e}")
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN']  # 기본값

def get_sp500_top_stocks():
    """S&P 500 상위 종목들"""
    # 실제 환경에서는 S&P 500 API나 웹 스크래핑으로 가져올 수 있음
    sp500_large_caps = [
        'AAPL', 'MSFT', 'AMZN', 'NVDA', 'GOOGL', 'TSLA', 'META', 'GOOG',
        'BRK.B', 'UNH', 'JNJ', 'XOM', 'JPM', 'V', 'PG', 'AVGO'
    ]
    return sp500_large_caps[:10]  # 상위 10개만

def get_dynamic_stock_list():
    """설정에 따른 동적 종목 리스트 생성"""
    if STOCK_SELECTION_METHOD == 'trending':
        logging.info("트렌딩 종목으로 설정...")
        return get_trending_stocks()
    elif STOCK_SELECTION_METHOD == 'sp500':
        logging.info("S&P 500 상위 종목으로 설정...")
        return get_sp500_top_stocks()
    else:
        logging.info("수동 설정 종목 사용...")
        return [symbol.strip() for symbol in STOCK_SYMBOLS_CONFIG.split(',')]

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

def get_enhanced_stock_data(symbol):
    """기본 quote + 추가 메트릭을 한번에 수집"""
    
    # 1. 기본 quote 데이터
    quote_url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_API_KEY}"
    
    try:
        quote_response = requests.get(quote_url, timeout=10)
        quote_response.raise_for_status()
        quote_data = quote_response.json()
        
        if not quote_data or quote_data.get('c') == 0:
            logging.warning(f"'{symbol}'에 대한 유효한 quote 데이터 없음")
            return None
            
        # 기본 주식 데이터
        enhanced_data = {
            'symbol': symbol,
            'current_price': float(quote_data.get('c', 0)),
            'open_price': float(quote_data.get('o', 0)),
            'high_price': float(quote_data.get('h', 0)),
            'low_price': float(quote_data.get('l', 0)),
            'previous_close': float(quote_data.get('pc', 0)),
            'price_change': float(quote_data.get('d', 0)),
            'percent_change': float(quote_data.get('dp', 0)),
            'timestamp': datetime.fromtimestamp(quote_data.get('t', 0)).isoformat()
        }
        
        # 2. 회사 기본 정보 (캐시된 정보, 매시간 업데이트)
        current_hour = datetime.now().hour
        if current_hour % 2 == 0:  # 2시간마다 업데이트
            try:
                profile_url = f"https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={FINNHUB_API_KEY}"
                profile_response = requests.get(profile_url, timeout=10)
                if profile_response.status_code == 200:
                    profile_data = profile_response.json()
                    enhanced_data.update({
                        'company_name': profile_data.get('name', ''),
                        'industry': profile_data.get('finnhubIndustry', ''),
                        'market_cap': float(profile_data.get('marketCapitalization', 0)),
                        'country': profile_data.get('country', ''),
                        'currency': profile_data.get('currency', 'USD'),
                        'exchange': profile_data.get('exchange', ''),
                        'ipo_date': profile_data.get('ipo', ''),
                    })
                    time.sleep(0.2)  # API 레이트 제한 준수
            except Exception as e:
                logging.warning(f"Profile 데이터 수집 실패 for {symbol}: {e}")
        
        # 3. 기본 재무 지표 (3시간마다)
        if current_hour % 3 == 0:
            try:
                metrics_url = f"https://finnhub.io/api/v1/stock/metric?symbol={symbol}&metric=all&token={FINNHUB_API_KEY}"
                metrics_response = requests.get(metrics_url, timeout=10)
                if metrics_response.status_code == 200:
                    metrics_data = metrics_response.json()
                    if 'metric' in metrics_data:
                        metric = metrics_data['metric']
                        enhanced_data.update({
                            'pe_ratio': float(metric.get('peBasicExclExtraTTM', 0)),
                            'pb_ratio': float(metric.get('pbAnnual', 0)),
                            'eps_ttm': float(metric.get('epsBasicExclExtraAnnual', 0)),
                            'dividend_yield': float(metric.get('dividendYieldIndicatedAnnual', 0)),
                            'beta': float(metric.get('beta', 0)),
                            'week_52_high': float(metric.get('52WeekHigh', 0)),
                            'week_52_low': float(metric.get('52WeekLow', 0)),
                            'volume_10day_avg': float(metric.get('10DayAverageTradingVolume', 0)),
                        })
                    time.sleep(0.2)  # API 레이트 제한 준수
            except Exception as e:
                logging.warning(f"Metrics 데이터 수집 실패 for {symbol}: {e}")
        
        # 계산된 추가 지표
        if enhanced_data['previous_close'] > 0:
            enhanced_data['daily_return'] = (enhanced_data['current_price'] - enhanced_data['previous_close']) / enhanced_data['previous_close']
            enhanced_data['volatility'] = abs(enhanced_data['daily_return'])
            enhanced_data['price_range'] = enhanced_data['high_price'] - enhanced_data['low_price']
            enhanced_data['price_position'] = (enhanced_data['current_price'] - enhanced_data['low_price']) / enhanced_data['price_range'] if enhanced_data['price_range'] > 0 else 0.5
        
        logging.info(f"'{symbol}' 확장 데이터 수집 성공: ${enhanced_data['current_price']:.2f} ({enhanced_data['percent_change']:+.2f}%)")
        return enhanced_data
        
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

    # 동적 종목 리스트 생성
    stock_symbols = get_dynamic_stock_list()
    logging.info(f"--- Finnhub 데이터 수집 시작 (무한 루프) ---")
    logging.info(f"선택된 종목 ({STOCK_SELECTION_METHOD}): {', '.join(stock_symbols)}")
    
    while True:
        logging.info(f"--- 새로운 수집 사이클 시작 ---")
        for symbol in stock_symbols:
            enhanced_data = get_enhanced_stock_data(symbol)
            send_to_kafka(producer, KAFKA_TOPIC, enhanced_data)
            time.sleep(2) # 각 심볼 요청 사이에 2초 지연 (API 더 많이 호출하므로)

        # 다음 사이클까지 대기 (동적 종목 수에 따라 조정)
        processing_time = len(stock_symbols) * 2  # 각 종목당 2초 + API 호출 시간
        remaining_wait_time = max(REQUEST_INTERVAL_SECONDS - processing_time, 10)  # 최소 10초 대기
        logging.info(f"--- 사이클 완료, 다음 사이클까지 {remaining_wait_time}초 대기... ---")
        time.sleep(remaining_wait_time)
    
    # 무한 루프이므로 아래 코드는 실행되지 않지만, 안전한 종료를 위해 남겨둘 수 있습니다.
    # producer.close()

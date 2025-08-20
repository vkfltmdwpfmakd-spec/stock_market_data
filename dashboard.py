import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
import time

# --- 페이지 설정 ---
st.set_page_config(
    page_title="주식 데이터 분석 대시보드",
    page_icon="📈",
    layout="wide",
)

# --- Spark Session 및 데이터 로드 ---
HDFS_PATH = "hdfs://namenode:8020/user/spark/analyzed_finnhub_data"

@st.cache_resource
def get_spark_session():
    return SparkSession.builder \
        .appName("StockDashboardReader") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()

@st.cache_data(ttl=300) # 5분마다 캐시 만료
def load_data_from_hdfs():
    """HDFS에서 Parquet 데이터를 읽어 Pandas 데이터프레임으로 변환합니다."""
    try:
        spark = get_spark_session()
        df = spark.read.parquet(HDFS_PATH)
        pandas_df = df.toPandas()
        
        # 데이터 타입 변환 및 정렬
        pandas_df['trade_date'] = pd.to_datetime(pandas_df['trade_date'])
        pandas_df = pandas_df.sort_values(by=['symbol', 'trade_date'])
        return pandas_df
    except Exception as e:
        # HDFS에 아직 데이터가 없을 경우 빈 프레임 반환
        if "Path does not exist" in str(e):
            return pd.DataFrame()
        st.error(f"HDFS 데이터 로딩 중 오류 발생: {e}")
        return pd.DataFrame()

# --- 대시보드 UI ---
st.title("📈 주식 데이터 분석 대시보드")

df = load_data_from_hdfs()

if df.empty:
    st.warning("아직 분석된 데이터가 없습니다. Airflow에서 `finnhub_stock_pipeline` DAG를 실행해주세요.")
else:
    # --- 인터랙티브 필터 ---
    st.sidebar.header("필터")
    all_symbols = df['symbol'].unique()
    selected_symbol = st.sidebar.selectbox("주식 종목을 선택하세요", all_symbols)

    # 선택된 종목 데이터 필터링
    symbol_df = df[df['symbol'] == selected_symbol]

    st.header(f"{selected_symbol} 분석 결과")

    # --- 라인 차트 ---
    st.subheader("일별 평균 가격 및 7일 이동평균선")
    chart_df = symbol_df.set_index('trade_date')[['daily_avg_price', '7_day_moving_average']]
    st.line_chart(chart_df)

    # --- 데이터 테이블 ---
    st.subheader("상세 데이터")
    st.dataframe(symbol_df)

# 자동 새로고침
placeholder = st.empty()
while True:
    with placeholder.container():
        st.info("대시보드는 5분마다 자동으로 새로고침됩니다.")
        time.sleep(300)
    st.rerun()

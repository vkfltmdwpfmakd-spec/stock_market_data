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

    st.header(f"{selected_symbol} 종합 분석 대시보드")

    # --- 주요 메트릭 카드 ---
    if not symbol_df.empty:
        latest_data = symbol_df.iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(
                "평균 가격", 
                f"${latest_data['average_price']:.2f}",
                f"{latest_data['avg_daily_return']*100:.2f}%" if pd.notna(latest_data.get('avg_daily_return')) else None
            )
        with col2:
            st.metric(
                "시가총액", 
                f"${latest_data['avg_market_cap']/1000:.1f}B" if pd.notna(latest_data.get('avg_market_cap')) else "N/A"
            )
        with col3:
            st.metric(
                "P/E 비율", 
                f"{latest_data['avg_pe_ratio']:.1f}" if pd.notna(latest_data.get('avg_pe_ratio')) else "N/A"
            )
        with col4:
            st.metric(
                "변동성", 
                f"{latest_data['avg_volatility']*100:.2f}%" if pd.notna(latest_data.get('avg_volatility')) else "N/A"
            )

    # --- 가격 차트 ---
    st.subheader("📈 가격 추이 및 이동평균선")
    
    # 기본 가격 차트 - 실제 컬럼명 사용
    price_cols = ['average_price', 'moving_avg_7_days']
    available_price_cols = [col for col in price_cols if col in symbol_df.columns]
    
    if available_price_cols:
        chart_df = symbol_df.set_index('trade_date')[available_price_cols]
        # 컬럼명을 한글로 변경
        column_rename = {}
        if 'average_price' in chart_df.columns:
            column_rename['average_price'] = '평균 가격'
        if 'moving_avg_7_days' in chart_df.columns:
            column_rename['moving_avg_7_days'] = '7일 이동평균'
        chart_df = chart_df.rename(columns=column_rename)
        st.line_chart(chart_df)
    else:
        st.warning("⚠️ 가격 데이터를 찾을 수 없습니다.")

    # --- 재무 지표 차트 ---
    financial_cols = ['avg_market_cap', 'avg_pe_ratio', 'avg_volatility']
    available_financial_cols = [col for col in financial_cols if col in symbol_df.columns and symbol_df[col].notna().any()]
    
    if available_financial_cols:
        st.subheader("📊 재무 지표 트렌드")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if 'avg_market_cap' in available_financial_cols:
                st.subheader("시가총액 (십억 달러)")
                market_cap_chart = symbol_df.set_index('trade_date')[['avg_market_cap']].copy()
                market_cap_chart['avg_market_cap'] = market_cap_chart['avg_market_cap'] / 1000  # 십억 단위로 변환
                st.line_chart(market_cap_chart)
        
        with col2:
            if 'avg_pe_ratio' in available_financial_cols:
                st.subheader("P/E 비율")
                pe_chart = symbol_df.set_index('trade_date')[['avg_pe_ratio']]
                st.line_chart(pe_chart)

    # --- 상세 데이터 테이블 ---
    st.subheader("📋 상세 분석 데이터")
    
    # 표시할 컬럼 선택
    display_cols = ['symbol', 'trade_date', 'average_price', 'moving_avg_7_days']
    if 'avg_market_cap' in symbol_df.columns:
        display_cols.append('avg_market_cap')
    if 'avg_pe_ratio' in symbol_df.columns:
        display_cols.append('avg_pe_ratio')
    if 'avg_volatility' in symbol_df.columns:
        display_cols.append('avg_volatility')
        
    display_df = symbol_df[display_cols].copy()
    
    # 컬럼명을 한글로 변경
    column_mapping = {
        'symbol': '종목',
        'trade_date': '날짜',
        'average_price': '평균가격',
        'moving_avg_7_days': '7일이평',
        'avg_market_cap': '시가총액(M)',
        'avg_pe_ratio': 'P/E비율',
        'avg_volatility': '변동성(%)'
    }
    
    display_df = display_df.rename(columns=column_mapping)
    
    # 시가총액을 백만 단위로, 변동성을 퍼센트로 표시
    if '시가총액(M)' in display_df.columns:
        display_df['시가총액(M)'] = display_df['시가총액(M)'].round(0)
    if '변동성(%)' in display_df.columns:
        display_df['변동성(%)'] = (display_df['변동성(%)'] * 100).round(2)
    
    st.dataframe(display_df, use_container_width=True)

# 새로고침 안내
st.info("💡 데이터를 새로고침하려면 페이지를 다시 로드하세요. (F5키 또는 새로고침 버튼)")
st.info("🔄 캐시는 5분마다 자동으로 업데이트됩니다.")

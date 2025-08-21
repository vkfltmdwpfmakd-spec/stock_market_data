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
        
        # Spark에서 중복 제거 (각 종목의 각 날짜에 대해 하나의 레코드만 유지)
        from pyspark.sql.functions import row_number, desc, col
        from pyspark.sql.window import Window
        
        window = Window.partitionBy("symbol", "trade_date").orderBy(
            desc("moving_avg_7_days"), 
            desc("average_price"),
            desc("data_points_per_day")
        )
        
        df_deduped = df.withColumn("row_num", row_number().over(window)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num")
        
        pandas_df = df_deduped.toPandas()
        
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

# 사이드바에 용어 설명 추가
with st.sidebar:
    with st.expander("📚 용어 및 지표 설명"):
        st.markdown("""
        ### 📊 주요 지표 설명
        
        **평균 가격**
        - 해당 날짜에 30초마다 수집된 모든 현재가(current_price)의 평균
        - 하루 동안 실시간으로 수집된 가격들의 산술평균
        - 예: 하루에 2,880번 수집(30초×24시간) → 2,880개 가격의 평균
        
        **시가총액 (Market Cap)**
        - 회사의 총 가치 = 주가 × 발행주식수
        - Finnhub API: 백만 달러 단위로 제공
        - 표시: 십억 달러(B) 단위로 변환하여 표시
        - 한국 돈: 1달러 = 약 1,350원 기준
        - 예: AAPL 3,376.6B = 약 4,559조원
        
        **P/E 비율 (Price-to-Earnings Ratio)**
        - 주가수익비율 = 주가 ÷ 주당순이익(EPS)
        - 주식이 얼마나 비싼지 판단하는 지표
        - 낮을수록 상대적으로 저평가
        - 일반적으로 15-25가 적정 수준
        
        **변동성 (Volatility)**
        - 주가의 일간 변화율 크기
        - 퍼센트(%) 단위로 표시
        - 높을수록 리스크가 큰 주식
        
        **7일 이동평균**
        - 최근 7일간 평균 가격의 이동평균
        - 단기 추세 파악에 활용
        - 현재가가 이동평균보다 높으면 상승 추세
        """)
        
        st.markdown("""
        ### ⏰ 데이터 수집 정보
        - **수집 주기**: 30초마다 실시간 수집
        - **분석 주기**: 15분마다 배치 분석  
        - **시간대**: UTC 기준 (한국시간 -9시간)
        - **데이터 출처**: Finnhub API
        """)
        
        st.markdown("""
        ### 💱 환율 참고 (근사치)
        - 1달러 ≈ 1,350원
        - 1십억달러(B) ≈ 1,350십억원 ≈ 1조 3500억원
        """)

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
            krw_price = latest_data['average_price'] * 1350  # USD to KRW
            st.metric(
                "평균 가격", 
                f"${latest_data['average_price']:.2f}",
                f"{latest_data['avg_daily_return']*100:.2f}%" if pd.notna(latest_data.get('avg_daily_return')) else None
            )
            st.caption(f"≈ {krw_price:,.0f}원")
            
        with col2:
            market_cap_val = latest_data.get('avg_market_cap', 0)
            if pd.notna(market_cap_val) and market_cap_val > 0:
                # Finnhub API는 백만 달러 단위로 제공 (예: 3376600 = 3.3766조 달러)
                market_cap_billion_usd = market_cap_val / 1000  # 백만 달러를 십억 달러로 변환
                market_cap_trillion_krw = market_cap_billion_usd * 1.35  # 1달러=1350원, 조원 단위
                st.metric("시가총액", f"${market_cap_billion_usd:.1f}B")
                st.caption(f"≈ {market_cap_trillion_krw:.1f}조원")
            else:
                st.metric("시가총액", "N/A")
                
        with col3:
            pe_ratio_val = latest_data.get('avg_pe_ratio', 0)
            st.metric(
                "P/E 비율", 
                f"{pe_ratio_val:.1f}" if pd.notna(pe_ratio_val) and pe_ratio_val > 0 else "N/A"
            )
            if pd.notna(pe_ratio_val) and pe_ratio_val > 0:
                if pe_ratio_val < 15:
                    st.caption("📊 저평가 구간")
                elif pe_ratio_val <= 25:
                    st.caption("📊 적정 구간")
                else:
                    st.caption("📊 고평가 구간")
        with col4:
            volatility_val = latest_data.get('avg_volatility', 0)
            st.metric(
                "변동성", 
                f"{volatility_val*100:.2f}%" if pd.notna(volatility_val) and volatility_val > 0 else "N/A"
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
    display_cols = ['symbol', 'collection_time', 'average_price', 'moving_avg_7_days']
    # collection_time이 없으면 trade_date 사용 (이전 버전 호환)
    if 'collection_time' not in symbol_df.columns:
        display_cols[1] = 'trade_date'
    
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
        'collection_time': '수집시간',
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

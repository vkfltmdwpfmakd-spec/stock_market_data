# Streamlit 대시보드 - Analyzer가 분석한 결과를 웹에서 볼 수 있게 해주는 화면
# HDFS에서 데이터 읽어와서 차트랑 표로 예쁘게 보여줌

import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time

# 페이지 기본 설정 - 와이드 레이아웃으로 차트가 더 잘 보임
st.set_page_config(
    page_title="주식 데이터 분석 대시보드",
    page_icon="📈",
    layout="wide",
)

# HDFS 경로 설정 - Analyzer가 분석 결과를 저장한 곳
HDFS_PATH = "hdfs://namenode:8020/user/spark/analyzed_finnhub_data"

@st.cache_resource
def get_spark_session():
    """스파크 세션 생성 - 웹 대시보드용이라 가벼운 설정"""
    return SparkSession.builder \
        .appName("StockDashboardReader") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()

@st.cache_data(ttl=300)  # 5분마다 캐시 갱신
def load_data_from_hdfs():
    """HDFS에서 분석된 데이터를 읽어서 pandas로 변환"""
    try:
        spark = get_spark_session()
        df = spark.read.parquet(HDFS_PATH)
        
        from pyspark.sql.functions import row_number, desc, col
        from pyspark.sql.window import Window

        # 데이터가 시간 단위(collection_time)까지 있으면 그걸로 중복 제거
        partition_cols = ["symbol"]
        if "collection_time" in df.columns:
            partition_cols.append("collection_time")
        else:
            partition_cols.append("trade_date")
        
        # 정렬 기준: 최신 데이터, 이동평균, 평균 가격 순
        order_by_cols = [
            desc("moving_avg_7_days"), 
            desc("average_price"),
            desc("data_count_for_avg")
        ]

        window = Window.partitionBy(partition_cols).orderBy(order_by_cols)
        
        df_deduped = df.withColumn("row_num", row_number().over(window)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num")
        
        pandas_df = df_deduped.toPandas()
        
        # 날짜/시간 타입 변환 및 정렬
        pandas_df['trade_date'] = pd.to_datetime(pandas_df['trade_date'])
        if 'collection_time' in pandas_df.columns:
            pandas_df['collection_time'] = pd.to_datetime(pandas_df['collection_time'])
            pandas_df = pandas_df.sort_values(by=['symbol', 'collection_time'])
        else:
            pandas_df = pandas_df.sort_values(by=['symbol', 'trade_date'])
            
        return pandas_df
    except Exception as e:
        if "Path does not exist" in str(e):
            return pd.DataFrame()  # 아직 분석 결과가 없을 때
        st.error(f"HDFS 데이터 로딩 중 오류 발생: {e}")
        return pd.DataFrame()

# 메인 대시보드 화면 구성
st.title("📈 주식 데이터 분석 대시보드")

# 사이드바에 용어 설명 - 처음 보는 사람도 이해하기 쉽게
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

# HDFS에서 데이터 로드 - 여기서 실제 데이터 가져옴
df = load_data_from_hdfs()

if df.empty:
    st.warning("아직 분석된 데이터가 없습니다. Airflow에서 `finnhub_stock_pipeline` DAG를 실행해주세요.")
else:
    # 사이드바 필터 - 종목 선택할 수 있게
    st.sidebar.header("필터")
    all_symbols = df['symbol'].unique()
    selected_symbol = st.sidebar.selectbox("주식 종목을 선택하세요", all_symbols)

    # 선택된 종목만 필터링해서 보여주기
    symbol_df = df[df['symbol'] == selected_symbol]

    st.header(f"{selected_symbol} 종합 분석 대시보드")

    # 주요 지표들을 카드 형태로 보여주기 - 한눈에 보기 편함
    if not symbol_df.empty:
        latest_data = symbol_df.iloc[-1]  # 가장 최신 데이터
        
        # 2x2 그리드로 지표 표시 (UI 개선)
        row1_col1, row1_col2 = st.columns(2)
        row2_col1, row2_col2 = st.columns(2)

        with row1_col1:
            krw_price = latest_data['average_price'] * 1350
            st.metric(
                "평균 가격", 
                f"${latest_data['average_price']:.2f}",
                f"{latest_data['avg_daily_return']*100:.2f}%" if pd.notna(latest_data.get('avg_daily_return')) else None
            )
            st.caption(f"≈ {krw_price:,.0f}원")

        with row1_col2:
            market_cap_val = latest_data.get('avg_market_cap', 0)
            if pd.notna(market_cap_val) and market_cap_val > 0:
                market_cap_billion_usd = market_cap_val / 1000
                market_cap_trillion_krw = market_cap_billion_usd * 1.35
                st.metric("시가총액", f"${market_cap_billion_usd:.1f}B")
                st.caption(f"≈ {market_cap_trillion_krw:.1f}조원")
            else:
                st.metric("시가총액", "N/A")

        with row2_col1:
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

        with row2_col2:
            volatility_val = latest_data.get('avg_volatility', 0)
            st.metric(
                "변동성", 
                f"{volatility_val*100:.2f}%" if pd.notna(volatility_val) and volatility_val > 0 else "N/A"
            )

    # 가격 차트 - 시간에 따른 가격 변화를 선으로 보여줌
    st.subheader("📈 가격 추이 및 이동평균선")
    
    # 차트에 넣을 컬럼들 확인
    price_cols = ['average_price', 'moving_avg_7_days']
    available_price_cols = [col for col in price_cols if col in symbol_df.columns]
    
    if available_price_cols:
        # collection_time이 있으면 시간별 차트, 없으면 날짜별 차트
        if 'collection_time' in symbol_df.columns:
            # 시간별 상세 차트 - 일중 변동성을 볼 수 있게
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('시간별 가격 변동', '일별 평균 및 이동평균'),
                vertical_spacing=0.1,
                row_heights=[0.7, 0.3]
            )
            
            # 상단: 시간별 세부 차트 (최근 3일)
            recent_data = symbol_df.sort_values('collection_time').tail(72 * 3)  # 최근 3일 (시간당 1개씩)
            
            fig.add_trace(
                go.Scatter(
                    x=recent_data['collection_time'],
                    y=recent_data['average_price'],
                    mode='lines+markers',
                    name='실시간 가격',
                    line=dict(color='#1f77b4', width=2),
                    marker=dict(size=4)
                ),
                row=1, col=1
            )
            
            # 하단: 일별 요약 차트
            daily_summary = symbol_df.groupby('trade_date').agg({
                'average_price': 'mean',
                'moving_avg_7_days': 'first'
            }).reset_index().sort_values('trade_date')
            
            fig.add_trace(
                go.Scatter(
                    x=daily_summary['trade_date'],
                    y=daily_summary['average_price'],
                    mode='lines+markers',
                    name='일평균 가격',
                    line=dict(color='#ff7f0e', width=3),
                    marker=dict(size=6)
                ),
                row=2, col=1
            )
            
            if 'moving_avg_7_days' in symbol_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=daily_summary['trade_date'],
                        y=daily_summary['moving_avg_7_days'],
                        mode='lines',
                        name='7일 이동평균',
                        line=dict(color='#2ca02c', width=2, dash='dash')
                    ),
                    row=2, col=1
                )
            
            fig.update_layout(
                height=600,
                title_text=f"{selected_symbol} 시간별 및 일별 가격 차트",
                showlegend=True
            )
            
            fig.update_xaxes(title_text="수집 시간", row=1, col=1)
            fig.update_xaxes(title_text="날짜", row=2, col=1)
            fig.update_yaxes(title_text="가격 ($)", row=1, col=1)
            fig.update_yaxes(title_text="가격 ($)", row=2, col=1)
            
            st.plotly_chart(fig, use_container_width=True)
            
        else:
            # 기존 날짜별 차트 유지 (구버전 호환)
            chart_df = symbol_df.set_index('trade_date')[available_price_cols]
            column_rename = {}
            if 'average_price' in chart_df.columns:
                column_rename['average_price'] = '평균 가격'
            if 'moving_avg_7_days' in chart_df.columns:
                column_rename['moving_avg_7_days'] = '7일 이동평균'
            chart_df = chart_df.rename(columns=column_rename)
            st.line_chart(chart_df)
    else:
        st.warning("⚠️ 가격 데이터를 찾을 수 없습니다.")
        
    # 시간대별 분석 - 장중 변동성 패턴 분석
    if 'collection_time' in symbol_df.columns and not symbol_df.empty:
        st.subheader("⏰ 시간대별 분석")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 시간대별 평균 가격
            hourly_analysis = symbol_df.copy()
            hourly_analysis['hour'] = pd.to_datetime(hourly_analysis['collection_time']).dt.hour
            hourly_avg = hourly_analysis.groupby('hour')['average_price'].mean().reset_index()
            
            fig_hour = px.bar(
                hourly_avg, 
                x='hour', 
                y='average_price',
                title='시간대별 평균 가격',
                labels={'hour': '시간 (UTC)', 'average_price': '평균 가격 ($)'}
            )
            fig_hour.update_layout(height=300)
            st.plotly_chart(fig_hour, use_container_width=True)
            
        with col2:
            # 시간대별 변동성
            if 'avg_volatility' in symbol_df.columns:
                hourly_vol = hourly_analysis.groupby('hour')['avg_volatility'].mean().reset_index()
                hourly_vol['avg_volatility_pct'] = hourly_vol['avg_volatility'] * 100
                
                fig_vol = px.bar(
                    hourly_vol,
                    x='hour',
                    y='avg_volatility_pct',
                    title='시간대별 평균 변동성',
                    labels={'hour': '시간 (UTC)', 'avg_volatility_pct': '변동성 (%)'}
                )
                fig_vol.update_layout(height=300)
                st.plotly_chart(fig_vol, use_container_width=True)
            else:
                st.info("변동성 데이터가 없습니다.")

    # 추가 재무지표 차트들 - 시가총액, PER 같은 것들
    financial_cols = ['avg_market_cap', 'avg_pe_ratio', 'avg_volatility']
    available_financial_cols = [col for col in financial_cols if col in symbol_df.columns and symbol_df[col].notna().any()]
    
    if available_financial_cols:
        st.subheader("📊 재무 지표 트렌드")
        
        col1, col2 = st.columns(2)  # 2열로 나눠서 차트 배치
        
        with col1:
            if 'avg_market_cap' in available_financial_cols:
                st.subheader("시가총액 (십억 달러)")
                market_cap_chart = symbol_df.set_index('trade_date')[['avg_market_cap']].copy()
                market_cap_chart['avg_market_cap'] = market_cap_chart['avg_market_cap'] / 1000  # 단위 변환
                st.line_chart(market_cap_chart)
        
        with col2:
            if 'avg_pe_ratio' in available_financial_cols:
                st.subheader("P/E 비율")
                pe_chart = symbol_df.set_index('trade_date')[['avg_pe_ratio']]
                st.line_chart(pe_chart)

    # 상세 데이터 테이블 - 숫자로만 보고 싶은 사람들용
    st.subheader("📋 상세 분석 데이터")
    
    # 테이블에 보여줄 컬럼들 선별 - 시간 데이터가 있으면 둘 다 표시
    display_cols = ['symbol']
    
    # 시간 관련 컬럼들 추가
    if 'collection_time' in symbol_df.columns:
        display_cols.extend(['collection_time', 'trade_date'])
    else:
        display_cols.append('trade_date')  # 구버전 호환
    
    # 시간 세부 정보 추가 (있는 경우)
    if 'collection_hour' in symbol_df.columns:
        display_cols.append('collection_hour')
    if 'collection_minute' in symbol_df.columns:
        display_cols.append('collection_minute')
    
    # 가격 정보 추가
    display_cols.extend(['average_price', 'moving_avg_7_days'])
    
    # 있는 컬럼들만 추가
    if 'avg_market_cap' in symbol_df.columns:
        display_cols.append('avg_market_cap')
    if 'avg_pe_ratio' in symbol_df.columns:
        display_cols.append('avg_pe_ratio')
    if 'avg_volatility' in symbol_df.columns:
        display_cols.append('avg_volatility')
    
    # 실제 존재하는 컬럼들만 선택
    available_display_cols = [col for col in display_cols if col in symbol_df.columns]
    display_df = symbol_df[available_display_cols].copy()
    
    # 최신 데이터부터 보여주기
    time_col = 'collection_time' if 'collection_time' in display_df.columns else 'trade_date'
    display_df = display_df.sort_values(time_col, ascending=False)
    
    # 컬럼명을 한국어로 바꿔서 보기 편하게
    column_mapping = {
        'symbol': '종목',
        'trade_date': '날짜',
        'collection_time': '수집시간',
        'collection_hour': '시',
        'collection_minute': '분',
        'average_price': '평균가격',
        'moving_avg_7_days': '7일이평',
        'avg_market_cap': '시가총액(M)',
        'avg_pe_ratio': 'P/E비율',
        'avg_volatility': '변동성(%)'
    }
    
    display_df = display_df.rename(columns=column_mapping)
    
    # 숫자 포맷 정리 - 보기 편하게 반올림
    if '시가총액(M)' in display_df.columns:
        display_df['시가총액(M)'] = display_df['시가총액(M)'].round(0)
    if '변동성(%)' in display_df.columns:
        display_df['변동성(%)'] = (display_df['변동성(%)'] * 100).round(2)
    
    st.dataframe(display_df, use_container_width=True)  # 테이블 전체 너비 사용

# 사용자 안내 메시지
st.info("💡 데이터를 새로고침하려면 페이지를 다시 로드하세요. (F5키 또는 새로고침 버튼)")
st.info("🔄 캐시는 5분마다 자동으로 업데이트됩니다.")  # 너무 자주 HDFS 읽으면 느려져서

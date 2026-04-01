import requests
import pandas as pd
import xml.etree.ElementTree as ET
import streamlit as st
from datetime import datetime, timedelta
import os
import urllib3

# --- [필수] 설정 ---
pd.set_option("styler.render.max_elements", 5000000)
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 삭제제

# --- 1. 설정 및 캐시 관리 ---
st.set_page_config(page_title="동탄 실거래가 V44 (해제거래 포함)", layout="wide")
CACHE_FILE = "dongtan_cache_all_v44.csv"
st.title("📊 동탄 실거래가 분석 개발자: 안재현")

# --- 2. 데이터 수집 엔진 ---
@st.cache_data(show_spinner=False)
def fetch_all_data():
    if os.path.exists(CACHE_FILE):
        try:
            return pd.read_csv(CACHE_FILE)
        except:
            pass

    service_key = st.secrets["SERVICE_KEY"]
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    lawd_cd = "41597"

    curr = datetime.now()
    months = [(curr - timedelta(days=30 * i)).strftime("%Y%m") for i in range(72)]

    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    for i, month in enumerate(months):
        status_text.text(f"🔍 동탄 데이터 전수 수집 중: {month} ({i + 1}/72)")
        progress_bar.progress((i + 1) / 72)
        params = {'serviceKey': service_key, 'LAWD_CD': lawd_cd, 'DEAL_YMD': month, 'numOfRows': '2000'}
        try:
            res = requests.get(url, params=params, verify=True, timeout=20)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                for item in root.findall('.//item'):
                    raw_price = item.findtext('dealAmount')
                    if not raw_price:
                        continue
                    all_data.append({
                        '해당동': (item.findtext('umdNm') or "").strip(),
                        '아파트명': (item.findtext('aptNm') or "").strip(),
                        '전용면적': round(float(item.findtext('excluUseAr') or 0), 2),
                        '층': item.findtext('floor') or "",
                        '거래금액_숫자': int(raw_price.strip().replace(',', '')),
                        '계약일자': f"{item.findtext('dealYear')}-{item.findtext('dealMonth').zfill(2)}-{item.findtext('dealDay').zfill(2)}",
                        '해제사유발생일': (item.findtext('cdealDay') or "").strip()
                    })
        except:
            continue

    df = pd.DataFrame(all_data)
    if not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)
        df.to_csv(CACHE_FILE, index=False)
    status_text.empty()
    progress_bar.empty()
    return df

# --- 3. UI 및 기간 설정 ---
today = datetime.now()
date_range = st.date_input("📅 분석 기간 선택", [today - timedelta(days=30), today])

if isinstance(date_range, (list, tuple)) and len(date_range) >= 1:
    s_ymd = date_range[0].strftime("%Y-%m-%d")
    e_ymd = date_range[1].strftime("%Y-%m-%d") if len(date_range) == 2 else s_ymd
else:
    s_ymd = e_ymd = date_range.strftime("%Y-%m-%d")

# --- 4. 분석 엔진 ---
df = fetch_all_data()

if df is not None and not df.empty:
    # [핵심] 1위, 2위 순위 계산은 '해제되지 않은' 데이터로만 수행
    valid_df = df[df['해제사유발생일'].isna() | (df['해제사유발생일'] == "")].copy()
    
    def get_rank_prices(group):
        sorted_prices = group.sort_values(by='거래금액_숫자', ascending=False)
        rank1_row = sorted_prices.iloc[0]
        rank2_row = sorted_prices.iloc[1] if len(sorted_prices) > 1 else sorted_prices.iloc[0]
        return pd.Series({
            '역사1위_가': rank1_row['거래금액_숫자'], 
            '역사1위_일': rank1_row['계약일자'],
            '역사2위_가': rank2_row['거래금액_숫자'], 
            '역사2위_일': rank2_row['계약일자']
        })

    if not valid_df.empty:
        rank_df = valid_df.groupby(['아파트명', '전용면적']).apply(get_rank_prices, include_groups=False).reset_index()
        # 원본 데이터(df)에 순위 정보를 붙임 (해제된 거래도 순위 비교는 가능하게 함)
        df = pd.merge(df, rank_df, on=['아파트명', '전용면적'], how='left')

        # 1위 대비 2위 비율 계산 (분모 0 방지)
        df['1위대비 2위'] = (df['역사2위_가'] / df['역사1위_가'] * 100).fillna(0)

        # 직전거래 및 변화량 계산 (해당 아파트/타입별)
        df = df.sort_values(by=['아파트명', '전용면적', '계약일자'])
        # 직전거래는 해제되지 않은 정상 거래를 기준으로 볼 수도 있으나, 여기서는 흐름 파악을 위해 순차 계산
        df['직전거래금액_숫자'] = df.groupby(['아파트명', '전용면적'])['거래금액_숫자'].shift(1)
        df['변화량_숫자'] = df['거래금액_숫자'] - df['직전거래금액_숫자']

        # 날짜 필터링
        p_df = df[(df['계약일자'] >= s_ymd) & (df['계약일자'] <= e_ymd)].copy()

        if not p_df.empty:
            st.markdown("---")
            f1, f2, f3 = st.columns(3)
            with f1:
                sel_d = st.selectbox("🏘️ 해당 동 선택", ["전체동"] + sorted(p_df['해당동'].unique().tolist()))
            filtered_by_dong = p_df if sel_d == "전체동" else p_df[p_df['해당동'] == sel_d]

            with f2:
                sel_a = st.selectbox("🏢 아파트 단지 선택", ["전체단지 보기"] + sorted(filtered_by_dong['아파트명'].unique().tolist()))
            filtered_by_apt = filtered_by_dong if sel_a == "전체단지 보기" else filtered_by_dong[filtered_by_dong['아파트명'] == sel_a]

            with f3:
                area_list = sorted(filtered_by_apt['전용면적'].unique().tolist())
                sel_area = st.selectbox("📏 전용면적 선택", ["전체타입 보기"] + [f"{a}㎡" for a in area_list])

            res = filtered_by_apt if sel_area == "전체타입 보기" else filtered_by_apt[filtered_by_apt['전용면적'] == float(sel_area.replace("㎡", ""))]

            res = res.sort_values(by=['계약일자'], ascending=False).reset_index(drop=True)
            res.insert(0, '순번', range(1, len(res) + 1))

            # 타입(평형) 변환 로직
            def format_type_pyeong(area):
                py = round(area / 3.3 * 1.3)
                return f"{int(area)}({py}평)"
            
            res['타입(평형)'] = res['전용면적'].apply(format_type_pyeong)

            # 포맷팅
            res['거래금액'] = res['거래금액_숫자'].apply(lambda x: f"{int(x):,} 만원")
            res['직전거래금액'] = res['직전거래금액_숫자'].apply(lambda x: f"{int(x):,} 만원" if pd.notna(x) else "-")
            res['역사적 1위(최고)'] = res.apply(lambda x: f"{int(x['역사1위_가']):,} 만원 ({x['역사1위_일']})" if pd.notna(x['역사1위_가']) else "-", axis=1)
            res['2위금액'] = res.apply(lambda x: f"{int(x['역사2위_가']):,} 만원 ({x['역사2위_일']})" if pd.notna(x['역사2위_가']) else "-", axis=1)
            res['변화량'] = res['변화량_숫자'].apply(lambda x: f"+{int(x):,} 만원" if x > 0 else (f"{int(x):,} 만원" if x < 0 else "0 만원") if pd.notna(x) else "-")

            # 특징 부여 (해제 거래 우선 표시)
            def get_feature(row):
                if pd.notna(row['해제사유발생일']) and row['해제사유발생일'] != "":
                    return '❌ 계약해제'
                if row['거래금액_숫자'] >= row['역사1위_가']: return '💎 전고돌파'
                if row['거래금액_숫자'] >= row['역사2위_가']: return '🥈 2위돌파'
                return ''
            res['특징'] = res.apply(get_feature, axis=1)

            # 컬럼 순서
            f_cols = ['순번', '해당동', '아파트명', '타입(평형)', '층', '거래금액', '변화량', '특징', '1위대비 2위', '2위금액', '역사적 1위(최고)', '계약일자']

            # 스타일 적용
            def style_rows(row):
                if row['특징'] == '❌ 계약해제':
                    return ['color: #adb5bd; text-decoration: line-through;'] * len(row) # 해제건은 회색+취소선
                return [''] * len(row)

            st.dataframe(
                res[f_cols].style.apply(style_rows, axis=1)
                .map(lambda v: 'color: red; font-weight: bold;' if str(v).startswith('+') else ('color: blue; font-weight: bold;' if str(v).startswith('-') and len(str(v)) > 1 else ''), subset=['변화량'])
                .map(lambda v: 'color: #ff4b4b; font-weight: bold;' if v == '❌ 계약해제' else ('color: #ffa500; font-weight: bold;' if v == '💎 전고돌파' else ''), subset=['특징'])
                .map(lambda v: 'color: red; font-weight: bold;' if isinstance(v, (int, float)) and v >= 100 else '', subset=['1위대비 2위']),
                column_config={
                    "1위대비 2위": st.column_config.ProgressColumn(
                        "1위대비 2위 (%)",
                        format="%.1f%%",
                        min_value=0,
                        max_value=100
                    )
                },
                height=700, use_container_width=True, hide_index=True
            )
        else:
            st.warning("해당 기간 내에 거래 데이터가 없습니다.")
else:
    st.error("데이터 수집에 실패했거나 데이터가 비어있습니다.")

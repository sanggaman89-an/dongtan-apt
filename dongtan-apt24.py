import requests
import pandas as pd
import xml.etree.ElementTree as ET
import streamlit as st
from datetime import datetime, timedelta
import os
import urllib3

# --- [필수] 설정 ---
pd.set_option("styler.render.max_elements", 5000000)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. 설정 및 캐시 관리 ---
st.set_page_config(page_title="동탄 실거래가 V43", layout="wide")
CACHE_FILE = "dongtan_cache_all_v43.csv"
st.title("📊 동탄 실거래가 정밀 분석 (Cloud 대응 버전)")

# --- 2. 데이터 수집 엔진 ---
@st.cache_data(show_spinner=False)
def fetch_all_data():
    if os.path.exists(CACHE_FILE):
        try:
            return pd.read_csv(CACHE_FILE)
        except:
            pass

    # API 키 (GitHub Secrets 등을 사용하지 않을 경우 직접 입력 확인 필요)
    service_key = "d0d96cc8b346473b0da1e093e53422254c6d6965636596bb9dba3b9e1f3f340c"
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    lawd_cd = "41597"

    curr = datetime.now()
    months = [(curr - timedelta(days=30 * i)).strftime("%Y%m") for i in range(72)]

    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    for i, month in enumerate(months):
        status_text.text(f"🔍 데이터 수집 중: {month} ({i + 1}/72)")
        progress_bar.progress((i + 1) / 72)
        params = {'serviceKey': service_key, 'LAWD_CD': lawd_cd, 'DEAL_YMD': month, 'numOfRows': '2000'}
        try:
            # timeout을 늘려 클라우드 지연 대응
            res = requests.get(url, params=params, verify=False, timeout=30)
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
        except Exception as e:
            continue

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
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

# [핵심 방어 로직] 데이터프레임이 비어있는지 먼저 확인
if df is not None and not df.empty:
    clean_df = df[df['해제사유발생일'].isna() | (df['해제사유발생일'] == "")].copy()

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

    if not clean_df.empty:
        rank_df = clean_df.groupby(['아파트명', '전용면적']).apply(get_rank_prices, include_groups=False).reset_index()
        clean_df = pd.merge(clean_df, rank_df, on=['아파트명', '전용면적'], how='left')

        clean_df['1위대비 2위'] = (clean_df['역사2위_가'] / clean_df['역사1위_가'] * 100).fillna(0)
        clean_df = clean_df.sort_values(by=['아파트명', '전용면적', '계약일자'])
        clean_df['직전거래금액_숫자'] = clean_df.groupby(['아파트명', '전용면적'])['거래금액_숫자'].shift(1)
        clean_df['변화량_숫자'] = clean_df['거래금액_숫자'] - clean_df['직전거래금액_숫자']

        p_df = clean_df[(clean_df['계약일자'] >= s_ymd) & (clean_df['계약일자'] <= e_ymd)].copy()

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
                # [오류 해결 포인트] filtered_by_apt에 컬럼이 있는지 확인 후 추출
                if '전용면적' in filtered_by_apt.columns:
                    area_list = sorted(filtered_by_apt['전용면적'].unique().tolist())
                else:
                    area_list = []
                sel_area = st.selectbox("📏 타입 선택", ["전체타입 보기"] + [f"{a}㎡" for a in area_list])

            # 필터 적용
            res = filtered_by_apt
            if sel_area != "전체타입 보기":
                target_area = float(sel_area.replace("㎡", ""))
                res = filtered_by_apt[filtered_by_apt['전용면적'] == target_area]

            res = res.sort_values(by=['계약일자'], ascending=False).reset_index(drop=True)
            res.insert(0, '순번', range(1, len(res) + 1))

            def format_type_pyeong(area):
                py = round(area / 3.3 * 1.3)
                return f"{int(area)}({py}평)"
            
            res['타입(평형)'] = res['전용면적'].apply(format_type_pyeong)
            res['거래금액'] = res['거래금액_숫자'].apply(lambda x: f"{int(x):,} 만원")
            res['직전거래금액'] = res['직전거래금액_숫자'].apply(lambda x: f"{int(x):,} 만원" if pd.notna(x) else "-")
            res['역사적 1위(최고)'] = res.apply(lambda x: f"{int(x['역사1위_가']):,} 만원 ({x['역사1위_일']})", axis=1)
            res['변화량'] = res['변화량_숫자'].apply(lambda x: f"+{int(x):,} 만원" if x > 0 else (f"{int(x):,} 만원" if x < 0 else "0 만원") if pd.notna(x) else "-")

            f_cols = ['순번', '해당동', '아파트명', '타입(평형)', '층', '거래금액', '변화량', '직전거래금액', '계약일자']

            st.dataframe(res[f_cols], height=700, use_container_width=True, hide_index=True)
        else:
            st.warning("⚠️ 선택한 기간에 실거래 내역이 없습니다.")
    else:
        st.warning("⚠️ 유효한 거래 데이터를 찾을 수 없습니다.")
else:
    st.error("❌ 데이터를 불러오지 못했습니다. API 키나 서버 연결을 확인하세요.")

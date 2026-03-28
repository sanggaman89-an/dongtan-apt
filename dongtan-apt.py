import requests
import pandas as pd
import xml.etree.ElementTree as ET
import streamlit as st
from datetime import datetime, timedelta
import os
import urllib3

# --- [필수] 설정 ---
# 불필요한 공백 문자를 제거하고 표준 코드로 정리했습니다.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. 설정 및 캐시 관리 ---
st.set_page_config(page_title="동탄 실거래가 V36", layout="wide")
CACHE_FILE = "dongtan_cache_all_v36.csv"
st.title("📊 동탄구 실거래가 정밀 분석 (반등 최고가 vs 역사적 최고가)")

# --- 2. 데이터 수집 엔진 ---
@st.cache_data(show_spinner=False)
def fetch_all_data():
    if os.path.exists(CACHE_FILE):
        try: return pd.read_csv(CACHE_FILE)
        except: pass
    
    # 서비스키와 URL 설정
    service_key = "d0d96cc8b346473b0da1e093e53422254c6d6965636596bb9dba3b9e1f3f340c"
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    lawd_cd = "41597"
    
    curr = datetime.now()
    months = [(curr - timedelta(days=30*i)).strftime("%Y%m") for i in range(72)]
    
    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, month in enumerate(months):
        status_text.text(f"🔍 데이터 수집 중: {month} ({i+1}/72)")
        progress_bar.progress((i + 1) / 72)
        params = {'serviceKey': service_key, 'LAWD_CD': lawd_cd, 'DEAL_YMD': month, 'numOfRows': '2000'}
        try:
            res = requests.get(url, params=params, verify=False, timeout=20)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                for item in root.findall('.//item'):
                    raw_price = item.findtext('dealAmount')
                    if not raw_price: continue
                    all_data.append({
                        '해당동': (item.findtext('umdNm') or "").strip(),
                        '아파트명': (item.findtext('aptNm') or "").strip(),
                        '전용면적': round(float(item.findtext('excluUseAr') or 0), 2),
                        '층': item.findtext('floor') or "",
                        '거래금액_숫자': int(raw_price.strip().replace(',', '')),
                        '계약일자': f"{item.findtext('dealYear')}-{item.findtext('dealMonth').zfill(2)}-{item.findtext('dealDay').zfill(2)}",
                        '거래유형': item.findtext('dealingGbn') or '-',
                        '해제사유발생일': (item.findtext('cdealDay') or "").strip(),
                        '건축년도': item.findtext('buildYear') or ""
                    })
        except: continue
    
    df = pd.DataFrame(all_data)
    if not df.empty: 
        df = df.drop_duplicates().reset_index(drop=True)
        df.to_csv(CACHE_FILE, index=False)
    status_text.empty()
    progress_bar.empty()
    return df

# --- 3. UI 및 날짜 범위 ---
today = datetime.now()
six_years_ago = today - timedelta(days=365 * 6)

col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    date_range = st.date_input("📅 조회 기간", [six_years_ago, today])
with col3:
    if st.button("🔄 데이터 전체 재수집"):
        if os.path.exists(CACHE_FILE): os.remove(CACHE_FILE)
        st.cache_data.clear()
        st.rerun()

# 날짜 안전하게 가져오기
if isinstance(date_range, list) and len(date_range) == 2:
    s_ymd, e_ymd = date_range[0].strftime("%Y-%m-%d"), date_range[1].strftime("%Y-%m-%d")
else:
    s_ymd = e_ymd = date_range[0].strftime("%Y-%m-%d") if isinstance(date_range, list) else date_range.strftime("%Y-%m-%d")

# --- 4. 분석 엔진 ---
df = fetch_all_data()
if not df.empty:
    clean_df = df[df['해제사유발생일'] == ""].copy()
    clean_df = clean_df.sort_values(by=['아파트명', '전용면적', '계약일자']).reset_index(drop=True)
    
    # 역사적 최고가
    hist_max = clean_df.groupby(['아파트명', '전용면적'])['거래금액_숫자'].agg(['max', 'idxmax']).reset_index()
    hist_max.columns = ['아파트명', '전용면적', '역사최고_가', 'idx']
    hist_max['역사최고_일'] = clean_df.loc[hist_max['idx'], '계약일자'].values
    
    clean_df = pd.merge(clean_df, hist_max[['아파트명', '전용면적', '역사최고_가', '역사최고_일']], on=['아파트명', '전용면적'], how='left')

    # 반등 최고가 (역사적 최고가 이후)
    rebound_list = []
    for (name, area), group in clean_df.groupby(['아파트명', '전용면적']):
        h_date = group['역사최고_일'].iloc[0]
        after_df = group[group['계약일자'] > h_date]
        if not after_df.empty:
            r_max = after_df.loc[after_df['거래금액_숫자'].idxmax()]
            rebound_list.append({'아파트명': name, '전용면적': area, '반등최고_가': r_max['거래금액_숫자'], '반등최고_일': r_max['계약일자']})
        else:
            rebound_list.append({'아파트명': name, '전용면적': area, '반등최고_가': group['역사최고_가'].iloc[0], '반등최고_일': group['역사최고_일'].iloc[0]})
    
    rebound_df = pd.DataFrame(rebound_list)
    clean_df = pd.merge(clean_df, rebound_df, on=['아파트명', '전용면적'], how='left')

    # 회복률 계산
    clean_df['회복률'] = (clean_df['거래금액_숫자'] / clean_df['반등최고_가'] * 100).fillna(0)
    clean_df['전일가_숫자'] = clean_df.groupby(['아파트명', '전용면적'])['거래금액_숫자'].shift(1)
    clean_df['변화량_숫자'] = clean_df['거래금액_숫자'] - clean_df['전일가_숫자']
    
    p_df = clean_df[(clean_df['계약일자'] >= s_ymd) & (clean_df['계약일자'] <= e_ymd)].copy()
    
    if not p_df.empty:
        st.markdown("---")
        f1, f2 = st.columns(2)
        with f1: sel_d = st.selectbox("🏘️ 해당 동 선택", ["전체동"] + sorted(p_df['해당동'].unique().tolist()))
        d_df = p_df if sel_d == "전체동" else p_df[p_df['해당동'] == sel_d]
        with f2: sel_a = st.selectbox("🏢 아파트 단지 선택", ["전체단지 보기"] + sorted(d_df['아파트명'].unique().tolist()))
        
        res = d_df if sel_a == "전체단지 보기" else d_df[d_df['아파트명'] == sel_a]
        res = res.sort_values(by=['계약일자'], ascending=False).reset_index(drop=True)
        res.insert(0, '순번', range(1, len(res) + 1))
        
        # 가공
        res['거래금액'] = res['거래금액_숫자'].apply(lambda x: f"{int(x):,} 만원")
        res['반등 최고가'] = res.apply(lambda x: f"{int(x['반등최고_가']):,} 만원 ({x['반등최고_일']})", axis=1)
        res['역사적 최고가'] = res.apply(lambda x: f"{int(x['역사최고_가']):,} 만원 ({x['역사최고_일']})", axis=1)
        res['변화량'] = res['변화량_숫자'].apply(lambda x: f"+{int(x):,} 만원" if x > 0 else (f"{int(x):,} 만원" if x < 0 else "0 만원") if pd.notna(x) else "-")
        res['특징'] = res.apply(lambda x: '💎 전고돌파' if x['거래금액_숫자'] >= x['역사최고_가'] else ('🔥 반등경신' if x['거래금액_숫자'] >= x['반등최고_가'] else ''), axis=1)

        f_cols = ['순번', '해당동', '아파트명', '전용면적', '층', '거래금액', '변화량', '특징', '반등 최고가', '회복률', '역사적 최고가', '계약일자']
        
        # --- 핵심 수정 부분 (width='stretch' 제거 및 use_container_width 사용) ---
        st.dataframe(
            res[f_cols].style.map(lambda v: 'color: red; font-weight: bold;' if str(v).startswith('+') else ('color: blue; font-weight: bold;' if str(v).startswith('-') and len(str(v))>1 else ''), subset=['변화량']),
            column_config={"회복률": st.column_config.ProgressColumn("회복률 (%)", format="%.1f%%", min_value=0, max_value=100)},
            height=600, 
            use_container_width=True, 
            hide_index=True
        )
    else: st.warning("조회 기간 내 거래가 없습니다.")

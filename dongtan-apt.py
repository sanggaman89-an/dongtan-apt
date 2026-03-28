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
st.set_page_config(page_title="동탄 실거래가 V36 (반등최고가 추적)", layout="wide")
CACHE_FILE = "dongtan_cache_all_v36.csv"
st.title("📊 동탄구 실거래가 정밀 분석 (반등 최고가 vs 역사적 최고가)")

# --- 2. 데이터 수집 엔진 ---
@st.cache_data(show_spinner=False)
def fetch_all_data():
    if os.path.exists(CACHE_FILE):
        try: return pd.read_csv(CACHE_FILE)
        except: pass
    
    service_key = "d0d96cc8b346473b0da1e093e53422254c6d6965636596bb9dba3b9e1f3f340c"
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    lawd_cd = "41597"
    
    curr = datetime.now()
    months = [(curr - timedelta(days=30*i)).strftime("%Y%m") for i in range(72)]
    
    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, month in enumerate(months):
        status_text.text(f"🔍 전수 수집 및 클린 데이터 분석 중: {month} ({i+1}/72)")
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
    st.write("")
    if st.button("🔄 데이터 전체 재수집 (이중 최고가 분석)"):
        if os.path.exists(CACHE_FILE): os.remove(CACHE_FILE)
        st.cache_data.clear()
        st.rerun()

s_ymd = date_range[0].strftime("%Y-%m-%d")
e_ymd = date_range[1].strftime("%Y-%m-%d") if len(date_range) == 2 else s_ymd

# --- 4. [고급] 분석 엔진 (이중 최고가 로직) ---
df = fetch_all_data()
if not df.empty:
    # 해제 거래 제외
    clean_df = df[df['해제사유발생일'] == ""].copy()
    clean_df = clean_df.sort_values(by=['아파트명', '전용면적', '계약일자']).reset_index(drop=True)
    
    # [로직 1] 역사적 최고가 찾기 (전체 기간 1등)
    def get_hist_max(group):
        max_idx = group['거래금액_숫자'].idxmax()
        return pd.Series([group.loc[max_idx, '거래금액_숫자'], group.loc[max_idx, '계약일자']], 
                         index=['역사최고_가', '역사최고_일'])
    
    hist_max_df = clean_df.groupby(['아파트명', '전용면적']).apply(get_hist_max, include_groups=False).reset_index()
    clean_df = pd.merge(clean_df, hist_max_df, on=['아파트명', '전용면적'], how='left')

    # [로직 2] 반등 최고가 찾기 (역사적 최고가 일자 이후 중 최고)
    def get_rebound_max(group):
        h_date = group['역사최고_일'].iloc[0]
        # 역사적 최고가 날짜 이후의 거래만 필터링
        after_df = group[group['계약일자'] > h_date]
        if not after_df.empty:
            r_idx = after_df['거래금액_숫자'].idxmax()
            return pd.Series([after_df.loc[r_idx, '거래금액_숫자'], after_df.loc[r_idx, '계약일자']], 
                             index=['반등최고_가', '반등최고_일'])
        else:
            # 역사적 최고가가 곧 현재까지의 최고인 경우
            return pd.Series([group['역사최고_가'].iloc[0], group['역사최고_일'].iloc[0]], 
                             index=['반등최고_가', '반등최고_일'])

    rebound_max_df = clean_df.groupby(['아파트명', '전용면적']).apply(get_rebound_max, include_groups=False).reset_index()
    clean_df = pd.merge(clean_df, rebound_max_df, on=['아파트명', '전용면적'], how='left')

    # [로직 3] 상승장 대응 (반등최고가 > 역사최고가인 경우 갱신)
    clean_df['최종_비교가'] = clean_df.apply(lambda x: x['반등최고_가'] if x['반등최고_가'] > x['역사최고_가'] else x['역사최고_가'], axis=1)

    # [로직 4] 회복률 및 변화량 계산
    clean_df['회복률'] = (clean_df['거래금액_숫자'] / clean_df['반등최고_가'] * 100).fillna(0)
    clean_df['전일가_숫자'] = clean_df.groupby(['아파트명', '전용면적'])['거래금액_숫자'].shift(1)
    clean_df['변화량_숫자'] = clean_df['거래금액_숫자'] - clean_df['전일가_숫자']
    
    # 필터링
    p_df = clean_df[(clean_df['계약일자'] >= s_ymd) & (clean_df['계약일자'] <= e_ymd)].copy()
    
    if not p_df.empty:
        st.markdown("---")
        f1, f2 = st.columns(2)
        with f1:
            sel_d = st.selectbox("🏘️ 해당 동 선택", ["전체동"] + sorted(p_df['해당동'].unique().tolist()))
        d_df = p_df if sel_d == "전체동" else p_df[p_df['해당동'] == sel_d]
        with f2:
            sel_a = st.selectbox("🏢 아파트 단지 선택", ["전체단지 보기"] + sorted(d_df['아파트명'].unique().tolist()))
            
        res = d_df if sel_a == "전체단지 보기" else d_df[d_df['아파트명'] == sel_a]
        res = res.sort_values(by=['계약일자', '회복률'], ascending=[False, False]).reset_index(drop=True)
        res.insert(0, '순번', range(1, len(res) + 1))
        
        # 출력 가공
        res['거래금액'] = res['거래금액_숫자'].apply(lambda x: f"{int(x):,} 만원")
        res['전일가'] = res['전일가_숫자'].apply(lambda x: f"{int(x):,} 만원" if pd.notna(x) else "-")
        res['반등 최고가'] = res.apply(lambda x: f"{int(x['반등최고_가']):,} 만원 ({x['반등최고_일']})", axis=1)
        res['역사적 최고가'] = res.apply(lambda x: f"{int(x['역사최고_가']):,} 만원 ({x['역사최고_일']})", axis=1)
        res['변화량'] = res['변화량_숫자'].apply(lambda x: f"+{int(x):,} 만원" if x > 0 else (f"{int(x):,} 만원" if x < 0 else "0 만원") if pd.notna(x) else "-")
        res['특징'] = res.apply(lambda x: '전고점 돌파' if x['거래금액_숫자'] > x['역사최고_가'] else ('반등고점 돌파' if x['거래금액_숫자'] > x['반등최고_가'] else ''), axis=1)

        f_cols = ['순번', '해당동', '아파트명', '전용면적', '층', '거래금액', '변화량', '특징', '반등 최고가', '회복률', '역사적 최고가', '계약일자', '거래유형']
        
        st.dataframe(
            res[f_cols].style.map(lambda v: 'color: red; font-weight: bold;' if str(v).startswith('+') else ('color: blue; font-weight: bold;' if str(v).startswith('-') and len(str(v))>1 else ''), subset=['변화량'])
                             .map(lambda v: 'color: red; font-weight: bold;' if v > 100 else '', subset=['회복률']),
            column_config={"회복률": st.column_config.ProgressColumn("회복률 (%)", format="%.1f%%", min_value=0, max_value=100)},
            height=600, width='stretch', hide_index=True
        )
    else: st.warning("유효 거래 내역 없음")

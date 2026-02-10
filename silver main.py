import pandas as pd
import requests
import io
import os
import re
import sys
from datetime import datetime

# ---------------------------------------------------------
# [설정] 엑셀 라이브러리 확인
# ---------------------------------------------------------
try:
    import openpyxl
except ImportError:
    print("!!! 경고: 'openpyxl' 라이브러리가 필요합니다. (pip install openpyxl)")
    sys.exit(1)

# ---------------------------------------------------------
# 1. 파일 다운로드 및 기본 데이터 수집 (기존과 동일)
# ---------------------------------------------------------
url = "https://www.cmegroup.com/delivery_reports/Silver_stocks.xls"
headers = {"User-Agent": "Mozilla/5.0"}

print("--- [1단계] 데이터 다운로드 및 처리 ---")
# ... (다운로드 및 파싱 로직은 위와 동일하게 수행된다고 가정하고 핵심 로직으로 넘어갑니다) ...
# 전체 코드를 복사해서 쓰실 수 있도록 다운로드 부분도 포함합니다.

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    raw_data = response.content.decode('utf-8', errors='ignore')
    try:
        dfs = pd.read_html(io.StringIO(raw_data))
        df_raw = dfs[0]
    except:
        df_raw = pd.read_excel(io.BytesIO(response.content), engine='xlrd')
except Exception as e:
    print(f"오류: {e}")
    sys.exit(1)

data_rows = []
activity_date = None
temp_depository = "" 
is_silver = False
exclude_list = ["TOTAL", "TROY OUNCE", "REPORT DATE", "ACTIVITY DATE", "NAN", "NEW YORK", "COMEX"]

def clean_val(x):
    s = str(x).replace(',', '').replace('nan', '0').replace('None', '0')
    try: return float(s)
    except: return 0.0

for index, row in df_raw.iterrows():
    vals = [str(v).strip() for v in row.values]
    first_val = vals[0]

    if not activity_date:
        match = re.search(r'Activity Date:\s*(\d{1,2}/\d{1,2}/\d{4})', " ".join(vals))
        if match: activity_date = match.group(1)

    if "SILVER" in first_val.upper():
        is_silver = True
        continue
    if not is_silver: continue
    if first_val.upper() == "DEPOSITORY": continue

    if first_val in ["Registered", "Eligible"]:
        if not temp_depository: continue
        try:
            data_rows.append({
                'Date': activity_date,
                'Region_Type': f"{temp_depository} {first_val}",
                'PREV_TOTAL': clean_val(row.iloc[2]),
                'RECEIVED': clean_val(row.iloc[3]),
                'WITHDRAWN': clean_val(row.iloc[4]),
                'NET_CHANGE': clean_val(row.iloc[5]),
                'ADJUSTMENT': clean_val(row.iloc[6]),
                'TOTAL_TODAY': clean_val(row.iloc[7])
            })
        except: continue
    elif first_val != "nan" and len(first_val) > 3:
        if not any(k in first_val.upper() for k in exclude_list):
            if not any(char.isdigit() for char in first_val):
                temp_depository = first_val

# ---------------------------------------------------------
# 2. 데이터 병합 및 월간 통계 생성 (핵심 추가 부분)
# ---------------------------------------------------------
print("\n--- [2단계] 데이터 병합 및 월간 통계 계산 ---")
excel_file = 'silver_daily_report.xlsx'

if data_rows:
    new_df = pd.DataFrame(data_rows)
    new_df = new_df[~new_df['Region_Type'].str.contains("TOTAL", case=False, na=False)]
    
    # 2-1. 기존 데이터 로드 (History 병합)
    full_df = new_df
    if os.path.exists(excel_file):
        try:
            existing_df = pd.read_excel(excel_file, sheet_name='Daily_Data')
            # 날짜 형식 통일 (문자열 -> 날짜객체)
            existing_df['Date'] = pd.to_datetime(existing_df['Date'])
            new_df['Date'] = pd.to_datetime(new_df['Date'])
            
            # 오늘 날짜가 없으면 추가
            current_date = pd.to_datetime(activity_date)
            if current_date not in existing_df['Date'].values:
                full_df = pd.concat([existing_df, new_df], ignore_index=True)
                print(f"-> 기존 데이터에 {activity_date} 추가 완료")
            else:
                full_df = existing_df
                print(f"-> {activity_date} 데이터가 이미 존재하여 기존 데이터를 사용합니다.")
        except:
            full_df = new_df

    # 2-2. 월별 그룹화 (Monthly Stats)
    # 날짜 컬럼을 Datetime으로 변환
    full_df['Date'] = pd.to_datetime(full_df['Date'])
    full_df['YearMonth'] = full_df['Date'].dt.to_period('M') # 예: 2026-01

    # (A) 각 항목(Region_Type)별 월간 합계
    # RECEIVED, WITHDRAWN은 '합계(Sum)', TOTAL_TODAY는 '월말잔고(Last)'를 구해야 함
    monthly_details = full_df.groupby(['YearMonth', 'Region_Type']).agg({
        'RECEIVED': 'sum',
        'WITHDRAWN': 'sum',
        'TOTAL_TODAY': 'last'  # 그 달의 마지막 기록을 재고로 간주
    }).reset_index()

    # (B) Registered / Eligible 구분 및 전체 합계 계산
    # Region_Type에서 Status 추출
    pattern = r'^(.*)\s+(Registered|Eligible)$'
    monthly_details[['Depository', 'Status']] = monthly_details['Region_Type'].str.extract(pattern)
    
    # 월별, 상태별(Registered/Eligible) 총 재고 합계 (Grand Total)
    monthly_grand_total = monthly_details.groupby(['YearMonth', 'Status'])['TOTAL_TODAY'].sum().reset_index()
    
    # 보기 좋게 피벗 (행: 월, 열: 상태, 값: 재고합계)
    grand_total_pivot = monthly_grand_total.pivot(index='YearMonth', columns='Status', values='TOTAL_TODAY').reset_index()
    grand_total_pivot['Grand_Total'] = grand_total_pivot.get('Registered', 0) + grand_total_pivot.get('Eligible', 0)
    
    # 최신순 정렬
    grand_total_pivot = grand_total_pivot.sort_values(by='YearMonth', ascending=False)
    monthly_details = monthly_details.sort_values(by=['YearMonth', 'Region_Type'], ascending=[False, True])

    print("\n[월간 요약 미리보기 - Grand Total]")
    print(grand_total_pivot.head())

    # ---------------------------------------------------------
    # 3. 엑셀 저장 (시트 3개: Daily / Summary / Monthly)
    # ---------------------------------------------------------
    print("\n--- [3단계] 엑셀 파일 저장 ---")
    
    # 저장 전 날짜 컬럼 문자열로 변환 (엑셀 호환성)
    full_df['YearMonth'] = full_df['YearMonth'].astype(str)
    monthly_details['YearMonth'] = monthly_details['YearMonth'].astype(str)
    grand_total_pivot['YearMonth'] = grand_total_pivot['YearMonth'].astype(str)

    # 당일 요약용 (기존 기능)
    summary_prep = new_df.copy()
    summary_prep[['Depository', 'Status']] = summary_prep['Region_Type'].str.extract(pattern)
    summary_day = summary_prep.pivot_table(index='Depository', columns='Status', values='TOTAL_TODAY', aggfunc='sum', fill_value=0)
    if 'Registered' not in summary_day.columns: summary_day['Registered'] = 0
    if 'Eligible' not in summary_day.columns: summary_day['Eligible'] = 0
    summary_day['Total_Stock'] = summary_day['Registered'] + summary_day['Eligible']

    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        # 시트 1: 전체 일별 데이터
        full_df.drop(columns=['YearMonth'], errors='ignore').to_excel(writer, sheet_name='Daily_Data', index=False)
        
        # 시트 2: 오늘 기준 창고별 요약
        summary_day.to_excel(writer, sheet_name='Today_Summary')
        
        # 시트 3: 월간 통계 (Grand Total + 상세)
        # (1) Grand Total 먼저 쓰기
        grand_total_pivot.to_excel(writer, sheet_name='Monthly_Stats', startrow=0, index=False)
        
        # (2) 한 칸 띄우고 상세 내역 쓰기
        start_row = len(grand_total_pivot) + 4
        writer.sheets['Monthly_Stats'].cell(row=start_row, column=1).value = ">>> [상세] 창고별 월간 입출고 및 기말 재고"
        monthly_details[['YearMonth', 'Region_Type', 'RECEIVED', 'WITHDRAWN', 'TOTAL_TODAY']].to_excel(
            writer, sheet_name='Monthly_Stats', startrow=start_row, index=False
        )

    print(f"성공: {excel_file} 저장 완료.")
    print(" -> 'Monthly_Stats' 시트에서 월간 합계 및 Registered/Eligible 총계를 확인하세요.")

else:
    print("실패: 처리할 데이터가 없습니다.")

import streamlit as st
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import random
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime 
import gspread
from gspread.exceptions import APIError as GspreadAPIError
from google.oauth2.service_account import Credentials

def _diagnose_gsheet_setup() -> str:
    """Google Sheets 연동 환경 진단 요약 문자열을 생성합니다."""
    messages = []
    try:
        if "gcp_service_account" not in st.secrets:
            messages.append("- secrets에 [gcp_service_account] 섹션이 없음 (.streamlit/secrets.toml 확인)")
            return "\n".join(messages)

        gcp = dict(st.secrets["gcp_service_account"])  # 복사본
        required = [
            "type",
            "project_id",
            "private_key_id",
            "private_key",
            "client_email",
            "client_id",
            "token_uri",
        ]
        missing = [k for k in required if k not in gcp or not gcp[k]]
        if missing:
            messages.append(f"- 누락된 키: {', '.join(missing)}")

        pk = str(gcp.get("private_key", ""))
        if not pk.startswith("-----BEGIN PRIVATE KEY-----"):
            messages.append("- private_key 형식 오류: PEM 헤더가 없음")
        if "\\n" not in gcp.get("private_key", "") and "\n" not in pk:
            messages.append("- private_key 줄바꿈 누락 가능성: 로컬 TOML에서는 \\n 로 이스케이프 필요")

        email = str(gcp.get("client_email", ""))
        if not email.endswith("iam.gserviceaccount.com"):
            messages.append("- client_email 값이 서비스 계정 이메일 형식이 아님")

        if not messages:
            messages.append("- secrets 형식은 정상으로 보임. Sheets/Drive API 활성화 및 시트 공유 권한 확인 필요")
    except Exception as e:
        messages.append(f"- 진단 중 예외 발생: {e}")
    return "\n".join(messages)

def _format_gspread_error(err: Exception) -> str:
    """gspread 예외를 읽기 쉬운 문자열로 변환합니다."""
    try:
        # gspread APIError인 경우 상세 파싱
        if isinstance(err, GspreadAPIError):
            status_code = None
            reason = None
            message = None

            try:
                resp = getattr(err, "response", None)
                if resp is not None:
                    status_code = getattr(resp, "status_code", None)
                    # JSON 본문 파싱 시도
                    try:
                        data = resp.json()
                        err_obj = data.get("error", {}) if isinstance(data, dict) else {}
                        message = err_obj.get("message")
                        # Google API 표준 에러 구조에서 reason 추출
                        details = err_obj.get("errors") or []
                        if isinstance(details, list) and details:
                            reason = details[0].get("reason")
                    except Exception:
                        message = getattr(resp, "text", None)
            except Exception:
                pass

            parts = []
            if status_code is not None:
                parts.append(f"status={status_code}")
            if reason:
                parts.append(f"reason={reason}")
            if message:
                parts.append(f"message={message}")
            if not parts:
                parts.append(str(err))
            return "; ".join(parts)

        # 일반 예외는 문자열화
        return str(err)
    except Exception:
        return str(err)

def get_gsheet_client():
    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        # secrets 존재 확인
        if "gcp_service_account" not in st.secrets:
            st.error("Streamlit secrets에 'gcp_service_account' 설정이 없습니다. .streamlit/secrets.toml을 확인하세요.")
            return None

        gcp_dict = dict(st.secrets["gcp_service_account"])  # 복사본 사용

        # 필수 키 존재 확인
        required_keys = [
            "type",
            "project_id",
            "private_key_id",
            "private_key",
            "client_email",
            "client_id",
            "token_uri",
        ]
        missing = [k for k in required_keys if k not in gcp_dict or not gcp_dict[k]]
        if missing:
            st.error(f"gcp_service_account 누락 키: {', '.join(missing)}")
            return None

        # private_key 개행 및 형식 정리
        private_key = gcp_dict.get('private_key', '')
        if isinstance(private_key, str):
            private_key = private_key.replace('\\n', '\n').replace('\\r\\n', '\n').replace('\\r', '\n')
        if not str(private_key).startswith('-----BEGIN PRIVATE KEY-----'):
            st.error("gcp_service_account.private_key 형식이 올바르지 않습니다. PEM 헤더를 포함해야 합니다.")
            return None
        gcp_dict['private_key'] = private_key

        try:
            credentials = Credentials.from_service_account_info(gcp_dict, scopes=scope)
        except Exception as cred_err:
            st.error(f"서비스 계정 자격 증명 생성 실패: {cred_err}")
            return None

        try:
            client = gspread.authorize(credentials)
            return client
        except Exception as auth_err:
            st.error(f"gspread 인증 실패: {auth_err}")
            return None
    except Exception as e:
        st.error(f"Google Sheets 클라이언트 초기화 중 알 수 없는 오류: {e}")
        return None

def safe_dataframe_display(df, use_container_width=True, hide_index=True):
    """데이터프레임을 안전하게 표시하는 함수"""
    try:
        # 모든 컬럼을 문자열로 변환하여 Arrow 호환성 문제 방지
        df_display = df.copy()
        for col in df_display.columns:
            try:
                df_display[col] = df_display[col].astype(str)
            except:
                # 변환 실패 시 원본 값 유지
                pass
        
        st.dataframe(df_display, use_container_width=use_container_width, hide_index=hide_index)
    except Exception as e:
        st.error(f"데이터프레임 표시 중 오류 발생: {e}")
        # 오류 발생 시 원본 데이터프레임을 문자열로 변환하여 표시
        st.write("데이터 표시에 문제가 있어 원본 형태로 표시합니다:")
        st.write(df)

def clean_dataframe_for_display(df):
    """데이터프레임을 표시용으로 정리하는 함수"""
    try:
        df_clean = df.copy()
        
        # IP 컬럼이 있으면 문자열로 변환
        if 'IP' in df_clean.columns:
            df_clean['IP'] = df_clean['IP'].astype(str)
        
        # 숫자 컬럼은 소수점 3자리까지 표시
        for col in df_clean.columns:
            if col not in ['팀명', '순위']:  # 팀명과 순위는 그대로 유지
                try:
                    df_clean[col] = df_clean[col].astype(float).round(3)
                except:
                    pass

        # 모든 숫자 컬럼을 문자열로 변환하여 Arrow 호환성 문제 방지
        for col in df_clean.columns:
            if col not in ['팀명', '순위']:  # 팀명과 순위는 그대로 유지
                try:
                    df_clean[col] = df_clean[col].astype(str)
                except:
                    pass
        
        return df_clean
    except Exception as e:
        st.error(f"데이터프레임 정리 중 오류 발생: {e}")
        return df


def append_simulation_to_sheet(df_result, sheet_name="SimulationLog"):
    try:
        client = get_gsheet_client()
        if client is None:
            diag = _diagnose_gsheet_setup()
            st.error("구글 시트 클라이언트를 초기화할 수 없습니다.\n원인 진단:\n" + diag)
            return
            
        # 스프레드시트 열기 (secrets에서 ID/URL 우선)
        sh = None
        try:
            cfg = st.secrets.get("gsheet", {})
        except Exception:
            cfg = {}

        spreadsheet_id = None
        if isinstance(cfg, dict):
            spreadsheet_id = cfg.get("spreadsheet_id")
            spreadsheet_url = cfg.get("spreadsheet_url")
            if not spreadsheet_id and spreadsheet_url:
                try:
                    import re as _re
                    m = _re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", str(spreadsheet_url))
                    if m:
                        spreadsheet_id = m.group(1)
                except Exception:
                    pass

        if spreadsheet_id:
            try:
                sh = client.open_by_key(spreadsheet_id)
            except Exception as e:
                st.error("스프레드시트(ID) 열기 실패:\n" + _format_gspread_error(e))
                return
        else:
            # ID 미지정 시 제목으로 열기 시도, 실패하면 생성
            try:
                sh = client.open("KBO_Simulation_Log")
            except Exception as open_err:
                try:
                    sh = client.create("KBO_Simulation_Log")
                except Exception as create_err:
                    err_text = str(create_err)
                    if "quota" in err_text.lower() and "storage" in err_text.lower():
                        st.error(
                            "Google Drive 저장 용량 초과로 새 스프레드시트를 만들 수 없습니다.\n"
                            "해결 방법:\n"
                            "- 드라이브 용량 확보(휴지통 비우기 포함) 후 다시 시도\n"
                            "- 또는 기존 스프레드시트의 ID를 secrets.gsheet.spreadsheet_id에 설정하고, 서비스 계정을 편집자로 공유"
                        )
                    else:
                        st.error("스프레드시트 생성 실패:\n" + _format_gspread_error(create_err))
                    return

        # 워크시트 열기 (없으면 생성) 및 헤더 추가
        created_new_worksheet = False
        try:
            worksheet = sh.worksheet(sheet_name)
        except Exception as ws_open_err:
            try:
                worksheet = sh.add_worksheet(title=sheet_name, rows="1000", cols="20")
                created_new_worksheet = True
            except Exception as ws_create_err:
                st.error("워크시트 생성 실패:\n" + _format_gspread_error(ws_create_err))
                return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_result = df_result.copy()
        df_result.insert(0, "timestamp", timestamp)

        # 새 워크시트인 경우 헤더 추가
        if created_new_worksheet:
            try:
                worksheet.append_row(df_result.columns.tolist(), value_input_option="USER_ENTERED")
            except Exception as header_err:
                st.warning("헤더 추가 실패(계속 진행):\n" + _format_gspread_error(header_err))

        try:
            worksheet.append_rows(df_result.values.tolist(), value_input_option="USER_ENTERED")
        except Exception as append_err:
            st.error("데이터 추가 실패:\n" + _format_gspread_error(append_err))
            return
        st.success(f"시뮬레이션 결과가 '{sheet_name}' 시트에 저장되었습니다.")
    except Exception as e:
        st.error("Google Sheets 저장 중 알 수 없는 오류:\n" + _format_gspread_error(e))

# 페이지 설정
st.set_page_config(
    page_title="KBO 팀 통계 분석기",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS 스타일링
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .team-stats {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=3600)  # 1시간마다 캐시 갱신
def scrape_kbo_team_batting_stats():
    """KBO 팀별 타자 기록 스크래핑"""
    url = "https://www.koreabaseball.com/Record/Team/Hitter/Basic1.aspx"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', class_='tEx') or soup.find('table')
        
        if table is None:
            st.error("타자 기록 테이블을 찾을 수 없습니다.")
            return None
            
        rows = table.find_all('tr')
        data = []
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            row_text = [cell.get_text().strip() for cell in cells]
            
            for j, text in enumerate(row_text):
                if text in ['롯데', '삼성', 'LG', '한화', 'KIA', '두산', 'NC', 'KT', 'SSG', '키움']:
                    if len(row_text) >= 15:
                        team_data = []
                        team_data.append(text)
                        
                        for k in range(j+1, min(j+14, len(row_text))):
                            try:
                                val = row_text[k]
                                if '.' in val and val.replace('.', '').replace('-', '').isdigit():
                                    team_data.append(float(val))
                                elif val.replace('-', '').isdigit():
                                    team_data.append(int(val))
                                else:
                                    team_data.append(val)
                            except:
                                team_data.append(row_text[k])
                        
                        if len(team_data) == 14:
                            data.append(team_data)
                    break
        
        if not data:
            st.error("팀 데이터를 찾을 수 없습니다.")
            return None
        
        columns = ['팀명', 'AVG', 'G', 'PA', 'AB', 'R', 'H', '2B', '3B', 'HR', 'TB', 'RBI', 'SAC', 'SF']
        df = pd.DataFrame(data, columns=columns)
        df = df.sort_values('AVG', ascending=False).reset_index(drop=True)
        df.insert(0, '순위', range(1, len(df) + 1))
        
        return df
        
    except Exception as e:
        st.error(f"스크래핑 중 오류 발생: {e}")
        return None

@st.cache_data(ttl=3600)
def scrape_kbo_team_batting_stats_advanced():
    """KBO 팀별 타자 고급 기록 스크래핑 (출루율, 장타율, OPS 등)"""
    url = "https://www.koreabaseball.com/Record/Team/Hitter/Basic2.aspx"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', class_='tEx') or soup.find('table')
        
        if table is None:
            st.error("고급 타자 기록 테이블을 찾을 수 없습니다.")
            return None
            
        rows = table.find_all('tr')
        data = []
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            row_text = [cell.get_text().strip() for cell in cells]
            
            # 팀명이 포함된 행인지 확인
            team_found = False
            for text in row_text:
                if text in ['롯데', '삼성', 'LG', '한화', 'KIA', '두산', 'NC', 'KT', 'SSG', '키움']:
                    team_found = True
                    break
            
            if team_found and len(row_text) >= 12:  # 팀명 + 최소 11개 지표
                team_data = []
                # 팀명 찾기
                for text in row_text:
                    if text in ['롯데', '삼성', 'LG', '한화', 'KIA', '두산', 'NC', 'KT', 'SSG', '키움']:
                        team_data.append(text)
                        break
                
                # 나머지 데이터 추출 (AVG, BB, IBB, HBP, SO, GDP, SLG, OBP, OPS, MH, RISP)
                data_started = False
                for val in row_text:
                    if data_started and len(team_data) < 12:  # 팀명 + 11개 지표
                        try:
                            if '.' in val and val.replace('.', '').replace('-', '').isdigit():
                                team_data.append(float(val))
                            elif val.replace('-', '').isdigit():
                                team_data.append(int(val))
                            else:
                                team_data.append(val)
                        except:
                            team_data.append(val)
                    elif val in ['롯데', '삼성', 'LG', '한화', 'KIA', '두산', 'NC', 'KT', 'SSG', '키움']:
                        data_started = True
                
                if len(team_data) == 12:  # 팀명 + 11개 지표
                    data.append(team_data)
        
        if not data:
            st.error("팀 데이터를 찾을 수 없습니다.")
            return None
        
        columns = ['팀명', 'AVG', 'BB', 'IBB', 'HBP', 'SO', 'GDP', 'SLG', 'OBP', 'OPS', 'MH', 'RISP']
        df = pd.DataFrame(data, columns=columns)
        df = df.sort_values('AVG', ascending=False).reset_index(drop=True)
        df.insert(0, '순위', range(1, len(df) + 1))
        
        return df
        
    except Exception as e:
        st.error(f"스크래핑 중 오류 발생: {e}")
        return None

@st.cache_data(ttl=3600)
def scrape_kbo_team_pitching_stats():
    """KBO 팀별 투수 기록 스크래핑"""
    url = "https://www.koreabaseball.com/Record/Team/Pitcher/Basic1.aspx"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', class_='tEx') or soup.find('table')
        
        if table is None:
            st.error("투수 기록 테이블을 찾을 수 없습니다.")
            return None
            
        rows = table.find_all('tr')
        data = []
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            row_text = [cell.get_text().strip() for cell in cells]
            
            for j, text in enumerate(row_text):
                if text in ['롯데', '삼성', 'LG', '한화', 'KIA', '두산', 'NC', 'KT', 'SSG', '키움']:
                    if len(row_text) >= 18:
                        team_data = []
                        team_data.append(text)
                        
                        for k in range(j+1, min(j+17, len(row_text))):
                            try:
                                val = row_text[k]
                                if k == j+9:  # IP 컬럼
                                    try:
                                        if '/' in val:
                                            parts = val.split()
                                            if len(parts) == 2:
                                                whole = float(parts[0])
                                                frac_parts = parts[1].split('/')
                                                fraction = float(frac_parts[0]) / float(frac_parts[1])
                                                team_data.append(str(whole + fraction))  # 문자열로 변환
                                            else:
                                                team_data.append(str(float(val)) if val.replace('.', '').replace('-', '').isdigit() else val)
                                        else:
                                            team_data.append(str(float(val)) if val.replace('.', '').replace('-', '').isdigit() else val)
                                    except:
                                        team_data.append(str(val))  # 문자열로 변환
                                elif '.' in val and val.replace('.', '').replace('-', '').isdigit():
                                    team_data.append(float(val))
                                elif val.replace('-', '').isdigit():
                                    team_data.append(int(val))
                                else:
                                    team_data.append(val)
                            except:
                                team_data.append(row_text[k])
                        
                        if len(team_data) == 17:
                            data.append(team_data)
                    break
        
        if not data:
            st.error("팀 데이터를 찾을 수 없습니다.")
            return None
        
        columns = ['팀명', 'ERA', 'G', 'W', 'L', 'SV', 'HLD', 'WPCT', 'IP', 'H', 'HR', 'BB', 'HBP', 'SO', 'R', 'ER', 'WHIP']
        df = pd.DataFrame(data, columns=columns)
        
        # IP 컬럼을 문자열로 확실히 변환
        try:
            df['IP'] = df['IP'].astype(str)
        except:
            # 변환 실패 시 모든 값을 문자열로 변환
            df['IP'] = df['IP'].apply(lambda x: str(x) if x is not None else '')
        
        df = df.sort_values('ERA', ascending=True).reset_index(drop=True)
        df.insert(0, '순위', range(1, len(df) + 1))
        
        return df
        
    except Exception as e:
        st.error(f"스크래핑 중 오류 발생: {e}")
        return None

@st.cache_data(ttl=3600)
def scrape_kbo_team_pitching_stats_advanced():
    """KBO 팀별 투수 고급 기록 스크래핑 (완투, 완봉, 퀄리티스타트 등)"""
    url = "https://www.koreabaseball.com/Record/Team/Pitcher/Basic2.aspx"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', class_='tEx') or soup.find('table')
        
        if table is None:
            st.error("투수 고급 기록 테이블을 찾을 수 없습니다.")
            return None
            
        rows = table.find_all('tr')
        data = []
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            row_text = [cell.get_text().strip() for cell in cells]
            
            # 팀명이 포함된 행인지 확인
            team_found = False
            for text in row_text:
                if text in ['롯데', '삼성', 'LG', '한화', 'KIA', '두산', 'NC', 'KT', 'SSG', '키움']:
                    team_found = True
                    break
            
            if team_found and len(row_text) >= 16:  # 팀명 + 최소 15개 지표
                team_data = []
                # 팀명 찾기
                for text in row_text:
                    if text in ['롯데', '삼성', 'LG', '한화', 'KIA', '두산', 'NC', 'KT', 'SSG', '키움']:
                        team_data.append(text)
                        break
                
                # 나머지 데이터 추출 (ERA, CG, SHO, QS, BSV, TBF, NP, AVG, 2B, 3B, SAC, SF, IBB, WP, BK)
                data_started = False
                for val in row_text:
                    if data_started and len(team_data) < 16:  # 팀명 + 15개 지표
                        try:
                            if '.' in val and val.replace('.', '').replace('-', '').isdigit():
                                team_data.append(float(val))
                            elif val.replace('-', '').isdigit():
                                team_data.append(int(val))
                            else:
                                team_data.append(val)
                        except:
                            team_data.append(val)
                    elif val in ['롯데', '삼성', 'LG', '한화', 'KIA', '두산', 'NC', 'KT', 'SSG', '키움']:
                        data_started = True
                
                if len(team_data) == 16:  # 팀명 + 15개 지표
                    data.append(team_data)
        
        if not data:
            st.error("팀 데이터를 찾을 수 없습니다.")
            return None
        
        columns = ['팀명', 'ERA', 'CG', 'SHO', 'QS', 'BSV', 'TBF', 'NP', 'AVG', '2B', '3B', 'SAC', 'SF', 'IBB', 'WP', 'BK']
        df = pd.DataFrame(data, columns=columns)
        df = df.sort_values('ERA', ascending=True).reset_index(drop=True)
        df.insert(0, '순위', range(1, len(df) + 1))
        
        return df
        
    except Exception as e:
        st.error(f"스크래핑 중 오류 발생: {e}")
        return None

@st.cache_data(ttl=3600)
def scrape_kbo_standings():
    """KBO 팀 순위 스크래핑"""
    url = "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 기준 날짜 추출
        date_info = None
        
        # 페이지에서 날짜 정보 찾기
        all_texts = soup.get_text()
        lines = all_texts.split('\n')
        for line in lines:
            line = line.strip()
            if '기준' in line and ('년' in line and '월' in line):
                date_info = line
                break
        
        # 대안: 특정 패턴으로 날짜 찾기
        if not date_info:
            import re
            date_pattern = r'\([0-9]{4}년\s*[0-9]{2}월[0-9]{2}일\s*기준\)'
            matches = re.findall(date_pattern, all_texts)
            if matches:
                date_info = matches[0]
        
        table = soup.find('table', class_='tEx') or soup.find('table')
        
        if table is None:
            st.error("순위 테이블을 찾을 수 없습니다.")
            return None, date_info
            
        rows = table.find_all('tr')
        data = []
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            row_text = [cell.get_text().strip() for cell in cells]
            
            for j, text in enumerate(row_text):
                if text in ['LG', '한화', '롯데', '삼성', 'SSG', 'NC', 'KIA', '두산', 'KT', '키움']:
                    if len(row_text) >= 10:
                        team_data = []
                        team_data.append(text)
                        
                        for k in range(j+1, min(j+8, len(row_text))):
                            try:
                                val = row_text[k]
                                if '.' in val and val.replace('.', '').replace('-', '').isdigit():
                                    team_data.append(float(val))
                                elif val.replace('-', '').isdigit():
                                    team_data.append(int(val))
                                else:
                                    team_data.append(val)
                            except:
                                team_data.append(row_text[k])
                        
                        if len(team_data) == 8:
                            data.append(team_data)
                    break
        
        if not data:
            st.error("팀 데이터를 찾을 수 없습니다.")
            return None, date_info
        
        columns = ['팀명', '경기', '승', '패', '무', '승률', '게임차', '최근10경기']
        df = pd.DataFrame(data, columns=columns)
        df = df.sort_values('승률', ascending=False).reset_index(drop=True)
        df.insert(0, '순위', range(1, len(df) + 1))
        
        return df, date_info
        
    except Exception as e:
        st.error(f"스크래핑 중 오류 발생: {e}")
        return None, None

def monte_carlo_simulation(win_probability, remaining_games, num_simulations=10000):
    """몬테카를로 시뮬레이션을 통한 기대 승수 계산"""
    total_wins = 0
    
    for _ in range(num_simulations):
        wins = 0
        for _ in range(remaining_games):
            if random.random() < win_probability:
                wins += 1
        total_wins += wins
    
    return total_wins / num_simulations

def calculate_championship_probability(teams_data, num_simulations=100000):
    """각 팀의 우승 확률을 계산하는 함수"""
    championship_wins = {team: 0 for team in teams_data['팀명']}
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for simulation in range(num_simulations):
        if simulation % 10000 == 0:
            progress = simulation / num_simulations
            progress_bar.progress(progress)
            status_text.text(f"우승 확률 계산 중... {simulation:,}/{num_simulations:,} ({progress:.1%})")
        
        final_wins = {}
        
        for _, team in teams_data.iterrows():
            team_name = team['팀명']
            current_wins = team['승']
            pythagorean_wpct = team['p_wpct']
            remaining_games = team['잔여경기']
            
            simulated_wins = 0
            for _ in range(remaining_games):
                if random.random() < pythagorean_wpct:
                    simulated_wins += 1
            
            final_wins[team_name] = current_wins + simulated_wins
        
        champion = max(final_wins, key=final_wins.get)
        championship_wins[champion] += 1
    
    progress_bar.progress(1.0)
    status_text.text("우승 확률 계산 완료!")
    
    championship_probabilities = {}
    for team, wins in championship_wins.items():
        championship_probabilities[team] = (wins / num_simulations) * 100
    
    return championship_probabilities

def calculate_playoff_probability(teams_data, num_simulations=50000):
    """플레이오프 진출 확률 계산 (상위 5팀)"""
    playoff_wins = {team: 0 for team in teams_data['팀명']}
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for simulation in range(num_simulations):
        if simulation % 10000 == 0:
            progress = simulation / num_simulations
            progress_bar.progress(progress)
            status_text.text(f"플레이오프 확률 계산 중... {simulation:,}/{num_simulations:,} ({progress:.1%})")
        
        final_wins = {}
        
        for _, team in teams_data.iterrows():
            team_name = team['팀명']
            current_wins = team['승']
            pythagorean_wpct = team['p_wpct']
            remaining_games = team['잔여경기']
            
            simulated_wins = 0
            for _ in range(remaining_games):
                if random.random() < pythagorean_wpct:
                    simulated_wins += 1
            
            final_wins[team_name] = current_wins + simulated_wins
        
        sorted_teams = sorted(final_wins.items(), key=lambda x: x[1], reverse=True)
        playoff_teams = [team[0] for team in sorted_teams[:5]]
        
        for team in playoff_teams:
            playoff_wins[team] += 1
    
    progress_bar.progress(1.0)
    status_text.text("플레이오프 확률 계산 완료!")
    
    playoff_probabilities = {}
    for team, wins in playoff_wins.items():
        playoff_probabilities[team] = (wins / num_simulations) * 100
    
    return playoff_probabilities

def main():
    # 헤더
    st.markdown('<h1 class="main-header">⚾ KBO 팀 통계 분석기</h1>', unsafe_allow_html=True)
    
    # 데이터 로딩
    with st.spinner("실시간 KBO 데이터를 가져오는 중..."):
        df_hitter = scrape_kbo_team_batting_stats()
        df_hitter_advanced = scrape_kbo_team_batting_stats_advanced()
        df_pitcher = scrape_kbo_team_pitching_stats()
        df_pitcher_advanced = scrape_kbo_team_pitching_stats_advanced()
        df_standings, date_info = scrape_kbo_standings()
    
    if df_hitter is None or df_hitter_advanced is None or df_pitcher is None or df_pitcher_advanced is None or df_standings is None:
        st.error("데이터를 가져올 수 없습니다. 잠시 후 다시 시도해주세요.")
        return
    
    # 기준 날짜 표시 (데이터 로딩 후)
    if date_info:
        st.markdown(f'<p style="text-align: center; font-size: 1rem; color: #666; margin-top: -1rem; margin-bottom: 2rem;">{date_info}</p>', unsafe_allow_html=True)
    
    # 타자 기록 결합 (팀명 기준)
    df_hitter_combined = pd.merge(df_hitter, df_hitter_advanced[['팀명', 'BB', 'IBB', 'HBP', 'SO', 'GDP', 'SLG', 'OBP', 'OPS', 'MH', 'RISP']], 
                                 on='팀명', how='left')
    
    # 투수 기록 결합 (팀명 기준)
    df_pitcher_combined = pd.merge(df_pitcher, df_pitcher_advanced[['팀명', 'CG', 'SHO', 'QS', 'BSV', 'TBF', 'NP', 'AVG', '2B', '3B', 'SAC', 'SF', 'IBB', 'WP', 'BK']], 
                                  on='팀명', how='left')
    
    # 탭 생성
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 현재 순위", "🏟️ 팀별 기록", "📊 시각화", "🏆 우승 확률", "📅 시뮬레이션 이력"])
    
    with tab1:
        st.header("📈 현재 순위")
        # 현재 순위 표시
        # st.subheader("현재 순위")
        # st.dataframe(df_standings, use_container_width=True, hide_index=True)
        
        # with col2:
        #     st.subheader("승률 분포")
        #     # 승률 기준으로 정렬된 데이터프레임 생성
        #     df_sorted = df_standings.sort_values('승률', ascending=False)
        #     # st.write(df_sorted)
        #     fig = px.bar(df_sorted, y='팀명', x='승률', 
        #                 # title="팀별 승률",
        #                 color='승률',
        #                 color_continuous_scale='RdYlGn',
        #                 orientation='h')
        #     fig.update_layout(xaxis_tickangle=0, showlegend=False)
        #     fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        #     fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        #     st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.header("🏟️ 팀별 기록")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("타자 기록")
            df_hitter_clean = clean_dataframe_for_display(df_hitter_combined)
            safe_dataframe_display(df_hitter_clean, use_container_width=True, hide_index=True)
        
        with col2:
            st.subheader("투수 기록")
            df_pitcher_clean = clean_dataframe_for_display(df_pitcher_combined)
            safe_dataframe_display(df_pitcher_clean, use_container_width=True, hide_index=True)
        
        # Top 3 팀들을 2열로 배치
        st.subheader("🏆 TOP 3 팀")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("타격 상위 3팀")
            # 타율 상위 3팀
            top3_avg = df_hitter_combined.nlargest(3, 'AVG')[['팀명', 'AVG']]
            st.write("**타율 상위 3팀**")
            col1_1, col1_2, col1_3 = st.columns(3)
            for i, (idx, row) in enumerate(top3_avg.iterrows()):
                if i == 0:
                    with col1_1:
                        st.metric(f"1위 {row['팀명']}", f"{row['AVG']:.3f}")
                elif i == 1:
                    with col1_2:
                        st.metric(f"2위 {row['팀명']}", f"{row['AVG']:.3f}")
                else:
                    with col1_3:
                        st.metric(f"3위 {row['팀명']}", f"{row['AVG']:.3f}")
            
            # OPS 상위 3팀
            top3_ops = df_hitter_combined.nlargest(3, 'OPS')[['팀명', 'OPS']]
            st.write("**OPS 상위 3팀**")
            col1_4, col1_5, col1_6 = st.columns(3)
            for i, (idx, row) in enumerate(top3_ops.iterrows()):
                if i == 0:
                    with col1_4:
                        st.metric(f"1위 {row['팀명']}", f"{row['OPS']:.3f}")
                elif i == 1:
                    with col1_5:
                        st.metric(f"2위 {row['팀명']}", f"{row['OPS']:.3f}")
                else:
                    with col1_6:
                        st.metric(f"3위 {row['팀명']}", f"{row['OPS']:.3f}")
        
        with col2:
            st.subheader("투수 상위 3팀")
            # ERA 상위 3팀
            top3_era = df_pitcher_combined.nsmallest(3, 'ERA')[['팀명', 'ERA']]
            st.write("**ERA 상위 3팀 (낮은 순)**")
            col2_1, col2_2, col2_3 = st.columns(3)
            for i, (idx, row) in enumerate(top3_era.iterrows()):
                if i == 0:
                    with col2_1:
                        st.metric(f"1위 {row['팀명']}", f"{row['ERA']:.2f}")
                elif i == 1:
                    with col2_2:
                        st.metric(f"2위 {row['팀명']}", f"{row['ERA']:.2f}")
                else:
                    with col2_3:
                        st.metric(f"3위 {row['팀명']}", f"{row['ERA']:.2f}")
            
            # WHIP 상위 3팀
            top3_whip = df_pitcher_combined.nsmallest(3, 'WHIP')[['팀명', 'WHIP']]
            st.write("**WHIP 상위 3팀 (낮은 순)**")
            col2_4, col2_5, col2_6 = st.columns(3)
            for i, (idx, row) in enumerate(top3_whip.iterrows()):
                if i == 0:
                    with col2_4:
                        st.metric(f"1위 {row['팀명']}", f"{row['WHIP']:.2f}")
                elif i == 1:
                    with col2_5:
                        st.metric(f"2위 {row['팀명']}", f"{row['WHIP']:.2f}")
                else:
                    with col2_6:
                        st.metric(f"3위 {row['팀명']}", f"{row['WHIP']:.2f}")
    
    with tab1:
        # st.header("📈 현재 순위")
        
        # 백그라운드에서 피타고리안 승률 계산 및 기본 시뮬레이션 실행
        df_runs = pd.merge(df_hitter[['팀명', 'R']], df_pitcher[['팀명', 'R']], on='팀명', how='left')
        df_runs.rename(columns={'R_x': 'R', 'R_y': 'RA'}, inplace=True)
        
        p_n = 1.834
        df_runs['p_wpct'] = df_runs['R']**p_n / (df_runs['R']**p_n + df_runs['RA']**p_n)
        df_runs['p_wpct'] = df_runs['p_wpct'].round(4)
        
        df_final = pd.merge(df_standings, df_runs, on='팀명', how='left')
        df_final['잔여경기'] = 144 - df_final['경기']
        
        # 기본 시뮬레이션 실행 (백그라운드)
        np.random.seed(42)
        random.seed(42)
        
        simulation_results = []
        for _, row in df_final.iterrows():
            team_name = row['팀명']
            current_wins = row['승']
            win_rate = row['승률']
            pythagorean_wpct = row['p_wpct']
            remaining_games = row['잔여경기']
            
            expected_wins_winrate = monte_carlo_simulation(win_rate, remaining_games, 10000)
            expected_wins_pythagorean = monte_carlo_simulation(pythagorean_wpct, remaining_games, 10000)
            
            final_expected_wins_winrate = current_wins + expected_wins_winrate
            final_expected_wins_pythagorean = current_wins + expected_wins_pythagorean
            
            simulation_results.append({
                '팀명': team_name,
                '현재승수': current_wins,
                '잔여경기': remaining_games,
                '승률': win_rate,
                'p_wpct': pythagorean_wpct,
                '기대승수_승률기반': expected_wins_winrate,
                '기대승수_피타고리안기반': expected_wins_pythagorean,
                '최종기대승수_승률기반': final_expected_wins_winrate,
                '최종기대승수_피타고리안기반': final_expected_wins_pythagorean
            })
        
        results_df = pd.DataFrame(simulation_results)
        df_final = df_final.merge(results_df[['팀명', '기대승수_승률기반', '기대승수_피타고리안기반', 
                                            '최종기대승수_승률기반', '최종기대승수_피타고리안기반']], 
                                on='팀명', how='left')
        
        # df_final을 전역 변수로 저장
        st.session_state['df_final'] = df_final
        
        # 현재 순위와 예측 분석 표시
        st.subheader("📊 현재 순위 및 예측 분석")
        
        # 피타고리안 승률과 최종 기대승수가 포함된 데이터프레임 생성
        df_display = df_final[['순위', '팀명', '경기', '승', '패', '무', '승률', '게임차', '최근10경기',
                              'p_wpct', '최종기대승수_피타고리안기반']].copy()
        df_display['p_wpct'] = df_display['p_wpct'].round(4)
        df_display['최종기대승수_피타고리안기반'] = df_display['최종기대승수_피타고리안기반'].round(1)
        df_display.rename(columns={'p_wpct': '피타고리안승률', '최종기대승수_피타고리안기반': '예상최종승수'}, inplace=True)
        
        df_display_clean = clean_dataframe_for_display(df_display)
        safe_dataframe_display(df_display_clean, use_container_width=True, hide_index=True)
    
    with tab3:
        st.header("📊 시각화")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 타율 vs 홈런
            fig1 = px.scatter(df_hitter_combined, x='AVG', y='HR', 
                             title="타율 vs 홈런",
                             hover_data=['팀명'],
                             text='팀명')
            fig1.update_traces(textposition="top center", marker_size=12)
            fig1.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            fig1.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # ERA vs 삼진
            fig2 = px.scatter(df_pitcher_combined, x='ERA', y='SO', 
                             title="ERA vs 삼진",
                             hover_data=['팀명'],
                             text='팀명')
            fig2.update_traces(textposition="top center", marker_size=12)
            fig2.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            fig2.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            st.plotly_chart(fig2, use_container_width=True)
        
        # 승률 vs 피타고리안 승률 비교
        df_final = st.session_state['df_final']
        fig3 = px.scatter(df_final, x='승률', y='p_wpct', 
                         title="실제 승률 vs 피타고리안 승률",
                         hover_data=['팀명'],
                         text='팀명')
        fig3.add_trace(go.Scatter(x=[0.250, 0.650], y=[0.250, 0.650], mode='lines', 
                                 name='기준선', line=dict(dash='dash', color='red')))
        fig3.update_traces(textposition="top center", marker_size=12)
        fig3.update_xaxes(range=[0.250, 0.650], showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig3.update_yaxes(range=[0.250, 0.650], showgrid=True, gridwidth=1, gridcolor='lightgray')
        st.plotly_chart(fig3, use_container_width=True)
    
    with tab4:
        # st.header("🏆 우승 확률")
        
        df_final = st.session_state['df_final']
        # st.subheader("우승 확률 계산")
        
        # 시뮬레이션 횟수 설정
        col1, col2 = st.columns(2)
        with col1:
            championship_simulations = st.slider("우승 확률 시뮬레이션 횟수", 5000, 50000, 5000, step=5000)
        with col2:
            playoff_simulations = st.slider("플레이오프 확률 시뮬레이션 횟수", 5000, 50000, 5000, step=5000)
        
        if st.button("시뮬레이션 시작"):
            with st.spinner("우승 확률과 플레이오프 확률을 계산하는 중..."):
                # 우승 확률 계산
                championship_probs = calculate_championship_probability(df_final, championship_simulations)
                df_final['우승확률_퍼센트'] = df_final['팀명'].map(championship_probs)
                
                # 플레이오프 확률 계산
                playoff_probs = calculate_playoff_probability(df_final, playoff_simulations)
                df_final['플레이오프진출확률_퍼센트'] = df_final['팀명'].map(playoff_probs)

                # Google Sheets에 저장 시도
                log_df = df_final[['팀명', '우승확률_퍼센트', '플레이오프진출확률_퍼센트']].copy()
                append_simulation_to_sheet(log_df, "SimulationLog")
                
                # 최종기대승수_피타고리안기반 컬럼이 없으면 승수로 대체
                display_col = '최종기대승수_피타고리안기반' if '최종기대승수_피타고리안기반' in df_final.columns else '승'
                
                # 통합 결과 테이블 생성
                combined_df = df_final[['순위', '팀명', display_col, '우승확률_퍼센트', '플레이오프진출확률_퍼센트']].copy()
                combined_df = combined_df.sort_values('우승확률_퍼센트', ascending=False).reset_index(drop=True)
                combined_df.rename(columns={display_col: '예상최종승수'}, inplace=True)
                
                st.subheader("🏆 KBO 우승 확률 & PO 진출 확률")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    combined_df_clean = clean_dataframe_for_display(combined_df)
                    combined_df_clean.rename(columns={'우승확률_퍼센트': '우승확률'}, inplace=True)
                    combined_df_clean.rename(columns={'플레이오프진출확률_퍼센트': 'PO확률'}, inplace=True)
                    safe_dataframe_display(combined_df_clean, use_container_width=True, hide_index=True)
                
                with col2:
                    # 우승 확률 시각화
                    fig = px.bar(combined_df, x='팀명', y='우승확률_퍼센트',
                                title="팀별 우승 확률",
                                color='우승확률_퍼센트',
                                color_continuous_scale='RdYlGn')
                    fig.update_layout(xaxis_tickangle=-45)
                    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                    st.plotly_chart(fig, use_container_width=True)
                
                # 플레이오프 확률 시각화
                fig2 = px.bar(combined_df, x='팀명', y='플레이오프진출확률_퍼센트',
                             title="팀별 플레이오프 진출 확률",
                             color='플레이오프진출확률_퍼센트',
                             color_continuous_scale='Blues')
                fig2.update_layout(xaxis_tickangle=-45)
                fig2.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                fig2.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                st.plotly_chart(fig2, use_container_width=True)

    with tab5:
        st.header("📅 시뮬레이션 이력")
    # with st.expander("📅 시뮬레이션 이력 분석"):
        try:
            client = get_gsheet_client()
            if client is None:
                st.info("Google Sheets 연결이 설정되지 않았습니다. 시뮬레이션 이력을 불러올 수 없습니다.")
                st.warning("진단 정보:\n" + _diagnose_gsheet_setup())
            else:
                worksheet = client.open("KBO_Simulation_Log").worksheet("SimulationLog")
                history = worksheet.get_all_records()
                df_history = pd.DataFrame(history)

                if not df_history.empty:
                    df_history['timestamp'] = pd.to_datetime(df_history['timestamp'])

                    df_summary = df_history.groupby(['timestamp']).agg({
                        '우승확률_퍼센트': 'mean',
                        '플레이오프진출확률_퍼센트': 'mean'
                    }).reset_index()

                    fig = px.line(df_summary, x='timestamp', y=['우승확률_퍼센트', '플레이오프진출확률_퍼센트'],
                                  title='일자별 평균 우승 / 플레이오프 확률', markers=True)
                    fig.update_layout(xaxis_title="날짜", yaxis_title="확률(%)")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("아직 시뮬레이션 이력이 없습니다. 우승 확률 탭에서 시뮬레이션을 실행해보세요.")
        except Exception as e:
            st.info("Google Sheets 연결에 문제가 있습니다. 시뮬레이션 이력을 불러올 수 없습니다.")
            # Google Sheets 연결 실패 시에도 앱이 계속 작동하도록 함
            pass


if __name__ == "__main__":
    main() 

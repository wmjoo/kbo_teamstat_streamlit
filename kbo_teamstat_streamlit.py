# kbo_teamstat_streamlit.py
from io import StringIO
import streamlit as st
import pandas as pd
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
from datetime import datetime
import gspread
from gspread.exceptions import APIError as GspreadAPIError
from google.oauth2.service_account import Credentials
import plotly.express as px
import plotly.graph_objects as go
from zoneinfo import ZoneInfo

# -----------------------------
# 페이지/스타일
# -----------------------------
st.set_page_config(
    page_title="KBO 팀 통계 분석기",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.markdown("""
<style>
    .main-header { font-size: 2.5rem; color: #1f77b4; text-align: center; margin-bottom: 2rem; }
    .metric-card { background-color: #f0f2f6; padding: 1rem; border-radius: .5rem; border-left: 4px solid #1f77b4; }
    .team-stats { background-color: #fff; padding: 1rem; border-radius: .5rem; box-shadow: 0 2px 4px rgba(0,0,0,.1); }
</style>
""", unsafe_allow_html=True)

# -----------------------------
# 상수/유틸
# -----------------------------
TEAM_NAMES = ['롯데', '삼성', 'LG', '한화', 'KIA', '두산', 'NC', 'KT', 'SSG', '키움']
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/126.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.6,en;q=0.4',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Referer': 'https://www.koreabaseball.com/Record/Team/Default.aspx',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# KBO 원본 URL 상수
KBO_URLS = {
    'hitter_basic1': 'https://www.koreabaseball.com/Record/Team/Hitter/Basic1.aspx',
    'hitter_basic2': 'https://www.koreabaseball.com/Record/Team/Hitter/Basic2.aspx',
    'pitcher_basic1': 'https://www.koreabaseball.com/Record/Team/Pitcher/Basic1.aspx',
    'pitcher_basic2': 'https://www.koreabaseball.com/Record/Team/Pitcher/Basic2.aspx',
    'standings': 'https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx',
}

def _parse_kbo_date_info_to_date(date_info: str) -> str | None:
    """'(YYYY년 M월 D일 기준)' 문자열에서 YYYY-MM-DD 형식의 기준일자를 추출."""
    try:
        if not date_info:
            return None
        m = re.search(r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일", str(date_info))
        if not m:
            return None
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{y:04d}-{mo:02d}-{d:02d}"
    except Exception:
        return None

# 팀별 고정 색상(요청 우선 적용). 지정되지 않은 팀은 기본 팔레트 사용
TEAM_COLOR_MAP = {
    'LG': '#B31942',      # 밝기 + 채도 보정
    '한화': '#FF8C00',     # Dark Orange
    '키움': '#A45A6B',     # 와인색
    '두산': '#003366',     # Deep Blue
    '삼성': '#1E90FF',     # 유지
    'SSG': '#FFD700',     # 유지
    'KT': '#4B4B4B',      # 짙은 그레이
    '롯데': '#FF4C4C',     # 밝은 레드
    'KIA': '#8B0000',      # Dark Red
    'NC': '#B8860B',       # 브론즈
}


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=('GET', 'HEAD'),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.headers.update(HEADERS)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

SESSION = _build_session()

def _diagnose_gsheet_setup() -> str:
    msgs = []
    try:
        if "gcp_service_account" not in st.secrets:
            msgs.append("- secrets에 [gcp_service_account] 섹션이 없음 (.streamlit/secrets.toml 확인)")
            return "\n".join(msgs)

        gcp = dict(st.secrets["gcp_service_account"])
        required = ["type","project_id","private_key_id","private_key","client_email","client_id","token_uri"]
        missing = [k for k in required if k not in gcp or not gcp[k]]
        if missing:
            msgs.append(f"- 누락된 키: {', '.join(missing)}")

        pk = str(gcp.get("private_key", ""))
        if not pk.startswith("-----BEGIN PRIVATE KEY-----"):
            msgs.append("- private_key 형식 오류: PEM 헤더가 없음")
        if "\\n" not in gcp.get("private_key", "") and "\n" not in pk:
            msgs.append("- private_key 줄바꿈 누락 가능성: TOML엔 \\n로 저장 필요")
        email = str(gcp.get("client_email",""))
        if not email.endswith("iam.gserviceaccount.com"):
            msgs.append("- client_email 값이 서비스 계정 이메일 형식이 아님")
        if not msgs:
            msgs.append("- secrets 형식은 정상. Sheets/Drive API 활성화 및 대상 시트 공유 권한 확인")
    except Exception as e:
        msgs.append(f"- 진단 중 예외 발생: {e}")
    return "\n".join(msgs)

def _format_gspread_error(err: Exception) -> str:
    try:
        if isinstance(err, GspreadAPIError):
            status_code, reason, message = None, None, None
            resp = getattr(err, "response", None)
            if resp is not None:
                status_code = getattr(resp, "status_code", None)
                try:
                    data = resp.json()
                    err_obj = data.get("error", {}) if isinstance(data, dict) else {}
                    message = err_obj.get("message")
                    details = err_obj.get("errors") or []
                    if isinstance(details, list) and details:
                        reason = details[0].get("reason")
                except Exception:
                    message = getattr(resp, "text", None)
            parts = []
            if status_code is not None: parts.append(f"status={status_code}")
            if reason: parts.append(f"reason={reason}")
            if message: parts.append(f"message={message}")
            return "; ".join(parts) if parts else str(err)
        return str(err)
    except Exception:
        return str(err)

def get_gsheet_client():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        if "gcp_service_account" not in st.secrets:
            st.error("Streamlit secrets에 'gcp_service_account' 없음")
            return None

        gcp = dict(st.secrets["gcp_service_account"])
        required = ["type","project_id","private_key_id","private_key","client_email","client_id","token_uri"]
        missing = [k for k in required if k not in gcp or not gcp[k]]
        if missing:
            st.error(f"gcp_service_account 누락 키: {', '.join(missing)}")
            return None

        pk = gcp.get("private_key","")
        if isinstance(pk, str):
            pk = pk.replace("\\r\\n","\n").replace("\\n","\n").replace("\\r","\n")
        if not str(pk).startswith("-----BEGIN PRIVATE KEY-----"):
            st.error("gcp_service_account.private_key 형식 오류(PEM 헤더 누락)")
            return None
        gcp["private_key"] = pk

        try:
            creds = Credentials.from_service_account_info(gcp, scopes=scope)
        except Exception as e:
            st.error(f"서비스 계정 자격 증명 생성 실패: {e}")
            return None

        try:
            return gspread.authorize(creds)
        except Exception as e:
            st.error(f"gspread 인증 실패: {e}")
            return None
    except Exception as e:
        st.error(f"Google Sheets 초기화 오류: {e}")
        return None

def _extract_sheet_id_from_url(url: str) -> str | None:
    try:
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", str(url))
        return m.group(1) if m else None
    except Exception:
        return None

def append_simulation_to_sheet(df_result: pd.DataFrame, sheet_name="SimulationLog", base_date: str | None = None):
    try:
        client = get_gsheet_client()
        if client is None:
            st.error("구글 시트 클라이언트를 초기화할 수 없습니다.\n원인 진단:\n" + _diagnose_gsheet_setup())
            return

        cfg = {}
        try:
            cfg = st.secrets.get("gsheet", {}) or {}
        except Exception:
            pass

        spreadsheet_id = cfg.get("spreadsheet_id")
        spreadsheet_url = cfg.get("spreadsheet_url")
        if not spreadsheet_id and spreadsheet_url:
            spreadsheet_id = _extract_sheet_id_from_url(spreadsheet_url)

        if spreadsheet_id:
            try:
                sh = client.open_by_key(spreadsheet_id)
            except Exception as e:
                st.error("스프레드시트(ID) 열기 실패:\n" + _format_gspread_error(e))
                return
        else:
            try:
                sh = client.open("KBO_Simulation_Log")
            except Exception:
                try:
                    sh = client.create("KBO_Simulation_Log")
                except Exception as e:
                    txt = str(e)
                    if "quota" in txt.lower() and "storage" in txt.lower():
                        st.error(
                            "Google Drive 저장 용량 초과로 새 스프레드시트를 만들 수 없습니다.\n"
                            "- 드라이브 용량 확보(휴지통 비우기 포함) 후 재시도\n"
                            "- 또는 기존 시트 ID를 secrets.gsheet.spreadsheet_id에 설정 + 서비스 계정에 편집자 공유"
                        )
                    else:
                        st.error("스프레드시트 생성 실패:\n" + _format_gspread_error(e))
                    return

        created_new_ws = False
        try:
            ws = sh.worksheet(sheet_name)
        except Exception:
            try:
                ws = sh.add_worksheet(title=sheet_name, rows="10000", cols="50")
                created_new_ws = True
            except Exception as e:
                st.error("워크시트 생성 실패:\n" + _format_gspread_error(e))
                return

        now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
        formatted_time = now_kst.strftime("%Y-%m-%d %H:%M:%S")
        df_out = df_result.copy()
        # 기준일(base_date)은 KBO 페이지 기준일자(예: 2025-08-10). 없으면 오늘 날짜 사용
        base_date_str = base_date if base_date else now_kst.strftime("%Y-%m-%d")
        df_out.insert(0, "base_date", base_date_str)
        df_out.insert(1, "timestamp", formatted_time)

        if created_new_ws:
            try:
                ws.append_row(df_out.columns.tolist(), value_input_option="USER_ENTERED")
            except Exception as e:
                st.warning("헤더 추가 실패(계속 진행):\n" + _format_gspread_error(e))

        try:
            ws.append_rows(df_out.values.tolist(), value_input_option="USER_ENTERED")
        except Exception as e:
            st.error("데이터 추가 실패:\n" + _format_gspread_error(e))
            return

        # st.success(f"시뮬레이션 결과가 '{sheet_name}' 시트에 저장되었습니다.")
    except Exception as e:
        st.error("Google Sheets 저장 중 알 수 없는 오류:\n" + _format_gspread_error(e))

def _open_log_worksheet(sheet_name: str = "SimulationLog"):
    """append 시 사용한 동일한 규칙으로 로그 워크시트를 연다.
    우선순위: secrets.gsheet.spreadsheet_id → secrets.gsheet.spreadsheet_url → 이름("KBO_Simulation_Log").
    생성은 하지 않고, 없으면 None 반환.
    """
    try:
        client = get_gsheet_client()
        if client is None:
            st.info("Google Sheets 연결이 설정되지 않았습니다. 이력을 불러올 수 없습니다.")
            st.warning("진단 정보:\n" + _diagnose_gsheet_setup())
            return None
        cfg = {}
        try:
            cfg = st.secrets.get("gsheet", {}) or {}
        except Exception:
            pass
        spreadsheet_id = cfg.get("spreadsheet_id")
        spreadsheet_url = cfg.get("spreadsheet_url")
        if not spreadsheet_id and spreadsheet_url:
            spreadsheet_id = _extract_sheet_id_from_url(spreadsheet_url)
        if spreadsheet_id:
            try:
                sh = client.open_by_key(spreadsheet_id)
            except Exception as e:
                st.error("스프레드시트(ID) 열기 실패:\n" + _format_gspread_error(e))
                return None
        else:
            try:
                sh = client.open("KBO_Simulation_Log")
            except Exception:
                # 생성은 하지 않음(읽기 탭)
                st.info("로그 스프레드시트를 찾을 수 없습니다. 먼저 우승 확률 탭에서 시뮬을 실행해 저장하세요.")
                return None
        try:
            return sh.worksheet(sheet_name)
        except Exception:
            st.info(f"'{sheet_name}' 워크시트를 찾을 수 없습니다. 시뮬 실행 후 다시 시도하세요.")
            return None
    except Exception as e:
        st.error("로그 스프레드시트 접근 중 오류:\n" + _format_gspread_error(e))
        return None

def safe_dataframe_display(df: pd.DataFrame, use_container_width=True, hide_index=True):
    try:
        df_display = df.copy()
        for c in df_display.columns:
            try:
                df_display[c] = df_display[c].astype(str)
            except Exception:
                pass
        st.dataframe(df_display, use_container_width=use_container_width, hide_index=hide_index)
    except Exception as e:
        st.error(f"데이터프레임 표시 오류: {e}")
        st.write("원본 형태로 표시합니다:")
        st.write(df)

def normalize_team_names(df: pd.DataFrame, col: str = "팀명") -> pd.DataFrame:
    """팀명 컬럼 공백/비가시문자 제거 및 표준화."""
    if df is not None and not df.empty and col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"\s+", "", regex=True)
            .str.strip()
        )
    return df

def clean_dataframe_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """표시용 정리: IP 문자열 유지, 수치 컬럼 반올림 후 문자열화(Arrow 호환)."""
    try:
        dfc = df.copy()
        if "IP" in dfc.columns:
            dfc["IP"] = dfc["IP"].astype(str)

        for c in dfc.columns:
            if c in ("팀명", "순위", "최근10경기"):
                continue
            if pd.api.types.is_float_dtype(dfc[c]):
                dfc[c] = dfc[c].round(3).astype(str)
            elif pd.api.types.is_integer_dtype(dfc[c]):
                dfc[c] = dfc[c].astype(str)

        return dfc
    except Exception as e:
        st.error(f"표시 정리 오류: {e}")
        return df

def _parse_ip_to_decimal(ip_str: str) -> float | None:
    """'123 2/3' 또는 '123.1' 같은 이닝 문자열을 소수로 변환(투수 IP는 보통 1/3 단위)."""
    if ip_str is None:
        return None
    s = str(ip_str).strip()
    if not s:
        return None
    m = re.match(r"^(\d+)\s+(\d)\/3$", s)
    if m:
        whole = float(m.group(1)); frac = int(m.group(2))/3.0
        return round(whole + frac, 4)
    m2 = re.match(r"^(\d+)\.(\d)$", s)
    if m2:
        whole = float(m2.group(1)); tenths = int(m2.group(2))
        if tenths in (1,2):
            frac = tenths/3.0
            return round(whole + frac, 4)
        return float(s)
    try:
        return float(s)
    except Exception:
        return None

def _first_table_html(url: str) -> tuple[pd.DataFrame | None, BeautifulSoup | None]:
    """해당 페이지에서 첫 테이블을 DataFrame으로 읽고, soup도 반환."""
    try:
        r = SESSION.get(url, timeout=15)
        r.raise_for_status()
        try:
            if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
                r.encoding = r.apparent_encoding or 'utf-8'
        except Exception:
            r.encoding = 'utf-8'
        soup = BeautifulSoup(r.text, "html.parser")
        # read_html: 여러 테이블 중 적절한 테이블 선택
        try:
            tables = pd.read_html(StringIO(r.text))
            if tables:
                # '팀' 문자열이 포함된 헤더를 우선 선택
                for t in tables:
                    cols_join = "".join(map(str, list(t.columns)))
                    if '팀' in cols_join or '팀명' in cols_join:
                        return t, soup
                return tables[0], soup
        except Exception:
            pass
        table = soup.find("table")
        if not table:
            try:
                table = soup.select_one("table, table.tData, table.table, .record > table")
            except Exception:
                table = None
        if not table:
            return None, soup
        rows = []
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            rows.append([c.get_text(strip=True) for c in cells])
        if not rows:
            return None, soup
        df = pd.DataFrame(rows[1:], columns=rows[0])
        return df, soup
    except Exception:
        return None, None

def _candidate_team_tokens() -> list[str]:
    return [
        'LG','한화','롯데','삼성','SSG','NC','KIA','두산','KT','키움',
        '기아','랜더스','트윈스','이글스','자이언츠','라이온즈','베어스','다이노스','위즈','히어로즈','타이거즈'
    ]

def _score_table_for_teams(df: pd.DataFrame) -> tuple[int, int]:
    """테이블 내 팀 토큰 발견 개수(최대열, 총합)을 점수로 반환."""
    try:
        tokens = _candidate_team_tokens()
        max_col_hits = 0
        total_hits = 0
        for col in df.columns:
            s = df[col].astype(str)
            hits = 0
            for tok in tokens:
                try:
                    hits += int(s.str.contains(tok, na=False).sum())
                except Exception:
                    continue
            total_hits += hits
            if hits > max_col_hits:
                max_col_hits = hits
        return max_col_hits, total_hits
    except Exception:
        return (0, 0)

def _choose_best_table_from_html(html_text: str, soup: BeautifulSoup) -> pd.DataFrame | None:
    # 1) pandas로 파싱 가능한 모든 테이블 점수화
    best: tuple[int,int,int,pd.DataFrame] | None = None
    try:
        tables = pd.read_html(StringIO(html_text))
        for idx, t in enumerate(tables):
            max_col_hits, total_hits = _score_table_for_teams(t)
            if best is None or (max_col_hits, total_hits) > (best[0], best[1]):
                best = (max_col_hits, total_hits, idx, t)
        if best and (best[0] >= 5 or best[1] >= 10):
            return best[3]
    except Exception:
        pass
    # 2) BeautifulSoup 기반 파싱 시도
    try:
        candidates = soup.find_all('table')
        best_bs: tuple[int,int,int,pd.DataFrame] | None = None
        for i, table in enumerate(candidates):
            rows = []
            for tr in table.find_all('tr'):
                cells = tr.find_all(['th','td'])
                if not cells:
                    continue
                rows.append([c.get_text(strip=True) for c in cells])
            if len(rows) < 3:
                continue
            df = pd.DataFrame(rows[1:], columns=rows[0])
            max_col_hits, total_hits = _score_table_for_teams(df)
            if best_bs is None or (max_col_hits, total_hits) > (best_bs[0], best_bs[1]):
                best_bs = (max_col_hits, total_hits, i, df)
        if best_bs and (best_bs[0] >= 5 or best_bs[1] >= 10):
            return best_bs[3]
    except Exception:
        pass
    return None

def _find_team_col_index(df: pd.DataFrame) -> int | None:
    tokens = _candidate_team_tokens()
    best_idx = None
    best_hits = -1
    for i, col in enumerate(df.columns):
        try:
            s = df[col].astype(str)
            hits = 0
            for tok in tokens:
                try:
                    hits += int(s.str.contains(tok, na=False).sum())
                except Exception:
                    continue
            if hits > best_hits:
                best_hits = hits
                best_idx = i
        except Exception:
            continue
    return best_idx if best_hits >= 5 else None

def _ensure_team_first_column(df: pd.DataFrame) -> pd.DataFrame:
    """팀명이 포함된 열을 찾아 0번째로 이동하고 헤더를 정리."""
    if df is None or df.empty:
        return df
    # 헤더 행이 내부에 중복될 수 있어 제거
    try:
        header_set = set(map(str, df.columns))
        mask_header_like = df.apply(lambda r: set(map(str, r.values)) == header_set, axis=1)
        if mask_header_like.any():
            df = df.loc[~mask_header_like].reset_index(drop=True)
    except Exception:
        pass
    idx = _find_team_col_index(df)
    if idx is None:
        return df
    if idx != 0:
        cols = list(df.columns)
        new_cols = [cols[idx]] + cols[:idx] + cols[idx+1:]
        df = df[new_cols].copy()
    return df

def _drop_rank_like_columns(df: pd.DataFrame, team_col_index: int = 0) -> pd.DataFrame:
    """순위 열 제거(팀명 열 제외).
    조건:
    - 헤더에 '순위' 또는 '순'이 명시된 경우
    - 또는 해당 열의 유효 숫자값이 정확히 1..N 형태(중복 없이 전체 길이와 동일)
    """
    try:
        cols = list(df.columns)
        drop_indices: list[int] = []
        for i, col in enumerate(cols):
            if i == team_col_index:
                continue
            cname = str(col)
            if '순위' in cname or cname.strip() in ('순', '순번', '랭킹'):
                drop_indices.append(i)
                continue
            try:
                s = pd.to_numeric(df.iloc[:, i], errors='coerce')
                non_na = s.dropna()
                # 정확히 1..N 형태인지 판단
                if len(non_na) > 0 and (non_na % 1 == 0).all():
                    uniq = sorted(non_na.astype(int).unique().tolist())
                    expected = list(range(1, len(df) + 1))
                    if uniq == expected:
                        drop_indices.append(i)
            except Exception:
                continue
        if drop_indices:
            keep = [j for j in range(len(cols)) if j not in set(drop_indices)]
            df = df.iloc[:, keep].copy()
        return df
    except Exception:
        return df

def _normalize_standings_df(df: pd.DataFrame) -> pd.DataFrame:
    """순위 테이블의 컬럼을 표준 순서로 재배치하고 '순위'처럼 불필요한 랭크열은 제거.
    타겟 순서: 팀명, 경기, 승, 패, 무, 승률, 게임차, 최근10경기
    """
    if df is None or df.empty:
        return df
    # 팀명 열을 앞으로 보장
    df = _ensure_team_first_column(df)
    # 순위 유사 열 제거
    df = _drop_rank_like_columns(df, team_col_index=0)
    # 컬럼명 정규화
    def norm(s: str) -> str:
        return re.sub(r"\s+", "", str(s))
    colmap = {i: norm(c) for i, c in enumerate(df.columns)}
    want = {
        '팀명': ['팀명', '팀', '구단'],
        '경기': ['경기', 'G', '게임수'],
        '승': ['승', 'W'],
        '패': ['패', 'L'],
        '무': ['무', 'D', 'T', '무승부'],
        '승률': ['승률', 'WPCT'],
        '게임차': ['게임차', 'GB'],
        '최근10경기': ['최근10경기', '최근10'],
    }
    found: dict[str, int | None] = {k: None for k in want}
    for target, keys in want.items():
        for idx, cname in colmap.items():
            if any(k in cname for k in keys):
                found[target] = idx
                break
    # 필수: 팀명, 경기, 승, 패, 승률
    essential = ['팀명', '경기', '승', '패', '승률']
    if any(found[k] is None for k in essential):
        return df
    order = [found['팀명'], found['경기'], found['승'], found['패'], found['무'], found['승률'], found['게임차'], found['최근10경기']]
    order = [i for i in order if i is not None]
    df2 = df.iloc[:, order].copy()
    # 컬럼명 부여
    cols_final = ['팀명', '경기', '승', '패']
    if found['무'] is not None:
        cols_final.append('무')
    cols_final += ['승률']
    if found['게임차'] is not None:
        cols_final.append('게임차')
    if found['최근10경기'] is not None:
        cols_final.append('최근10경기')
    df2.columns = cols_final
    return df2

def _standardize_kbo_team_name(raw_name: str) -> str | None:
    """페이지마다 다른 팀명 표기를 표준 팀명으로 통일.
    예: 'SSG랜더스' → 'SSG', '키움히어로즈' → '키움', '기아' → 'KIA' 등
    """
    if raw_name is None:
        return None
    name = str(raw_name)
    name = re.sub(r"\s+", "", name)
    upper = name.upper()
    # 명확한 토큰 우선
    if 'LG' in upper:
        return 'LG'
    if 'DOOSAN' in upper or '두산' in name:
        return '두산'
    if 'SAMSUNG' in upper or '삼성' in name:
        return '삼성'
    if 'LOTTE' in upper or '롯데' in name:
        return '롯데'
    if 'HANHWA' in upper or '한화' in name:
        return '한화'
    if 'NC' in upper:
        return 'NC'
    if 'KT' in upper:
        return 'KT'
    if 'SSG' in upper or '랜더스' in name:
        return 'SSG'
    if 'KIWOOM' in upper or '키움' in name:
        return '키움'
    if 'KIA' in upper or '기아' in name:
        return 'KIA'
    return None

def _fuzzy_map_team_name(raw_name: str) -> str | None:
    """느슨한 기준으로 팀명을 표준 팀명으로 매핑.
    - TEAM_NAMES 또는 대표 토큰이 포함되어 있으면 매핑
    - 예: '삼성 라이온즈' → '삼성', 'KT 위즈' → 'KT'
    """
    if raw_name is None:
        return None
    s = str(raw_name)
    s_compact = re.sub(r"\s+", "", s)
    # 직접 표준화 먼저
    std = _standardize_kbo_team_name(s_compact)
    if std:
        return std
    # 포함 관계로 매핑
    synonyms = {
        '롯데': ['롯데', '자이언츠', 'LOTTE'],
        '삼성': ['삼성', '라이온즈', 'SAMSUNG'],
        'LG': ['LG', '트윈스'],
        '한화': ['한화', '이글스', 'HANHWA'],
        'KIA': ['KIA', '기아', '타이거즈'],
        '두산': ['두산', '베어스', 'DOOSAN'],
        'NC': ['NC', '다이노스'],
        'KT': ['KT', '위즈'],
        'SSG': ['SSG', '랜더스'],
        '키움': ['키움', '히어로즈', 'KIWOOM'],
    }
    upper = s_compact.upper()
    for std_name, keys in synonyms.items():
        for key in keys:
            if key.upper() in upper:
                return std_name
    return None

# -----------------------------
# 스크래핑 함수
# -----------------------------
@st.cache_data(ttl=3600)
def scrape_kbo_team_batting_stats():
    url = "https://www.koreabaseball.com/Record/Team/Hitter/Basic1.aspx"
    raw, soup = _first_table_html(url)
    df = _ensure_team_first_column(raw) if raw is not None else None
    if df is None or df.empty:
        # 최후의 보루: soup에서 베스트 테이블 재선택
        _, soup2 = _first_table_html(url)
        if soup2 is not None:
            best = _choose_best_table_from_html(StringIO(soup2.text).getvalue() if hasattr(soup2, 'text') else '', soup2)
            if best is not None and not best.empty:
                df = _ensure_team_first_column(best)
        if df is None or df.empty:
            st.error("타자 기본 기록 테이블을 찾을 수 없습니다.")
            return None
    # 순위 유사 열 제거로 컬럼 시프트 방지
    df = _drop_rank_like_columns(df, team_col_index=0)
    # 팀명 표준화 후 필터링
    try:
        df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: _standardize_kbo_team_name(x) or _fuzzy_map_team_name(x))
    except Exception:
        pass
    df = df[df.iloc[:, 0].isin(TEAM_NAMES)].copy()
    if df.empty:
        st.error("타자 기본 기록 테이블 파싱 실패(팀명 필터 결과 0행). 잠시 후 다시 시도해주세요.")
        return None
    cols = ['팀명','AVG','G','PA','AB','R','H','2B','3B','HR','TB','RBI','SAC','SF']
    take = min(len(df.columns), len(cols))
    df = df.iloc[:, :take].copy()
    df.columns = cols[:take]
    for c in df.columns:
        if c != '팀명':
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.sort_values('AVG', ascending=False).reset_index(drop=True)
    df.insert(0, '순위', pd.Series(range(1, len(df)+1), dtype='Int64'))
    return df

@st.cache_data(ttl=3600)
def scrape_kbo_team_batting_stats_advanced():
    url = "https://www.koreabaseball.com/Record/Team/Hitter/Basic2.aspx"
    raw, soup = _first_table_html(url)
    df = _ensure_team_first_column(raw) if raw is not None else None
    if df is None or df.empty:
        _, soup2 = _first_table_html(url)
        if soup2 is not None:
            best = _choose_best_table_from_html(StringIO(soup2.text).getvalue() if hasattr(soup2, 'text') else '', soup2)
            if best is not None and not best.empty:
                df = _ensure_team_first_column(best)
        if df is None or df.empty:
            st.error("타자 고급 기록 테이블을 찾을 수 없습니다.")
            return None
    df = _drop_rank_like_columns(df, team_col_index=0)
    try:
        df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: _standardize_kbo_team_name(x) or _fuzzy_map_team_name(x))
    except Exception:
        pass
    df = df[df.iloc[:, 0].isin(TEAM_NAMES)].copy()
    if df.empty:
        st.error("타자 고급 기록 테이블 파싱 실패(팀명 필터 결과 0행). 잠시 후 다시 시도해주세요.")
        return None
    cols = ['팀명','AVG','BB','IBB','HBP','SO','GDP','SLG','OBP','OPS','MH','RISP']
    take = min(len(df.columns), len(cols))
    df = df.iloc[:, :take].copy()
    df.columns = cols[:take]
    for c in df.columns:
        if c != '팀명':
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.sort_values('AVG', ascending=False).reset_index(drop=True)
    df.insert(0, '순위', pd.Series(range(1, len(df)+1), dtype='Int64'))
    return df

@st.cache_data(ttl=3600)
def scrape_kbo_team_pitching_stats():
    url = "https://www.koreabaseball.com/Record/Team/Pitcher/Basic1.aspx"
    raw, soup = _first_table_html(url)
    df = _ensure_team_first_column(raw) if raw is not None else None
    if df is None or df.empty:
        _, soup2 = _first_table_html(url)
        if soup2 is not None:
            best = _choose_best_table_from_html(StringIO(soup2.text).getvalue() if hasattr(soup2, 'text') else '', soup2)
            if best is not None and not best.empty:
                df = _ensure_team_first_column(best)
        if df is None or df.empty:
            st.error("투수 기본 기록 테이블을 찾을 수 없습니다.")
            return None
    df = _drop_rank_like_columns(df, team_col_index=0)
    try:
        df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: _standardize_kbo_team_name(x) or _fuzzy_map_team_name(x))
    except Exception:
        pass
    df = df[df.iloc[:, 0].isin(TEAM_NAMES)].copy()
    if df.empty:
        st.error("투수 기본 기록 테이블 파싱 실패(팀명 필터 결과 0행). 잠시 후 다시 시도해주세요.")
        return None
    cols = ['팀명','ERA','G','W','L','SV','HLD','WPCT','IP','H','HR','BB','HBP','SO','R','ER','WHIP']
    take = min(len(df.columns), len(cols))
    df = df.iloc[:, :take].copy()
    df.columns = cols[:take]
    if 'IP' in df.columns:
        df['IP_decimal'] = df['IP'].apply(_parse_ip_to_decimal)
        df['IP'] = df['IP'].astype(str)
    for c in df.columns:
        if c not in ['팀명','IP']:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.sort_values('ERA', ascending=True).reset_index(drop=True)
    df.insert(0, '순위', pd.Series(range(1, len(df)+1), dtype='Int64'))
    return df

@st.cache_data(ttl=3600)
def scrape_kbo_team_pitching_stats_advanced():
    url = "https://www.koreabaseball.com/Record/Team/Pitcher/Basic2.aspx"
    raw, soup = _first_table_html(url)
    df = _ensure_team_first_column(raw) if raw is not None else None
    if df is None or df.empty:
        _, soup2 = _first_table_html(url)
        if soup2 is not None:
            best = _choose_best_table_from_html(StringIO(soup2.text).getvalue() if hasattr(soup2, 'text') else '', soup2)
            if best is not None and not best.empty:
                df = _ensure_team_first_column(best)
        if df is None or df.empty:
            st.error("투수 고급 기록 테이블을 찾을 수 없습니다.")
            return None
    df = _drop_rank_like_columns(df, team_col_index=0)
    try:
        df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: _standardize_kbo_team_name(x) or _fuzzy_map_team_name(x))
    except Exception:
        pass
    df = df[df.iloc[:, 0].isin(TEAM_NAMES)].copy()
    if df.empty:
        st.error("투수 고급 기록 테이블 파싱 실패(팀명 필터 결과 0행). 잠시 후 다시 시도해주세요.")
        return None
    cols = ['팀명','ERA','CG','SHO','QS','BSV','TBF','NP','AVG','2B','3B','SAC','SF','IBB','WP','BK']
    take = min(len(df.columns), len(cols))
    df = df.iloc[:, :take].copy()
    df.columns = cols[:take]
    for c in df.columns:
        if c != '팀명':
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.sort_values('ERA', ascending=True).reset_index(drop=True)
    df.insert(0, '순위', pd.Series(range(1, len(df)+1), dtype='Int64'))
    return df

@st.cache_data(ttl=3600)
def scrape_kbo_standings():
    url = "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx"
    raw, soup = _first_table_html(url)
    df = _ensure_team_first_column(raw) if raw is not None else None
    date_info = None
    if soup:
        all_texts = soup.get_text("\n")
        m = re.search(r"\(\d{4}년\s*\d{1,2}월\s*\d{1,2}일\s*기준\)", all_texts)
        if m:
            date_info = m.group(0)
    if df is None or df.empty:
        _, soup2 = _first_table_html(url)
        if soup2 is not None:
            best = _choose_best_table_from_html(StringIO(soup2.text).getvalue() if hasattr(soup2, 'text') else '', soup2)
            if best is not None and not best.empty:
                df = _ensure_team_first_column(best)
        if df is None or df.empty:
            st.error("순위 테이블을 찾을 수 없습니다.")
            return None, date_info
    # 컬럼 구조 정규화 및 팀명 표준화
    df = _normalize_standings_df(df)
    try:
        df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: _standardize_kbo_team_name(x) or _fuzzy_map_team_name(x))
    except Exception:
        pass
    df = df[df.iloc[:, 0].isin(['LG','한화','롯데','삼성','SSG','NC','KIA','두산','KT','키움'])].copy()
    for c in ['경기','승','패','무','승률']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    # 무승부 보정: 원본에 없거나 결측이면 경기-승-패로 계산
    try:
        if '무' not in df.columns:
            df['무'] = (df['경기'] - df['승'] - df['패']).clip(lower=0)
        else:
            null_mask = df['무'].isna()
            if null_mask.any():
                df.loc[null_mask, '무'] = (df.loc[null_mask, '경기'] - df.loc[null_mask, '승'] - df.loc[null_mask, '패']).clip(lower=0)
    except Exception:
        pass
    # 컬럼 재정렬(무 포함 보장)
    try:
        desired = [col for col in ['팀명','경기','승','패','무','승률','게임차','최근10경기'] if col in df.columns]
        df = df[desired + [c for c in df.columns if c not in desired]]
    except Exception:
        pass
    df = df.sort_values('승률', ascending=False).reset_index(drop=True)
    df.insert(0, '순위', pd.Series(range(1, len(df)+1), dtype='Int64'))
    return df, date_info

# -----------------------------
# 시뮬레이션(고속/안전)
# -----------------------------
def monte_carlo_expected_wins(p: float, n_games: int, n_sims: int = 10_000) -> float:
    if n_games <= 0:
        return 0.0
    if p <= 0.0:
        return 0.0
    if p >= 1.0:
        return float(n_games)
    wins = np.random.binomial(n=n_games, p=float(p), size=n_sims)
    return float(wins.mean())

def calculate_championship_probability(teams_df: pd.DataFrame, num_simulations: int = 100_000) -> dict:
    """
    피타고리안 승률 기반 우승 확률 계산(빈 DF/팀 수 0/잔여경기 0 전부 방어).
    """
    if teams_df is None or teams_df.empty:
        st.warning("시뮬레이션 대상 팀 데이터가 없습니다.")
        return {}

    required = {"팀명", "승", "p_wpct", "잔여경기"}
    missing = [c for c in required if c not in teams_df.columns]
    if missing:
        st.error(f"시뮬레이션 필수 컬럼 누락: {', '.join(missing)}")
        return {}

    df = teams_df.copy()
    # 타입 정리
    df["잔여경기"] = pd.to_numeric(df["잔여경기"], errors="coerce").fillna(0).astype(int).clip(lower=0)
    df["승"] = pd.to_numeric(df["승"], errors="coerce").fillna(0).astype(int)
    df["p_wpct"] = pd.to_numeric(df["p_wpct"], errors="coerce").fillna(0.0).astype(float)

    # 팀명 정규화
    df = normalize_team_names(df)

    # 유효 팀만(수치형 NaN 제거)
    df = df.loc[df["p_wpct"].notna() & df["승"].notna() & df["잔여경기"].notna()].reset_index(drop=True)
    T = len(df)
    if T == 0:
        st.warning("유효한 팀 데이터가 없어 시뮬레이션을 생략합니다.")
        return {}

    names = df["팀명"].tolist()
    current_wins = df["승"].to_numpy(dtype=int)
    p = df["p_wpct"].to_numpy(dtype=float)
    n_remain = df["잔여경기"].to_numpy(dtype=int)

    # 잔여경기 전체 0인 경우: 현재 승수 기준
    if np.all(n_remain == 0):
        winners = {n: 0.0 for n in names}
        try:
            if len(current_wins) > 0:
                winners[names[int(np.argmax(current_wins))]] = 100.0
        except ValueError:
            # 빈 시퀀스 안전망
            pass
        st.info("모든 팀의 잔여 경기가 0입니다. 현재 승수 기준으로 우승 확률을 산출했습니다.")
        return winners

    wins_count = {n: 0 for n in names}
    prog = st.progress(0.0)
    text = st.empty()

    # 배치 처리로 메모리/속도 균형
    batch = 10_000
    n_batches = int(np.ceil(num_simulations / batch))

    for b in range(n_batches):
        this_batch = batch if (b + 1) * batch <= num_simulations else (num_simulations - b * batch)
        if this_batch <= 0:
            continue

        # (B, T) 배열을 "열(팀)별"로 생성 → n,p가 벡터여도 안전
        sim = np.empty((this_batch, T), dtype=int)
        for t in range(T):
            nt = int(n_remain[t]); pt = float(p[t])
            if nt <= 0 or pt <= 0.0:
                sim[:, t] = 0
            elif pt >= 1.0:
                sim[:, t] = nt
            else:
                sim[:, t] = np.random.binomial(n=nt, p=pt, size=this_batch)

        final_wins = sim + current_wins  # (B, T)
        if final_wins.size == 0 or final_wins.shape[1] == 0:
            # 안전망: T==0 또는 B==0
            continue

        try:
            idx = np.argmax(final_wins, axis=1)  # (B,)
        except ValueError:
            # 빈 시퀀스 안전망
            continue
        for i in idx:
            wins_count[names[int(i)]] += 1

        if b % 2 == 0:
            prog.progress((b + 1) / n_batches)
            text.text(f"우승 확률 계산 중... {min((b + 1) * batch, num_simulations):,}/{num_simulations:,}")

    prog.progress(1.0)
    text.text("우승 확률 계산 완료!")
    return {k: v / num_simulations * 100.0 for k, v in wins_count.items()}

def calculate_playoff_probability(teams_df: pd.DataFrame, num_simulations: int = 50_000) -> dict:
    """
    상위 5팀 플레이오프 진출 확률(빈 DF/팀 수 < 5/잔여경기 0 방어).
    """
    if teams_df is None or teams_df.empty:
        st.warning("시뮬레이션 대상 팀 데이터가 없습니다.")
        return {}

    required = {"팀명", "승", "p_wpct", "잔여경기"}
    missing = [c for c in required if c not in teams_df.columns]
    if missing:
        st.error(f"시뮬레이션 필수 컬럼 누락: {', '.join(missing)}")
        return {}

    df = teams_df.copy()
    df["잔여경기"] = pd.to_numeric(df["잔여경기"], errors="coerce").fillna(0).astype(int).clip(lower=0)
    df["승"] = pd.to_numeric(df["승"], errors="coerce").fillna(0).astype(int)
    df["p_wpct"] = pd.to_numeric(df["p_wpct"], errors="coerce").fillna(0.0).astype(float)

    df = normalize_team_names(df)
    df = df.loc[df["p_wpct"].notna() & df["승"].notna() & df["잔여경기"].notna()].reset_index(drop=True)

    T = len(df)
    if T == 0:
        st.warning("유효한 팀 데이터가 없어 시뮬레이션을 생략합니다.")
        return {}

    names = df["팀명"].tolist()
    current_wins = df["승"].to_numpy(dtype=int)
    p = df["p_wpct"].to_numpy(dtype=float)
    n_remain = df["잔여경기"].to_numpy(dtype=int)

    top_k = min(5, T)
    po_counts = {n: 0 for n in names}
    prog = st.progress(0.0)
    text = st.empty()

    batch = 10_000
    n_batches = int(np.ceil(num_simulations / batch))

    for b in range(n_batches):
        this_batch = batch if (b + 1) * batch <= num_simulations else (num_simulations - b * batch)
        if this_batch <= 0:
            continue

        sim = np.empty((this_batch, T), dtype=int)
        for t in range(T):
            nt = int(n_remain[t]); pt = float(p[t])
            if nt <= 0 or pt <= 0.0:
                sim[:, t] = 0
            elif pt >= 1.0:
                sim[:, t] = nt
            else:
                sim[:, t] = np.random.binomial(n=nt, p=pt, size=this_batch)

        final_wins = sim + current_wins
        if final_wins.size == 0 or final_wins.shape[1] == 0:
            continue

        # 빠른 상위 선택
        try:
            topk_idx = np.argpartition(-final_wins, kth=top_k - 1, axis=1)[:, :top_k]
        except ValueError:
            # 빈 배열 안전망
            continue
        rows = np.arange(final_wins.shape[0])[:, None]
        ordered = topk_idx[rows, np.argsort(-final_wins[rows, topk_idx], axis=1)]

        for row in ordered:
            for i in row:
                po_counts[names[int(i)]] += 1

        if b % 2 == 0:
            prog.progress((b + 1) / n_batches)
            text.text(f"플레이오프 확률 계산 중... {min((b + 1) * batch, num_simulations):,}/{num_simulations:,}")

    prog.progress(1.0)
    text.text("플레이오프 확률 계산 완료!")
    return {k: v / num_simulations * 100.0 for k, v in po_counts.items()}

def _validate_sim_inputs(df_final: pd.DataFrame) -> bool:
    """시뮬 시작 전 입력 검증. 문제가 있으면 사용자에게 원인 표시하고 False."""
    need = {"팀명", "승", "p_wpct", "잔여경기"}
    if df_final is None or df_final.empty:
        st.error("시뮬레이션 입력이 비어 있습니다(df_final).")
        return False
    miss = [c for c in need if c not in df_final.columns]
    if miss:
        st.error(f"시뮬레이션 필수 컬럼 누락: {', '.join(miss)}")
        return False
    if df_final["팀명"].isna().all():
        st.error("팀명 컬럼이 비어 있어 시뮬레이션을 수행할 수 없습니다.")
        return False
    return True

# -----------------------------
# 메인
# -----------------------------
def main():
    st.markdown('<h1 class="main-header">⚾ KBO 팀 통계 분석기</h1>', unsafe_allow_html=True)

    # 데이터 로딩
    with st.spinner("실시간 KBO 데이터를 가져오는 중..."):
        df_hitter = scrape_kbo_team_batting_stats()
        df_hitter_adv = scrape_kbo_team_batting_stats_advanced()
        df_pitcher = scrape_kbo_team_pitching_stats()
        df_pitcher_adv = scrape_kbo_team_pitching_stats_advanced()
        df_standings, date_info = scrape_kbo_standings()

    if any(x is None for x in [df_hitter, df_hitter_adv, df_pitcher, df_pitcher_adv, df_standings]):
        st.error("데이터를 가져올 수 없습니다. 잠시 후 다시 시도해주세요.")
        return

    # 팀명 정규화(병합 전)
    df_hitter = normalize_team_names(df_hitter)
    df_hitter_adv = normalize_team_names(df_hitter_adv)
    df_pitcher = normalize_team_names(df_pitcher)
    df_pitcher_adv = normalize_team_names(df_pitcher_adv)
    df_standings = normalize_team_names(df_standings)

    if date_info:
        st.markdown(
            f'<p style="text-align:center;font-size:1rem;color:#666;margin-top:-1rem;margin-bottom:2rem;">{date_info}</p>',
            unsafe_allow_html=True
        )

    # 결합
    df_hitter_combined = pd.merge(
        df_hitter,
        df_hitter_adv[['팀명','BB','IBB','HBP','SO','GDP','SLG','OBP','OPS','MH','RISP']],
        on='팀명', how='left'
    )
    # 타자 표에 득점 R도 포함되므로 그대로 유지
    df_pitcher_combined = pd.merge(
        df_pitcher,
        df_pitcher_adv[['팀명','CG','SHO','QS','BSV','TBF','NP','AVG','2B','3B','SAC','SF','IBB','WP','BK']],
        on='팀명', how='left'
    )

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 현재 순위", "🏟️ 팀별 기록", "📊 시각화", "🏆 우승확률", "📅 히스토리"])

    with tab1:
        # 피타고리안 승률 계산
        df_runs = pd.merge(
            df_hitter[['팀명','R']],
            df_pitcher[['팀명','R']].rename(columns={'R': 'RA'}),
            on='팀명', how='left'
        )
        p_n = 1.834
        df_runs['p_wpct'] = (df_runs['R']**p_n) / ((df_runs['R']**p_n) + (df_runs['RA']**p_n))
        df_runs['p_wpct'] = df_runs['p_wpct'].round(4)

        # 득점(R), 실점(RA), 피타고리안 승률을 모두 결합
        df_final = pd.merge(df_standings, df_runs[['팀명','R','RA','p_wpct']], on='팀명', how='left')
        df_final['잔여경기'] = (144 - df_final['경기']).clip(lower=0)

        # 기본 기대승수
        np.random.seed(42)
        df_final['기대승수_승률기반'] = [
            monte_carlo_expected_wins(
                p=float(r['승률']) if pd.notna(r['승률']) else 0.0,
                n_games=int(r['잔여경기']) if pd.notna(r['잔여경기']) else 0,
                n_sims=100_000
            )
            for _, r in df_final.iterrows()
        ]
        df_final['기대승수_피타고리안기반'] = [
            monte_carlo_expected_wins(
                p=float(r['p_wpct']) if pd.notna(r['p_wpct']) else 0.0,
                n_games=int(r['잔여경기']) if pd.notna(r['잔여경기']) else 0,
                n_sims=100_000
            )
            for _, r in df_final.iterrows()
        ]
        df_final['최종기대승수_승률기반'] = (df_final['승'] + df_final['기대승수_승률기반']).round(1)
        df_final['최종기대승수_피타고리안기반'] = (df_final['승'] + df_final['기대승수_피타고리안기반']).round(1)

        st.session_state['df_final'] = df_final.copy()

        # st.subheader("📊 현재 순위 및 예측 분석")
        # 필요한 컬럼이 없으면 빈 값으로 채워 안전하게 표시
        _needed = ['순위','팀명','경기','승','패','무','승률','게임차','최근10경기','R','RA','p_wpct','최종기대승수_피타고리안기반']
        for _c in _needed:
            if _c not in df_final.columns:
                df_final[_c] = pd.NA
        display = df_final[_needed].copy()
        display.rename(columns={'p_wpct':'피타고리안','최종기대승수_피타고리안기반':'예상승수'}, inplace=True)
        display['피타고리안'] = display['피타고리안'].round(4)
        safe_dataframe_display(clean_dataframe_for_display(display), use_container_width=True, hide_index=True)
        st.caption(f"원본 데이터: [KBO 팀 순위]({KBO_URLS['standings']})  |  [타자 기본]({KBO_URLS['hitter_basic1']})  |  [투수 기본]({KBO_URLS['pitcher_basic1']})")

        with st.expander("🔎 데이터 수집 디버그", expanded=False):
            try:
                st.write({
                    '타자기본': None if df_hitter is None else df_hitter.shape,
                    '타자고급': None if df_hitter_adv is None else df_hitter_adv.shape,
                    '투수기본': None if df_pitcher is None else df_pitcher.shape,
                    '투수고급': None if df_pitcher_adv is None else df_pitcher_adv.shape,
                    '순위': None if df_standings is None else df_standings.shape,
                })
                dbg_cols = st.columns(4)
                with dbg_cols[0]:
                    st.caption('타자기본 head'); st.write(None if df_hitter is None else df_hitter.head())
                with dbg_cols[1]:
                    st.caption('타자고급 head'); st.write(None if df_hitter_adv is None else df_hitter_adv.head())
                with dbg_cols[2]:
                    st.caption('투수기본 head'); st.write(None if df_pitcher is None else df_pitcher.head())
                with dbg_cols[3]:
                    st.caption('투수고급 head'); st.write(None if df_pitcher_adv is None else df_pitcher_adv.head())
                # with dbg_cols[4]:
                st.caption('순위 head'); st.write(None if df_standings is None else df_standings.head())
            except Exception as e:
                st.write(f"디버그 출력 중 오류: {e}")

    with tab2:
        # st.header("🏟️ 팀별 기록")
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("타자 기록")
            safe_dataframe_display(clean_dataframe_for_display(df_hitter_combined), True, True)
            st.caption(f"원본 데이터: [타자 기본]({KBO_URLS['hitter_basic1']}) · [타자 고급]({KBO_URLS['hitter_basic2']})")
        with c2:
            st.subheader("투수 기록")
            safe_dataframe_display(clean_dataframe_for_display(df_pitcher_combined), True, True)
            st.caption(f"원본 데이터: [투수 기본]({KBO_URLS['pitcher_basic1']}) · [투수 고급]({KBO_URLS['pitcher_basic2']})")

        st.subheader("🏆 TOP 3 팀")
        l, r = st.columns(2)
        with l:
            st.subheader("타격 상위 3팀")
            top3_avg = df_hitter_combined.nlargest(3, 'AVG')[['팀명','AVG']].reset_index(drop=True)
            cols = st.columns(3)
            for i, row in top3_avg.iterrows():
                cols[i].metric(f"{i+1}위 {row['팀명']}", f"{row['AVG']:.3f}")
            st.write("**OPS 상위 3팀**")
            top3_ops = df_hitter_combined.nlargest(3, 'OPS')[['팀명','OPS']].reset_index(drop=True)
            cols = st.columns(3)
            for i, row in top3_ops.iterrows():
                cols[i].metric(f"{i+1}위 {row['팀명']}", f"{row['OPS']:.3f}")
        with r:
            st.subheader("투수 상위 3팀")
            st.write("**ERA 상위 3팀 (낮은 순)**")
            top3_era = df_pitcher_combined.nsmallest(3, 'ERA')[['팀명','ERA']].reset_index(drop=True)
            cols = st.columns(3)
            for i, row in enumerate(top3_era.itertuples(index=False)):
                cols[i].metric(f"{i+1}위 {row.팀명}", f"{row.ERA:.2f}")
            st.write("**WHIP 상위 3팀 (낮은 순)**")
            top3_whip = df_pitcher_combined.nsmallest(3, 'WHIP')[['팀명','WHIP']].reset_index(drop=True)
            cols = st.columns(3)
            for i, row in enumerate(top3_whip.itertuples(index=False)):
                cols[i].metric(f"{i+1}위 {row.팀명}", f"{row.WHIP:.2f}")

    with tab3:
        # st.header("📊 시각화")
        c1, c2 = st.columns(2)
        with c1:
            fig1 = px.scatter(df_hitter_combined, x='AVG', y='HR', title="타율 vs 홈런", hover_data=['팀명'], text='팀명')
            fig1.update_traces(textposition="top center", marker_size=12)
            fig1.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            fig1.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            st.plotly_chart(fig1, use_container_width=True)
        with c2:
            fig2 = px.scatter(df_pitcher_combined, x='ERA', y='SO', title="ERA vs 삼진", hover_data=['팀명'], text='팀명')
            fig2.update_traces(textposition="top center", marker_size=12)
            fig2.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            fig2.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            st.plotly_chart(fig2, use_container_width=True)

        df_final = st.session_state['df_final']
        fig3 = px.scatter(df_final, x='승률', y='p_wpct', 
                    title="실제 승률 vs 피타고리안 승률", hover_data=['팀명'], text='팀명')
        fig3.add_trace(go.Scatter(x=[0.25, 0.65], y=[0.25, 0.65], mode='lines', name='기준선',
                                  line=dict(dash='dash', color='red')))
        fig3.update_traces(textposition="middle left", marker_size=12)
        fig3.update_xaxes(range=[0.25, 0.65], showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig3.update_yaxes(range=[0.25, 0.65], showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig3.update_layout(showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)
        
        st.caption(f"원본 데이터: [타자 기본]({KBO_URLS['hitter_basic1']}) · [타자 고급]({KBO_URLS['hitter_basic2']}) · [투수 기본]({KBO_URLS['pitcher_basic1']}) · [투수 고급]({KBO_URLS['pitcher_basic2']}) · [팀 순위]({KBO_URLS['standings']})")

    with tab4:
        df_final = st.session_state['df_final']
        c1, c2 = st.columns(2)
        with c1:
            championship_simulations = st.slider("우승 확률 시뮬레이션 횟수", 50_000, 300_000, 100_000, step=10_000)
        with c2:
            playoff_simulations = st.slider("플레이오프 확률 시뮬레이션 횟수", 50_000, 300_000, 100_000, step=10_000)

        if st.button("시뮬레이션 시작"):
            with st.spinner("우승/플레이오프 확률 계산 중..."):
                # 입력 검증: 비정상 입력이면 중단
                if not _validate_sim_inputs(df_final):
                    st.stop()

                champs = calculate_championship_probability(df_final, championship_simulations)
                df_final['우승확률_퍼센트'] = df_final['팀명'].map(champs)
                po = calculate_playoff_probability(df_final, playoff_simulations)
                df_final['플레이오프진출확률_퍼센트'] = df_final['팀명'].map(po)

                log_df = df_final[['팀명','우승확률_퍼센트','플레이오프진출확률_퍼센트']].copy()
                # 기준일은 순위 페이지에서 추출한 date_info 사용
                base_date = _parse_kbo_date_info_to_date(date_info) if date_info else None
                append_simulation_to_sheet(log_df, "SimulationLog", base_date=base_date)

                display_col = '최종기대승수_피타고리안기반' if '최종기대승수_피타고리안기반' in df_final.columns else '승'
                combined = df_final[['순위','팀명',display_col,'우승확률_퍼센트','플레이오프진출확률_퍼센트']].copy()
                combined.rename(columns={display_col:'예상최종승수'}, inplace=True)
                combined = combined.sort_values('우승확률_퍼센트', ascending=False).reset_index(drop=True)

                # st.subheader("🏆 KBO 우승 확률 & PO 진출 확률")
                disp = clean_dataframe_for_display(combined).rename(
                    columns={'우승확률_퍼센트':'우승확률','플레이오프진출확률_퍼센트':'PO확률'}
                )
                safe_dataframe_display(disp, True, True)

                cc1, cc2 = st.columns(2)
                with cc1:
                    fig = px.bar(combined, x='팀명', y='우승확률_퍼센트', title="팀별 우승 확률",
                                 color='우승확률_퍼센트', color_continuous_scale='RdYlGn')
                    try:
                        fig.update_traces(text=combined['우승확률_퍼센트'], texttemplate='%{text:.3f}%', textposition='outside', cliponaxis=False)
                    except Exception:
                        pass
                    fig.update_layout(xaxis_tickangle=-45, showlegend=False, coloraxis_showscale=False)
                    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray', range=[0,100], dtick=10, ticksuffix='%')
                    st.plotly_chart(fig, use_container_width=True)

                with cc2:
                    fig2 = px.bar(combined, x='팀명', y='플레이오프진출확률_퍼센트', title="팀별 플레이오프 진출 확률",
                                color='플레이오프진출확률_퍼센트', color_continuous_scale='Blues')
                    try:
                        fig2.update_traces(text=combined['플레이오프진출확률_퍼센트'], texttemplate='%{text:.2f}%', textposition='outside', cliponaxis=False)
                    except Exception:
                        pass
                    fig2.update_layout(xaxis_tickangle=-45, showlegend=False, coloraxis_showscale=False)
                    fig2.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                    fig2.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray', range=[0,100], dtick=10, ticksuffix='%')
                    st.plotly_chart(fig2, use_container_width=True)
                st.caption(f"원본 데이터: [팀 순위]({KBO_URLS['standings']})")

        # Bradley-Terry 모형 기반 순위 예측 히트맵
        st.subheader("🔥 Bradley-Terry 모형 순위 예측 히트맵")
        st.markdown("""
        **방법론**: 팀간 상대 전적을 기반으로 Bradley-Terry 모형으로 팀 강도를 추정하고, 
        상대당 16경기 기준 잔여 일정을 10만 회 시뮬레이션하여 최종 순위 분포를 예측합니다.
        """)
        
        if st.button("Bradley-Terry 순위 예측 시작"):
            with st.spinner("Bradley-Terry 모형으로 순위 예측 계산 중..."):
                try:
                    # 1) 팀간 승패표 크롤링
                    url_vs = "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx"
                    raw_vs, soup_vs = _first_table_html(url_vs)
                    
                    if raw_vs is None or soup_vs is None:
                        st.error("팀간 승패표를 가져올 수 없습니다.")
                        st.stop()
                    
                    # 두 번째 테이블(팀간 승패표) 찾기
                    tables = soup_vs.find_all("table")
                    if len(tables) < 2:
                        st.error("팀간 승패표를 찾을 수 없습니다.")
                        st.stop()
                    
                    # 두 번째 테이블 파싱
                    df_vs_raw = pd.read_html(str(tables[1]))[0]
                    
                    # 2) 데이터 정규화
                    teams = df_final['팀명'].tolist()
                    n = len(teams)
                    idx = {t: i for i, t in enumerate(teams)}
                    
                    # 팀간 승패표 정규화
                    def normalize_tvt(df_vs, teams):
                        # 컬럼명에서 팀명만 추출 (예: "LG (승-패-무)" -> "LG")
                        new_cols = []
                        for c in df_vs.columns:
                            col_str = str(c).strip()
                            if col_str == "팀명":
                                new_cols.append("팀명")
                            else:
                                # 팀명 추출 (괄호 앞부분)
                                team_name = col_str.split(" (")[0].strip()
                                if team_name in teams:
                                    new_cols.append(team_name)
                                else:
                                    new_cols.append(col_str)
                        
                        df_vs.columns = new_cols
                        
                        # 팀명 컬럼 처리
                        if "팀명" not in df_vs.columns:
                            df_vs.rename(columns={df_vs.columns[0]: "팀명"}, inplace=True)
                        df_vs["팀명"] = df_vs["팀명"].astype(str).str.strip()
                        
                        # 필요한 컬럼만 선택
                        available_teams = [t for t in teams if t in df_vs.columns]
                        cols = ["팀명"] + available_teams
                        df_vs = df_vs[[c for c in cols if c in df_vs.columns]].copy()
                        
                        # 데이터 정리
                        for c in df_vs.columns[1:]:
                            df_vs[c] = df_vs[c].astype(str).str.replace(r"\s+", "", regex=True)
                        
                        return df_vs
                    
                    df_vs = normalize_tvt(df_vs_raw, teams)
                    
                    # 승패무 파싱
                    def parse_wlt(cell):
                        s = str(cell).strip()
                        if s in ["■", "-", "—", "–", "nan", "None", ""] or s.lower() == "nan":
                            return (np.nan, np.nan, np.nan)
                        parts = s.split("-")
                        try:
                            if len(parts) == 3:
                                return int(parts[0]), int(parts[1]), int(parts[2])
                            if len(parts) == 2:
                                return int(parts[0]), int(parts[1]), 0
                        except:
                            pass
                        return (np.nan, np.nan, np.nan)
                    
                    # 승패무 행렬 구성
                    W = np.zeros((n, n), dtype=int)
                    L = np.zeros((n, n), dtype=int)
                    T = np.zeros((n, n), dtype=int)
                    
                    for _, row in df_vs.iterrows():
                        i = idx.get(row["팀명"])
                        if i is None:
                            continue
                        for opp in teams:
                            if opp not in df_vs.columns or i == idx[opp]:
                                continue
                            w, l, t = parse_wlt(row[opp])
                            if not np.isnan(w):
                                j = idx[opp]
                                W[i, j], L[i, j], T[i, j] = int(w), int(l), int(t)
                    
                    # 대칭 보정
                    for i in range(n):
                        for j in range(n):
                            if i == j:
                                continue
                            W[j, i], L[j, i], T[j, i] = L[i, j], W[i, j], T[i, j]
                    
                    G_played = W + L + T
                    
                    # 3) Bradley-Terry 모형 추정
                    def bt_fit(W, T, G, max_iter=1000, tol=1e-10):
                        n = W.shape[0]
                        s = np.ones(n)
                        s /= s.sum()
                        
                        def update(s):
                            new_s = np.zeros_like(s, dtype=float)
                            for i in range(n):
                                w_i = (W[i, :] + 0.5 * T[i, :]).sum()
                                denom = 0.0
                                for j in range(n):
                                    if i == j:
                                        continue
                                    n_ij = G[i, j]
                                    if n_ij > 0:
                                        denom += n_ij / (s[i] + s[j])
                                new_s[i] = w_i / denom if denom > 0 else s[i]
                            new_s = np.clip(new_s, 1e-12, None)
                            return new_s / new_s.sum()
                        
                        for _ in range(max_iter):
                            new_s = update(s)
                            if np.max(np.abs(new_s - s)) < tol:
                                return new_s
                            s = new_s
                        return s
                    
                    s = bt_fit(W, T, G_played)
                    S = s.reshape(-1, 1)
                    
                    # 확률 계산 시 안전장치 추가
                    with np.errstate(divide='ignore', invalid='ignore'):
                        P = S / (S + S.T)
                        # NaN, 무한대, 음수 값 처리
                        P = np.nan_to_num(P, nan=0.5, posinf=1.0, neginf=0.0)
                        P = np.clip(P, 0.0, 1.0)
                    np.fill_diagonal(P, 0.0)
                    
                    # 무승부율 계산
                    with np.errstate(divide='ignore', invalid='ignore'):
                        tie_pair = np.where(G_played > 0, T / G_played, np.nan)
                    league_tie_rate = float(np.nanmean(tie_pair))
                    tie_pair = np.where(np.isnan(tie_pair), league_tie_rate, tie_pair)
                    # 무승부율도 안전장치 추가
                    tie_pair = np.nan_to_num(tie_pair, nan=0.0, posinf=1.0, neginf=0.0)
                    tie_pair = np.clip(tie_pair, 0.0, 1.0)
                    np.fill_diagonal(tie_pair, 0.0)
                    
                    # 4) 시뮬레이션
                    TARGET_PER_PAIR = 16
                    R = np.maximum(0, TARGET_PER_PAIR - G_played)
                    np.fill_diagonal(R, 0)
                    
                    SEASONS = 1_000_000
                    rng = np.random.default_rng(42)
                    
                    cur_w = df_final.set_index("팀명").loc[teams, "승"].to_numpy()
                    cur_l = df_final.set_index("팀명").loc[teams, "패"].to_numpy()
                    cur_t = df_final.set_index("팀명").loc[teams, "무"].to_numpy()
                    
                    final_w = np.zeros((SEASONS, n), dtype=np.int32)
                    final_l = np.zeros((SEASONS, n), dtype=np.int32)
                    final_t = np.zeros((SEASONS, n), dtype=np.int32)
                    
                    pairs = [(i, j) for i in range(n) for j in range(i + 1, n) if R[i, j] > 0]
                    for (i, j) in pairs:
                        r = int(R[i, j])
                        # 확률값 안전장치
                        tie_prob = np.clip(float(tie_pair[i, j]), 0.0, 1.0)
                        win_prob = np.clip(float(P[i, j]), 0.0, 1.0)
                        
                        ties = rng.binomial(r, tie_prob, size=SEASONS)
                        non_ties = r - ties
                        wins_i = rng.binomial(non_ties, win_prob, size=SEASONS)
                        wins_j = non_ties - wins_i
                        
                        final_w[:, i] += wins_i
                        final_l[:, i] += wins_j
                        final_t[:, i] += ties
                        final_w[:, j] += wins_j
                        final_l[:, j] += wins_i
                        final_t[:, j] += ties
                    
                    # 현재 성적 합산
                    final_w += cur_w
                    final_l += cur_l
                    final_t += cur_t
                    
                    # 최종 승률
                    games_tot = final_w + final_l + final_t
                    with np.errstate(divide='ignore', invalid='ignore'):
                        win_pct = (final_w + 0.5 * final_t) / np.maximum(1, games_tot)
                        # 승률도 안전장치 추가
                        win_pct = np.nan_to_num(win_pct, nan=0.0, posinf=1.0, neginf=0.0)
                        win_pct = np.clip(win_pct, 0.0, 1.0)
                    
                    # 순위 산정
                    noise = rng.normal(0, 1e-9, size=win_pct.shape)
                    rank_order = np.argsort(-(win_pct + noise), axis=1)
                    seed = np.empty_like(rank_order)
                    for s_idx in range(SEASONS):
                        seed[s_idx, rank_order[s_idx]] = np.arange(1, n + 1)
                    
                    # 5) 순위 분포 계산
                    rank_pct = np.zeros((n, n), dtype=float)
                    for i in range(n):
                        counts = np.bincount(seed[:, i], minlength=n + 1)[1:]
                        rank_pct[i] = (counts / SEASONS) * 100.0
                    
                    rank_cols = [f"{r}위" for r in range(1, n + 1)]
                    rank_df = pd.DataFrame(rank_pct, columns=rank_cols, index=teams).round(1)
                    
                    # 승패무 행렬을 보기 좋게 표시
                    def create_vs_table(W, L, T, teams):
                        vs_data = []
                        for i, team1 in enumerate(teams):
                            row = [team1]
                            for j, team2 in enumerate(teams):
                                if i == j:
                                    row.append("-")
                                else:
                                    w, l, t = W[i, j], L[i, j], T[i, j]
                                    if w == 0 and l == 0 and t == 0:
                                        row.append("-")
                                    else:
                                        row.append(f"{w}-{l}-{t}")
                            vs_data.append(row)
                        
                        vs_cols = ["팀명"] + teams
                        return pd.DataFrame(vs_data, columns=vs_cols)
                    
                    # 7) 히트맵 시각화 (현재 순위 순서로 정렬)
                    fig_heatmap = go.Figure()
                    
                    # 현재 순위 순서로 팀 정렬 (1위부터 10위까지)
                    current_rank_order = df_final.sort_values('순위')['팀명'].tolist()
                    # rank_pct 행렬을 현재 순위 순서로 재정렬
                    rank_pct_sorted = rank_pct[[teams.index(team) for team in current_rank_order]]
                    teams_sorted = current_rank_order
                    
                    # 흰색→빨강 색상맵
                    colorscale = [[0, 'white'], [1, 'red']]
                    
                    fig_heatmap.add_trace(go.Heatmap(
                        z=rank_pct_sorted,
                        x=rank_cols,
                        y=teams_sorted,
                        colorscale=colorscale,
                        zmin=0,
                        zmax=100,
                        text=rank_pct_sorted.round(1),
                        texttemplate="%{text:.2f}",
                        textfont={"size": 10},
                        showscale=True,
                        colorbar=dict(title=dict(text="확률 (%)", side="right")),
                        showlegend=False
                    ))
                    
                    fig_heatmap.update_layout(
                        title="Bradley-Terry 모형 기반 팀별 최종 순위 예측 (10만 회 시뮬레이션)",
                        xaxis_title="최종 순위",
                        yaxis_title="팀명 (현재 순위 순)",
                        width=800,
                        height=500,
                        showlegend=False
                    )
                    
                    fig_heatmap.update_xaxes(showgrid=False)
                    fig_heatmap.update_yaxes(showgrid=False)
                    
                    st.plotly_chart(fig_heatmap, use_container_width=True)                    
                    st.success("Bradley-Terry 모형 순위 예측이 완료되었습니다!")
                                        # 결과 테이블 표시 (현재 순위 순서로 정렬)
                    # 6) 팀간 승패표 표시
                    #   st.subheader("📊 팀간 승패표 (Bradley-Terry 모형 입력 데이터)")
                    with st.expander("🔍 팀간 승패표 (Bradley-Terry 모형 입력 데이터)", expanded=False):                  
                        vs_table = create_vs_table(W, L, T, teams)
                        safe_dataframe_display(vs_table, use_container_width=True, hide_index=True)

                    with st.expander("🔍 순위별 확률 분포 (%)", expanded=False):
                        # st.subheader("📊 순위별 확률 분포 (%)")
                        rank_df_sorted = rank_df.loc[current_rank_order].reset_index().rename(columns={"index": "팀명"})
                        safe_dataframe_display(rank_df_sorted, use_container_width=True, hide_index=True)

                    
                except Exception as e:
                    st.error(f"Bradley-Terry 모형 계산 중 오류가 발생했습니다: {str(e)}")
                    st.info("팀간 승패표 데이터를 가져오는 데 문제가 있을 수 있습니다. 잠시 후 다시 시도해주세요.")
                    
                    # 디버그 정보 추가
                    with st.expander("🔍 디버그 정보", expanded=False):
                        try:
                            st.write("팀간 승패표 원본 데이터:")
                            st.write(df_vs_raw.head())
                            st.write("정규화된 팀간 승패표:")
                            st.write(df_vs.head())
                            st.write("승패무 행렬 정보:")
                            st.write(f"W 행렬 형태: {W.shape}")
                            st.write(f"L 행렬 형태: {L.shape}")
                            st.write(f"T 행렬 형태: {T.shape}")
                            st.write(f"G_played 행렬 형태: {G_played.shape}")
                        except Exception as debug_e:
                            st.write(f"디버그 정보 출력 중 오류: {debug_e}")

    with tab5:
        # st.header("📅 시뮬레이션 이력")
        try:
            ws = _open_log_worksheet("SimulationLog")
            if ws is None:
                return
            # 많은 행이 있을 수 있으므로 get_all_records 대신 get_all_values 후 DataFrame 변환
            values = ws.get_all_values()
            if not values or len(values) < 2:
                st.info("아직 시뮬레이션 이력이 없습니다.")
                return
            header, rows = values[0], values[1:]
            df_hist = pd.DataFrame(rows, columns=header)
            # 스키마 정규화
            rename_map = {
                '우승': '우승',
                'PO': 'PO',
                '팀명': '팀명',
                'timestamp': 'timestamp',
                'base_date': 'base_date',
            }
            for k, v in list(rename_map.items()):
                if k not in df_hist.columns and v in df_hist.columns:
                    # 이미 원하는 이름이면 스킵
                    continue
                if k in df_hist.columns:
                    df_hist.rename(columns={k: v}, inplace=True)
            # 타입 캐스팅
            if 'timestamp' in df_hist.columns:
                try:
                    df_hist['timestamp'] = pd.to_datetime(df_hist['timestamp'], errors='coerce')
                except Exception:
                    pass
            if 'base_date' in df_hist.columns:
                try:
                    df_hist['base_date'] = pd.to_datetime(df_hist['base_date'], errors='coerce').dt.date
                except Exception:
                    pass
            for col in ['우승', 'PO']:
                if col in df_hist.columns:
                    df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce')
            if df_hist.empty:
                st.info("아직 시뮬레이션 이력이 없습니다.")
                return
            # 일자 컬럼 생성
            # 날짜 소스 선택: 기본은 기준일자(base_date), 필요 시 실행일(timestamp) 기준으로 전환 가능
            src_col1, src_col2 = st.columns(2)
            # with src_col1:
            #     use_run_date = st.checkbox("실행일(로그 시각) 기준으로 보기", value=False)
            # 기준일자 우선 또는 실행일 선택
            use_run_date = False
            if use_run_date and 'timestamp' in df_hist.columns:
                df_hist['date'] = pd.to_datetime(df_hist['timestamp'], errors='coerce').dt.date
            elif 'base_date' in df_hist.columns and df_hist['base_date'].notna().any():
                df_hist['date'] = df_hist['base_date']
            elif 'timestamp' in df_hist.columns:
                df_hist['date'] = pd.to_datetime(df_hist['timestamp'], errors='coerce').dt.date
            else:
                df_hist['date'] = pd.NaT
            # n_days = st.number_input("최근 N일", min_value=3, max_value=180, value=30, step=1)
            show_markers = True #st.checkbox("마커 표시", value=True)

            # 일자별 집계: 동일 일자에 여러 로그가 있으면 평균으로 집계(팀별)
            df_day = df_hist.groupby(['date','팀명'], as_index=False).agg({'우승':'mean','PO':'mean'})
            df_day = df_day.sort_values(['date','팀명'])
            # 최근 N일 필터
            # try:
            #     if df_day['date'].notna().any():
            #         last_date = pd.to_datetime(df_day['date']).max()
            #         start_date = (pd.to_datetime(last_date) - pd.Timedelta(days=int(n_days)-1)).date()
            #         mask = pd.to_datetime(df_day['date']).dt.date >= start_date
            #         df_day = df_day.loc[mask]
            # except Exception:
            #     pass

            # 범례(팀명) 정렬: 현재 순위 승률 높은 순
            try:
                team_order = df_standings.sort_values('승률', ascending=False)['팀명'].tolist() if '승률' in df_standings.columns else df_standings['팀명'].tolist()
            except Exception:
                team_order = None

            # 팀별 라인플랏(우승) — 일자별 평균
            if {'date','팀명','우승'}.issubset(df_day.columns):
                fig_c = px.line(
                    df_day, x='date', y='우승', color='팀명', markers=show_markers,
                    title='팀별 우승 확률 (일자별)',
                    category_orders={'팀명': team_order} if team_order else None
                )
                try:
                    # 라인 색상 고정
                    for tr in fig_c.data:
                        team = tr.name
                        if team in TEAM_COLOR_MAP:
                            tr.line.color = TEAM_COLOR_MAP[team]
                            tr.marker.color = TEAM_COLOR_MAP[team]
                except Exception:
                    pass
                # 마커 사이즈 키우기
                try:
                    fig_c.update_traces(marker=dict(size=10))
                except Exception:
                    pass
                fig_c.update_yaxes(range=[0, 100], dtick=10, ticksuffix='%')
                st.plotly_chart(fig_c, use_container_width=True)
                # 그래프 바로 아래에 해당 데이터(피벗) 표시
                try:
                    pivot_win = (
                        df_day.pivot_table(index='date', columns='팀명', values='우승', aggfunc='mean').sort_index()
                    )
                    if team_order:
                        existing_cols = [c for c in team_order if c in pivot_win.columns]
                        pivot_win = pivot_win.reindex(columns=existing_cols)
                    pivot_win = pivot_win.dropna(how='all')
                    with st.expander("🔎 일자별 우승 확률", expanded=False):
                        safe_dataframe_display(pivot_win.round(2).reset_index(), use_container_width=True, hide_index=True)
                except Exception:
                    pass
            # 팀별 라인플랏(PO) — 일자별 평균
            if {'date','팀명','PO'}.issubset(df_day.columns):
                fig_p = px.line(
                    df_day, x='date', y='PO', color='팀명', markers=show_markers,
                    title='팀별 PO 진출 확률 (일자별)',
                    category_orders={'팀명': team_order} if team_order else None
                )
                try:
                    for tr in fig_p.data:
                        team = tr.name
                        if team in TEAM_COLOR_MAP:
                            tr.line.color = TEAM_COLOR_MAP[team]
                            tr.marker.color = TEAM_COLOR_MAP[team]
                except Exception:
                    pass
                # 마커 사이즈 키우기
                try:
                    fig_p.update_traces(marker=dict(size=10))
                except Exception:
                    pass
                fig_p.update_yaxes(range=[0, 100], dtick=10, ticksuffix='%')
                st.plotly_chart(fig_p, use_container_width=True)
                try:
                    pivot_po = (
                        df_day.pivot_table(index='date', columns='팀명', values='PO', aggfunc='mean').sort_index()
                    )
                    if team_order:
                        existing_cols_po = [c for c in team_order if c in pivot_po.columns]
                        pivot_po = pivot_po.reindex(columns=existing_cols_po)
                    pivot_po = pivot_po.dropna(how='all')
                    with st.expander("🔎 일자별 PO 확률", expanded=False):
                        safe_dataframe_display(pivot_po.round(2).reset_index(), use_container_width=True, hide_index=True)
                except Exception:
                    pass

            # 표는 아래로 이동하여 원본 기록을 그대로 표시
            df_hist_sorted = df_hist.sort_values('timestamp') if 'timestamp' in df_hist else df_hist

            with st.expander("🔎 원본 데이터", expanded=False):
                st.dataframe(df_hist_sorted.drop(columns=['timestamp','date']).sort_values(['base_date', '팀명'], ascending=False), use_container_width=True,
                            hide_index=True)
        except Exception as e:
            st.info("이력 로딩 중 오류가 발생했습니다. " + str(e))

if __name__ == "__main__":
    main()
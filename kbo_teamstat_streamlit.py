# kbo_teamstat_streamlit.py
from io import StringIO
import streamlit as st
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import gspread
from gspread.exceptions import APIError as GspreadAPIError
from google.oauth2.service_account import Credentials
import plotly.express as px
import plotly.graph_objects as go

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
HEADERS = {'User-Agent': 'Mozilla/5.0'}

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

def append_simulation_to_sheet(df_result: pd.DataFrame, sheet_name="SimulationLog"):
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

        df_out = df_result.copy()
        df_out.insert(0, "timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

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

        st.success(f"시뮬레이션 결과가 '{sheet_name}' 시트에 저장되었습니다.")
    except Exception as e:
        st.error("Google Sheets 저장 중 알 수 없는 오류:\n" + _format_gspread_error(e))

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
            try:
                dfc[c] = pd.to_numeric(dfc[c])
            except Exception:
                continue

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
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        # read_html 경고 방지: StringIO로 감싼 literal HTML 사용
        try:
            tables = pd.read_html(StringIO(r.text))
            if tables:
                return tables[0], soup
        except Exception:
            pass
        table = soup.find("table")
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

# -----------------------------
# 스크래핑 함수
# -----------------------------
@st.cache_data(ttl=3600)
def scrape_kbo_team_batting_stats():
    url = "https://www.koreabaseball.com/Record/Team/Hitter/Basic1.aspx"
    df, _ = _first_table_html(url)
    if df is None or df.empty:
        st.error("타자 기본 기록 테이블을 찾을 수 없습니다.")
        return None
    df = df[df.iloc[:,0].isin(TEAM_NAMES)].copy()
    cols = ['팀명','AVG','G','PA','AB','R','H','2B','3B','HR','TB','RBI','SAC','SF']
    take = min(len(df.columns), len(cols))
    df = df.iloc[:, :take].copy()
    df.columns = cols[:take]
    for c in df.columns:
        if c != '팀명':
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.sort_values('AVG', ascending=False).reset_index(drop=True)
    df.insert(0, '순위', range(1, len(df)+1))
    return df

@st.cache_data(ttl=3600)
def scrape_kbo_team_batting_stats_advanced():
    url = "https://www.koreabaseball.com/Record/Team/Hitter/Basic2.aspx"
    df, _ = _first_table_html(url)
    if df is None or df.empty:
        st.error("타자 고급 기록 테이블을 찾을 수 없습니다.")
        return None
    df = df[df.iloc[:,0].isin(TEAM_NAMES)].copy()
    cols = ['팀명','AVG','BB','IBB','HBP','SO','GDP','SLG','OBP','OPS','MH','RISP']
    take = min(len(df.columns), len(cols))
    df = df.iloc[:, :take].copy()
    df.columns = cols[:take]
    for c in df.columns:
        if c != '팀명':
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.sort_values('AVG', ascending=False).reset_index(drop=True)
    df.insert(0, '순위', range(1, len(df)+1))
    return df

@st.cache_data(ttl=3600)
def scrape_kbo_team_pitching_stats():
    url = "https://www.koreabaseball.com/Record/Team/Pitcher/Basic1.aspx"
    df, _ = _first_table_html(url)
    if df is None or df.empty:
        st.error("투수 기본 기록 테이블을 찾을 수 없습니다.")
        return None
    df = df[df.iloc[:,0].isin(TEAM_NAMES)].copy()
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
    df.insert(0, '순위', range(1, len(df)+1))
    return df

@st.cache_data(ttl=3600)
def scrape_kbo_team_pitching_stats_advanced():
    url = "https://www.koreabaseball.com/Record/Team/Pitcher/Basic2.aspx"
    df, _ = _first_table_html(url)
    if df is None or df.empty:
        st.error("투수 고급 기록 테이블을 찾을 수 없습니다.")
        return None
    df = df[df.iloc[:,0].isin(TEAM_NAMES)].copy()
    cols = ['팀명','ERA','CG','SHO','QS','BSV','TBF','NP','AVG','2B','3B','SAC','SF','IBB','WP','BK']
    take = min(len(df.columns), len(cols))
    df = df.iloc[:, :take].copy()
    df.columns = cols[:take]
    for c in df.columns:
        if c != '팀명':
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.sort_values('ERA', ascending=True).reset_index(drop=True)
    df.insert(0, '순위', range(1, len(df)+1))
    return df

@st.cache_data(ttl=3600)
def scrape_kbo_standings():
    url = "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx"
    df, soup = _first_table_html(url)
    date_info = None
    if soup:
        all_texts = soup.get_text("\n")
        m = re.search(r"\(\d{4}년\s*\d{1,2}월\s*\d{1,2}일\s*기준\)", all_texts)
        if m:
            date_info = m.group(0)
    if df is None or df.empty:
        st.error("순위 테이블을 찾을 수 없습니다.")
        return None, date_info
    df = df[df.iloc[:,0].isin(['LG','한화','롯데','삼성','SSG','NC','KIA','두산','KT','키움'])].copy()
    cols = ['팀명','경기','승','패','무','승률','게임차','최근10경기']
    take = min(len(df.columns), len(cols))
    df = df.iloc[:, :take].copy()
    df.columns = cols[:take]
    for c in ['경기','승','패','무','승률']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.sort_values('승률', ascending=False).reset_index(drop=True)
    df.insert(0, '순위', range(1, len(df)+1))
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
        winners[names[int(np.argmax(current_wins))]] = 100.0
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
        if final_wins.size == 0:
            # 안전망: T==0 또는 B==0
            continue

        idx = np.argmax(final_wins, axis=1)  # (B,)
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
        if final_wins.size == 0:
            continue

        # 빠른 상위 선택
        topk_idx = np.argpartition(-final_wins, kth=top_k - 1, axis=1)[:, :top_k]
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
    df_pitcher_combined = pd.merge(
        df_pitcher,
        df_pitcher_adv[['팀명','CG','SHO','QS','BSV','TBF','NP','AVG','2B','3B','SAC','SF','IBB','WP','BK']],
        on='팀명', how='left'
    )

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 현재 순위", "🏟️ 팀별 기록", "📊 시각화", "🏆 우승 확률", "📅 시뮬레이션 이력"])

    with tab1:
        # 피타고리안 승률 계산
        df_runs = pd.merge(
            df_hitter[['팀명','R']],
            df_pitcher[['팀명','R']],
            on='팀명', how='left', suffixes=('', '_A')
        )
        df_runs.rename(columns={'R': 'R', 'R_A': 'RA'}, inplace=True)
        p_n = 1.834
        df_runs['p_wpct'] = (df_runs['R']**p_n) / ((df_runs['R']**p_n) + (df_runs['RA']**p_n))
        df_runs['p_wpct'] = df_runs['p_wpct'].round(4)

        df_final = pd.merge(df_standings, df_runs[['팀명','p_wpct']], on='팀명', how='left')
        df_final['잔여경기'] = (144 - df_final['경기']).clip(lower=0)

        # 기본 기대승수
        np.random.seed(42)
        df_final['기대승수_승률기반'] = [
            monte_carlo_expected_wins(p=float(r['승률']), n_games=int(r['잔여경기']), n_sims=10_000)
            for _, r in df_final.iterrows()
        ]
        df_final['기대승수_피타고리안기반'] = [
            monte_carlo_expected_wins(p=float(r['p_wpct']), n_games=int(r['잔여경기']), n_sims=10_000)
            for _, r in df_final.iterrows()
        ]
        df_final['최종기대승수_승률기반'] = (df_final['승'] + df_final['기대승수_승률기반']).round(1)
        df_final['최종기대승수_피타고리안기반'] = (df_final['승'] + df_final['기대승수_피타고리안기반']).round(1)

        st.session_state['df_final'] = df_final.copy()

        st.subheader("📊 현재 순위 및 예측 분석")
        display = df_final[['순위','팀명','경기','승','패','무','승률','게임차','최근10경기','p_wpct','최종기대승수_피타고리안기반']].copy()
        display.rename(columns={'p_wpct':'피타고리안승률','최종기대승수_피타고리안기반':'예상최종승수'}, inplace=True)
        display['피타고리안승률'] = display['피타고리안승률'].round(4)
        safe_dataframe_display(clean_dataframe_for_display(display), use_container_width=True, hide_index=True)

    with tab2:
        st.header("🏟️ 팀별 기록")
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("타자 기록")
            safe_dataframe_display(clean_dataframe_for_display(df_hitter_combined), True, True)
        with c2:
            st.subheader("투수 기록")
            safe_dataframe_display(clean_dataframe_for_display(df_pitcher_combined), True, True)

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
        st.header("📊 시각화")
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
        fig3 = px.scatter(df_final, x='승률', y='p_wpct', title="실제 승률 vs 피타고리안 승률", hover_data=['팀명'], text='팀명')
        fig3.add_trace(go.Scatter(x=[0.25, 0.65], y=[0.25, 0.65], mode='lines', name='기준선',
                                  line=dict(dash='dash', color='red')))
        fig3.update_traces(textposition="top center", marker_size=12)
        fig3.update_xaxes(range=[0.25, 0.65], showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig3.update_yaxes(range=[0.25, 0.65], showgrid=True, gridwidth=1, gridcolor='lightgray')
        st.plotly_chart(fig3, use_container_width=True)

    with tab4:
        df_final = st.session_state['df_final']
        c1, c2 = st.columns(2)
        with c1:
            championship_simulations = st.slider("우승 확률 시뮬레이션 횟수", 5_000, 50_000, 5_000, step=5_000)
        with c2:
            playoff_simulations = st.slider("플레이오프 확률 시뮬레이션 횟수", 5_000, 50_000, 5_000, step=5_000)

        if 'df_final' in st.session_state:
            with st.expander("🔧 시뮬레이션 입력 디버그", expanded=False):
                df_dbg = st.session_state['df_final'].copy()
                st.write("입력 DF 샘플:", df_dbg.head(10))
                st.write("행/열:", df_dbg.shape)
                st.write("필수 컬럼 존재 여부:", {c: (c in df_dbg.columns) for c in ["팀명","승","p_wpct","잔여경기"]})
                st.write("결측치 개수:", df_dbg[["팀명","승","p_wpct","잔여경기"]].isna().sum())


        if st.button("시뮬레이션 시작"):
            with st.spinner("우승/플레이오프 확률 계산 중..."):
                champs = calculate_championship_probability(df_final, championship_simulations)
                df_final['우승확률_퍼센트'] = df_final['팀명'].map(champs)
                po = calculate_playoff_probability(df_final, playoff_simulations)
                df_final['플레이오프진출확률_퍼센트'] = df_final['팀명'].map(po)

                log_df = df_final[['팀명','우승확률_퍼센트','플레이오프진출확률_퍼센트']].copy()
                append_simulation_to_sheet(log_df, "SimulationLog")

                display_col = '최종기대승수_피타고리안기반' if '최종기대승수_피타고리안기반' in df_final.columns else '승'
                combined = df_final[['순위','팀명',display_col,'우승확률_퍼센트','플레이오프진출확률_퍼센트']].copy()
                combined.rename(columns={display_col:'예상최종승수'}, inplace=True)
                combined = combined.sort_values('우승확률_퍼센트', ascending=False).reset_index(drop=True)

                st.subheader("🏆 KBO 우승 확률 & PO 진출 확률")
                cc1, cc2 = st.columns(2)
                with cc1:
                    disp = clean_dataframe_for_display(combined).rename(
                        columns={'우승확률_퍼센트':'우승확률','플레이오프진출확률_퍼센트':'PO확률'}
                    )
                    safe_dataframe_display(disp, True, True)
                with cc2:
                    fig = px.bar(combined, x='팀명', y='우승확률_퍼센트', title="팀별 우승 확률",
                                 color='우승확률_퍼센트', color_continuous_scale='RdYlGn')
                    fig.update_layout(xaxis_tickangle=-45)
                    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                    st.plotly_chart(fig, use_container_width=True)

                fig2 = px.bar(combined, x='팀명', y='플레이오프진출확률_퍼센트', title="팀별 플레이오프 진출 확률",
                              color='플레이오프진출확률_퍼센트', color_continuous_scale='Blues')
                fig2.update_layout(xaxis_tickangle=-45)
                fig2.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                fig2.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                st.plotly_chart(fig2, use_container_width=True)

    with tab5:
        st.header("📅 시뮬레이션 이력")
        try:
            client = get_gsheet_client()
            if client is None:
                st.info("Google Sheets 연결이 설정되지 않았습니다. 이력을 불러올 수 없습니다.")
                st.warning("진단 정보:\n" + _diagnose_gsheet_setup())
            else:
                try:
                    ws = client.open("KBO_Simulation_Log").worksheet("SimulationLog")
                except Exception:
                    st.info("아직 로그 시트가 없습니다. 우승 확률 탭에서 시뮬레이션을 실행해보세요.")
                    return
                history = ws.get_all_records()
                df_hist = pd.DataFrame(history)
                if df_hist.empty:
                    st.info("아직 시뮬레이션 이력이 없습니다.")
                else:
                    df_hist['timestamp'] = pd.to_datetime(df_hist['timestamp'])
                    df_sum = df_hist.groupby('timestamp', as_index=False).agg({
                        '우승확률_퍼센트':'mean',
                        '플레이오프진출확률_퍼센트':'mean'
                    })
                    fig = px.line(df_sum, x='timestamp',
                                  y=['우승확률_퍼센트','플레이오프진출확률_퍼센트'],
                                  title='일자별 평균 우승 / 플레이오프 확률', markers=True)
                    fig.update_layout(xaxis_title="날짜", yaxis_title="확률(%)")
                    st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.info(f"Google Sheets 연결에 문제가 있습니다. 이력을 불러올 수 없습니다. {e}")

if __name__ == "__main__":
    main()

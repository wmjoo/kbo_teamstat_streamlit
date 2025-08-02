# KBO 팀 통계 분석기

⚾ KBO(한국야구위원회) 팀들의 실시간 통계를 분석하고 시각화하는 Streamlit 애플리케이션입니다.

## 주요 기능

- 📈 **실시간 순위**: 현재 KBO 팀 순위 및 승률 분석
- 🏟️ **팀별 기록**: 타자 및 투수 기록 상세 분석
- 📊 **시각화**: 다양한 통계 차트 및 그래프
- 🏆 **우승 확률**: 몬테카를로 시뮬레이션을 통한 우승 확률 계산
- 🎯 **플레이오프 확률**: 상위 5팀 진출 확률 분석

## 기술 스택

- **Frontend**: Streamlit
- **Data Processing**: Pandas, NumPy
- **Web Scraping**: Requests, BeautifulSoup4
- **Visualization**: Plotly
- **Parsing**: lxml

## 설치 및 실행

### 로컬 실행

1. 저장소 클론
```bash
git clone <repository-url>
cd kbo_teamstat_streamlit
```

2. 가상환경 생성 및 활성화
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. 의존성 설치
```bash
pip install -r requirements.txt
```

4. 애플리케이션 실행
```bash
streamlit run kbo_teamstat_streamlit.py
```

### Streamlit Cloud 배포

1. GitHub에 코드 푸시
2. [Streamlit Cloud](https://share.streamlit.io/)에서 새 앱 생성
3. GitHub 저장소 연결
4. 메인 파일 경로: `kbo_teamstat_streamlit.py`

## 데이터 소스

- [KBO 공식 웹사이트](https://www.koreabaseball.com/)
- 실시간 팀 타자 기록
- 실시간 팀 투수 기록
- 실시간 순위 정보

## 분석 지표

### 타자 기록
- 타율 (AVG)
- 출루율 (OBP)
- 장타율 (SLG)
- OPS
- 홈런 (HR)
- 타점 (RBI)

### 투수 기록
- 평균자책점 (ERA)
- WHIP
- 삼진 (SO)
- 완투 (CG)
- 완봉 (SHO)

### 예측 모델
- 피타고리안 승률
- 몬테카를로 시뮬레이션
- 우승 확률
- 플레이오프 진출 확률

## 라이선스

MIT License

## 기여

버그 리포트나 기능 제안은 이슈를 통해 제출해주세요. 
# ============ Stage 1: Builder ============
FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /app

# 시스템 의존성 (pyodbc 등 C 확장 빌드용) - builder에서만
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        gnupg \
        unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

# 파이썬 의존성 설치 (레이어 캐싱 최적화)
COPY requirements.txt .
RUN pip install --user -r requirements.txt

# 소스 복사
COPY . .

# ============ Stage 2: Final Runtime (최소 이미지) ============
FROM python:3.11-slim

# 필수 환경변수
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ACCEPT_EULA=Y

WORKDIR /app

# 1. ODBC Driver 18 설치 (Debian 13 호환: Debian 12 .deb 패키지 직접 다운로드 + 설치)
#    Microsoft 공식 문서 권장 방식 - 리포지토리 추가 없이 안전함
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        unixodbc && \
    # Debian 12용 Microsoft repo 구성 .deb 다운로드 및 설치
    curl -sSL -O https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb && \
    # 패키지 업데이트 및 ODBC Driver 18 + tools 설치
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        mssql-tools18 && \
    # mssql-tools (sqlcmd 등) PATH 추가
    echo '/opt/mssql-tools18/bin' >> /etc/profile.d/mssql-tools.sh && \
    # 완전 정리 (이미지 크기 최소화)
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
           packages-microsoft-prod.deb

# 2. Builder에서 Python 패키지 복사 (--user 설치로 root/.local 사용)
COPY --from=builder /root/.local /root/.local

# 3. 애플리케이션 코드 복사
COPY --from=builder /app /app

# Python PATH 추가
ENV PATH=/root/.local/bin:$PATH

EXPOSE 5000

# 건강 체크 (pyodbc 임포트 테스트)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys; import pyodbc; sys.exit(0)" || exit 1

CMD ["python", "app.py"]
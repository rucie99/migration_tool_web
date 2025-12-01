# ============ Stage 1: Builder ============
# 중요: Debian 12(slim) 대신 Debian 11(slim-bullseye) 사용
# 이유: OpenSSL 1.1.1 기반으로 ODBC 17 및 구형 SQL Server와의 호환성 확보
FROM python:3.11-slim-bullseye AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /app

# 시스템 의존성 (pyodbc 등 C 확장 빌드용)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        gnupg \
        unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

# 파이썬 의존성 설치
COPY requirements.txt .
RUN pip install --user -r requirements.txt

# 소스 복사
COPY . .

# ============ Stage 2: Final Runtime (ODBC 17 적용) ============
FROM python:3.11-slim-bullseye

# 필수 환경변수
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ACCEPT_EULA=Y

WORKDIR /app

# 1. ODBC Driver 17 설치 및 OpenSSL 보안 설정 완화
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        gnupg2 \
        ca-certificates \
        unixodbc \
        openssl && \
    # Microsoft GPG 키 및 Debian 11용 Repo 등록 (ODBC 17용)
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    # 패키지 업데이트 및 설치
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql17 \
        mssql-tools && \
    # mssql-tools PATH 추가 (v17은 mssql-tools18이 아님)
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc && \
    # ----------------------------------------------------------------------
    # [핵심] OpenSSL 보안 레벨 다운그레이드 (구형 DB 접속 문제 해결)
    # MinProtocol을 TLSv1.0으로 낮추고, SECLEVEL을 1로 변경하여 레거시 연결 허용
    # ----------------------------------------------------------------------
    sed -i 's/MinProtocol = TLSv1.2/MinProtocol = TLSv1.0/g' /etc/ssl/openssl.cnf || true && \
    sed -i 's/DEFAULT@SECLEVEL=2/DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf || true && \
    # 정리
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# 2. Builder에서 Python 패키지 복사
COPY --from=builder /root/.local /root/.local

# 3. 애플리케이션 코드 복사
COPY --from=builder /app /app

# Python PATH 및 MSSQL Tools PATH 추가
ENV PATH=/root/.local/bin:/opt/mssql-tools/bin:$PATH

CMD ["python", "app.py"]
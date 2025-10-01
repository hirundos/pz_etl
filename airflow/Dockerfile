# =================================================================
# Base Stage - 공통 기반 이미지 정의 (Spark 3.5.0으로 통일)
# =================================================================
FROM gcr.io/spark-operator/spark-py:v3.5.0 as base
USER root
WORKDIR /opt/spark
# jars와 applications 디렉토리를 미리 생성합니다.
RUN mkdir -p /opt/spark/jars && mkdir -p /opt/spark/applications

# =================================================================
# Bronze Stage - 'bronze' 이미지 빌드
# =================================================================
FROM base as bronze
# bronze용 로컬 JAR 파일을 복사합니다.
COPY postgresql-42.2.18.jar ./jars/
COPY gcs-connector-hadoop3-2.2.11.jar ./jars/
# bronze 스크립트를 기존 Dockerfile과 동일한 경로에 복사합니다.
COPY bronze.py ./jobs/
# 컨테이너 실행 유저를 non-root로 변경합니다.
USER 185

# =================================================================
# Silver Stage - 'silver' 이미지 빌드
# =================================================================
FROM base as silver
# Python 의존성을 설치합니다.
COPY requirements-silver.txt .
RUN pip install --no-cache-dir -r requirements-silver.txt
# GCS Connector를 다운로드합니다.
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -o ./jars/gcs-connector-hadoop3-latest.jar
# silver 스크립트를 복사합니다.
COPY silver.py ./applications/
USER 185

# =================================================================
# Gold Stage - 'gold' 이미지 빌드
# =================================================================
FROM base as gold
# Python 의존성을 설치합니다.
COPY requirements-gold.txt .
RUN pip install --no-cache-dir -r requirements-gold.txt
# GCS 및 BigQuery Connector를 다운로드합니다.
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -o ./jars/gcs-connector-hadoop3-latest.jar && \
    curl -L https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.38.0/spark-bigquery-with-dependencies_2.12-0.38.0.jar \
    -o ./jars/spark-bigquery-with-dependencies_2.12-0.38.0.jar
# gold 스크립트를 복사합니다.
COPY gold.py ./applications/
USER 185

# =================================================================
# Base Stage - 공통 기반 이미지 정의 (Spark 3.5.6 공식 이미지)
# =================================================================
FROM spark:3.5.6-scala2.12-java11-ubuntu as base

USER root
WORKDIR /opt/spark/work-dir

# jars, applications, jobs 디렉토리 생성
RUN apt-get update && apt-get install -y python3 python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/spark/work-dir/jars \
    && mkdir -p /opt/spark/work-dir/applications \
    && mkdir -p /opt/spark/work-dir/jobs


# =================================================================
# Bronze Stage - 'bronze' 이미지 빌드
# =================================================================
FROM base as bronze
# 필요한 JAR 파일 복사
COPY postgresql-42.2.18.jar ./jars/
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -o ./jars/gcs-connector-hadoop3-latest.jar
# bronze 스크립트 복사
COPY bronze.py ./jobs/
# 실행 유저는 spark
USER spark


# =================================================================
# Silver Stage - 'silver' 이미지 빌드
# =================================================================
FROM base as silver
# Python 의존성 설치 (root 권한에서 실행)
COPY requirements-silver.txt .
RUN pip install --no-cache-dir -r requirements-silver.txt
# 필요한 JAR 복사
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -o ./jars/gcs-connector-hadoop3-latest.jar
# silver 스크립트 복사
COPY silver.py ./applications/
USER spark


# =================================================================
# Gold Stage - 'gold' 이미지 빌드
# =================================================================
FROM base as gold
# Python 의존성 설치 (root 권한에서 실행)
COPY requirements-gold.txt .
RUN pip install --no-cache-dir -r requirements-gold.txt
# 필요한 JAR 복사
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -o ./jars/gcs-connector-hadoop3-latest.jar && \
    curl -L https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.38.0/spark-bigquery-with-dependencies_2.12-0.38.0.jar \
    -o ./jars/spark-bigquery-with-dependencies_2.12-0.38.0.jar
# gold 스크립트 복사
COPY gold.py ./applications/
USER spark

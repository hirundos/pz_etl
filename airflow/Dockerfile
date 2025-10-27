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
    && mkdir -p /opt/spark/work-dir/applications

COPY postgresql-42.2.18.jar ./jars/
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -o ./jars/gcs-connector-hadoop3-latest.jar  && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    -o ./jars/delta-spark_2.12-3.2.1.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
    -o ./jars/delta-storage-3.2.1.jar && \
    curl -L https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.38.0/spark-bigquery-with-dependencies_2.12-0.38.0.jar \
    -o ./jars/spark-bigquery-with-dependencies_2.12-0.38.0.jar

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bronze.py bronze_validator.py ./applications/
COPY silver.py silver_validator.py ./applications/
COPY gold.py gold_validator.py ./applications/

USER spark    

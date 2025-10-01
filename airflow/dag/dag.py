from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="spark_etl_pipeline_k8s",
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    catchup=False,
) as dag:

    bronze = KubernetesPodOperator(
        task_id="spark_bronze",
        name="spark-bronze",
        namespace="airflow",  # airflow가 올라간 namespace
        image="bitnami/kubectl:latest",  # kubectl이 있는 가벼운 이미지
        cmds=["sh", "-c"],
        arguments=["kubectl apply -f /opt/airflow/dags/spark-apps/bronze.yaml"],
        get_logs=True,
    )

    silver = KubernetesPodOperator(
        task_id="spark_silver",
        name="spark-silver",
        namespace="airflow",
        image="bitnami/kubectl:latest",
        cmds=["sh", "-c"],
        arguments=["kubectl apply -f /opt/airflow/dags/spark-apps/silver.yaml"],
        get_logs=True,
    )

    gold = KubernetesPodOperator(
        task_id="spark_gold",
        name="spark-gold",
        namespace="airflow",
        image="bitnami/kubectl:latest",
        cmds=["sh", "-c"],
        arguments=["kubectl apply -f /opt/airflow/dags/spark-apps/gold.yaml"],
        get_logs=True,
    )

    bronze >> silver >> gold

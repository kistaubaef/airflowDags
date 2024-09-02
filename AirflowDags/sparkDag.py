from datetime import datetime
from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

with DAG(
    dag_id="spark_kubernetes_dag_2",
    schedule=None,
    start_date=datetime(2024, 9, 1),
    catchup=False
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit",
        namespace="default",
        application_file="spark-deployment.yaml",
        do_xcom_push=True,
        params={"app_name": "spark-datamart-k8s-from-airflow-v2"} 
    )

    submit_sensor = SparkKubernetesSensor(
        task_id="submit_sensor",
        namespace="default",
        application_name="{{ task_instance.xcom_pull(task_ids='submit')['metadata']['name'] }}",
        attach_log=True,
    )

    submit >> submit_sensor


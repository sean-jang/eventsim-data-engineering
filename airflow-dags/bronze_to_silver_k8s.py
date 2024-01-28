from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

with DAG (dag_id='SparkKubernetesOperator',
          description='bronze to silver spark kubernetes operator',
          schedule_interval='@once',
          start_date=datetime(2024, 1, 1),
          catchup=False) as dag:

    submit = SparkKubernetesOperator(
            task_id='bronze_to_silver',
            namespace='spark-k8s-operator',
            application_file='k8s.yaml',
            kubernetes_conn_id='eventsim-eks',
            do_xcom_push=True
            )

    submit
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="sparkdag",
    start_date=datetime(2026,3,1),
    schedule="@daily",   # ✅ correct for Airflow 3
    catchup=False,
    tags=["pyspark","mysql","mongodb"]
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="run_policy_migration",
        application="/Users/rahul/Documents/python/Data_Engineer/src/spark/spark_transform.py",
        conn_id="spark_default",
        verbose=True
    )

    spark_job
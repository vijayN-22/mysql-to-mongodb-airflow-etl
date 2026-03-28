from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

SPARK_SCRIPT = "/Users/rahul/Documents/python/Data_Engineer/src/spark/spark_transform.py"

with DAG(
    dag_id="policy_migration_pipeline",
    start_date=datetime(2026,3,21),
    schedule="@daily",
    catchup=False,
    tags=["pyspark","mysql","mongodb"]
) as dag:
#ashfjkdsfhkskfjdsf
    run_policy_pipeline = SparkSubmitOperator(
        task_id="run_policy_migration",
        application=SPARK_SCRIPT,
        packages="org.mongodb.spark:mongo-spark-connector_2.13:10.3.0,mysql:mysql-connector-java:8.0.33",
        conn_id="spark_default",
        env_vars={
        "PYTHONPATH": "/Users/rahul/Documents/python/Data_Engineer"
    },
        conf={
        "spark.driverEnv.PYTHONPATH": "/Users/rahul/Documents/python/Data_Engineer",
        "spark.executorEnv.PYTHONPATH": "/Users/rahul/Documents/python/Data_Engineer"
    },
        verbose=True
    )

    run_policy_pipeline
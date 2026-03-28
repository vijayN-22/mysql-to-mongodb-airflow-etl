from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

BASE_DIR = "/Users/rahul/Documents/python/Data_Engineer"
sys.path.append(BASE_DIR)
# Import your existing functions
from src.spark.spark_transform import (
    create_spark_session,
    read_from_mysql,
    transform,
    write_to_mongo
)

# Temporary storage paths
EXTRACT_PATH = "/tmp/extract_data"
TRANSFORM_PATH = "/tmp/transformed_data"


# -------------------------
# TASK 1: EXTRACT
# -------------------------
def extract_task():
    spark = create_spark_session()
    df = read_from_mysql(spark)

    df.write.mode("overwrite").parquet(EXTRACT_PATH)

    print("✅ Extract completed")
    spark.stop()


# -------------------------
# TASK 2: TRANSFORM
# -------------------------
def transform_task():
    spark = create_spark_session()

    df = spark.read.parquet(EXTRACT_PATH)
    transformed_df = transform(df)

    transformed_df.write.mode("overwrite").parquet(TRANSFORM_PATH)

    print("✅ Transform completed")
    spark.stop()


# -------------------------
# TASK 3: LOAD
# -------------------------
def load_task():
    spark = create_spark_session()

    df = spark.read.parquet(TRANSFORM_PATH)
    write_to_mongo(df)

    print("✅ Load completed")
    spark.stop()


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="spark_migration_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["pyspark", "mysql", "mongodb"]
) as dag:

    extract = PythonOperator(
        task_id="extract_mysql",
        python_callable=extract_task
    )

    transform_op = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id="load_to_mongo",
        python_callable=load_task
    )

    # FLOW
    extract >> transform_op >> load
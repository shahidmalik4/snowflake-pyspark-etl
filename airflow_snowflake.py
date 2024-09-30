from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG, set schedule_interval to None for manual trigger
dag = DAG(
    dag_id='snowflake_pyspark_airflow',
    default_args=default_args,
    description='Snowflake and PySpark ETL Pipeline',
    schedule_interval=None,  # No automatic scheduling, only manual
    catchup=False,           # Don't run missed DAG runs
)

# Define the SparkSubmitOperator
spark_submit_task = SparkSubmitOperator(
    task_id='spark_snowflake_submit_job',
    application='/home/cipher/pyspark_files/pyspark_snowflake.py',
    conn_id='spark_default',
    executor_cores=2,
    executor_memory='2g',
    jars='/home/cipher/pyspark_files/postgresql-42.7.3.jar,/home/cipher/pyspark_files/snowflake-jdbc-3.13.14.jar,/home/cipher/pyspark_files/spark-snowflake_2.12-2.10.0-spark_3.0.jar',
    total_executor_cores=2,
    name='SNOWFLAKE_PYSARK_PROJECT',
    verbose=True,
    conf={'spark.master': 'local[*]'},
    dag=dag
)
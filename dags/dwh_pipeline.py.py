from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

@dag(
    dag_id="dwh_pipeline",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["dwh", "spark"],
)
def dwh_pipeline():

    run_spark = BashOperator(
        task_id="run_spark_pipeline",
        bash_command="""
            /mnt/c/Users/yazan/Downloads/spark-4.0.0-bin-hadoop3/spark-4.0.0-bin-hadoop3/bin/spark-submit \
                --driver-memory 16g \
                --conf spark.eventLog.enabled=false \
                --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.hadoop:hadoop-aws:3.4.1 \
                /home/yazan/airflow/scripts/DWH_Pipeline.py
        """,
    )

    trigger_monitoring = TriggerDagRunOperator(
        task_id="trigger_monitoring",
        trigger_dag_id="monitoring_dag",
        wait_for_completion=False,
    )

    run_spark >> trigger_monitoring

dwh_pipeline()
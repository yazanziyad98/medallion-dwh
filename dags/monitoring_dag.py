import pendulum
from airflow.sdk import dag, task
import sys

sys.path.insert(0, "/home/yazan/airflow")
from scripts.monitor_gold_layer import monitor_gold_bucket

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["monitoring", "gold_layer"]
)
def monitoring_dag():

    @task
    def monitor():
        monitor_gold_bucket()

    monitor()

monitoring_dag()
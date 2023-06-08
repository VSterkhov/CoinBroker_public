from __future__ import print_function
import logging
import airflow

from airflow import DAG
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.exceptions import AirflowTaskTimeout

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}

dag = DAG(
    dag_id="spark_kafka_consumer",
    schedule_interval="*/5 * * * *",
    default_args=args,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1),
    catchup=False
)

class SparkWithTimeout(SparkSubmitOperator):
    def execute(self, context):
        try:
            super().execute(context)
        except AirflowTaskTimeout:
            logging.info('Shutdown the task after the specified time - {execution_timeout}'.format(execution_timeout=self.execution_timeout))
            self.on_kill()
            "return code {0}"

spark_task = SparkWithTimeout(
    task_id="spark_task",
    name="Spark Kafka Consumer",
    application="/home/toor/airflow/jars/spark_kafka_consumer.jar",
    java_class='ru.broom.spark_kafka_consumer.Main',
    execution_timeout = timedelta(minutes=120),
    retries=-1,
    dag=dag
)

hql_script = """
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET dfs.block.size=33554432;
Insert overwrite table coin_broker.trade_volume partition(current_day, currency) 
select * from coin_broker.trade_volume;
"""

merge_trade_volume = HiveOperator(
    task_id='merge_trade_volume',
    hql=hql_script,
    hive_cli_conn_id='hiveserver2_default',
    retries=-1,
    dag=dag
)
spark_task >> merge_trade_volume
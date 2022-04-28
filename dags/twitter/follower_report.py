from functools import partial
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from dags.twitter.utils import update_tardis_status, create_databricks_connection
from plugins.config import AppConfig, TwitterConfig
from plugins.utilities.clusters.twitter import get_twitter_silver_cluster_config, get_twitter_silver_lib
from plugins.utilities.slack_service import success_alert, failure_alert

# ======================================
# CONFIGURATION
# ======================================

default_args = {'owner': 'us_rreddyb',
                'depends_on_past': False,
                'start_date': datetime(2021, 10, 19),
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5)
                }

log_date_time = datetime.now()
log_date = log_date_time.strftime("%Y-%m-%d")
# ======================================
# DAG and TASK DECLARATION
# ======================================

with DAG("twitter_followers_report",
         schedule_interval='00 07 * * *',
         catchup=False,
         default_args=default_args,
         tags=['twitter'],
         max_active_runs=1,
         concurrency=10,
         on_success_callback=success_alert
         ) as dag:
    # Dummy start task
    start_task = DummyOperator(
        task_id='start_task')

    # Dummy end task
    end_task = DummyOperator(
        task_id='end_task',
        trigger_rule='all_success')

    # Python operator to create databricks connection
    create_conn = PythonOperator(
        task_id='SetConnection',
        python_callable=create_databricks_connection)

    # Trigger the databricks notebook for silver processing
    silver_data_load = DatabricksSubmitRunOperator(
        task_id='Twitter_Silver_Process',
        databricks_conn_id='databricks_default',
        json={
            'notebook_task': {
                'notebook_path': TwitterConfig.twitter_notebook_path,
                'base_parameters': {
                    'env': AppConfig.environment,
                    'trigger_date': '{{ds}}'
                }
            },
            'new_cluster': get_twitter_silver_cluster_config(AppConfig.environment),
            'libraries': get_twitter_silver_lib()
        },
        on_failure_callback=failure_alert,
        on_success_callback=partial(update_tardis_status,
                                    'Data',
                                    'twitter.followers_report',
                                    log_date,
                                    'Data Complete',
                                    'Silver processing completed'),
        task_concurrency=1
    )

    start_task >> create_conn >> silver_data_load >> end_task

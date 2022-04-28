from functools import partial
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from plugins.utilities.clusters.google_analytics import get_ga360_api_report_cluster
from plugins.config import AppConfig, GA360apiReportConfig
from plugins.utilities.databricks.DatabricksUtils import create_databricks_connection
from plugins.utilities.tardis_sensor import TardisDataStatusSensor
from dags.google_analytics.functions.ga360_api_report_utils import success_alert, failure_alert, update_tardis_status


# ======================================
# CONFIGURATION
# ======================================

cluster, lib = get_ga360_api_report_cluster(AppConfig.environment)
notebook_path = GA360apiReportConfig.silver_notebook
connectors = GA360apiReportConfig.ga360_connectors

default_args = {'owner': 'us_achoudha',
                'depends_on_past': False,
                'start_date': datetime(2022, 3, 18),
                'email_on_failure': False,
                'email_on_retry': False,
                }

# ======================================
# DAG and TASK DECLARATION
# ======================================

with DAG("GA360_api_report_silver_DAG",
         schedule_interval='30 12 * * *',
         catchup=False,
         default_args=default_args,
         tags=['Fivetran', 'google', 'silver'],
         on_success_callback=success_alert,
         max_active_runs=1,
         concurrency=19
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
        task_id='set_connection',
        python_callable=create_databricks_connection)

    # Iterate over the connector list
    for connector in connectors:
        tardis_poller = TardisDataStatusSensor(
            task_id=f'tardis_poller_{connector}',
            sources=connector,
            start_logdate='{{ next_ds }}',
            status='Data Staged',
            poke_interval=300,
            timeout=3600,
            on_failure_callback=failure_alert
        )

        # Trigger the databricks notebook for silver processing
        silver_data_load = DatabricksSubmitRunOperator(
            task_id=f'silver_processing_{connector}',
            json={
                'notebook_task': {
                    'notebook_path': notebook_path,
                    'base_parameters': {
                        'connector': connector,
                        'load_date': '{{ next_ds }}',
                        'env': AppConfig.environment
                    }
                },
                'new_cluster': cluster,
                'libraries': lib
            },
            on_failure_callback=failure_alert,
            on_success_callback=partial(update_tardis_status,
                                        'Data',
                                        connector,
                                        'next_ds',
                                        'Data Complete',
                                        'Silver processing completed'),
            task_concurrency=1
        )

        start_task >> create_conn >> tardis_poller >> silver_data_load >> end_task

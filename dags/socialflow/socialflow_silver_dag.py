""" This DAG is responsible to Trigger and Load Socialflow Silver Data into Databricks Silver Table.
Task Flow:-

Start --> Check Tardis Connection --> Trigger DataBricks Notebook --> End

"""
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime
from plugins.config import AppConfig, SocialflowConfig
from plugins.utilities.clusters.socialflow import get_socialflow_cluster_config, get_socialflow_lib
from dags.socialflow.utilities import update_tardis_status, success_alert, failure_alert
from functools import partial
from plugins.utilities.tardis_sensor import TardisDataStatusSensor


SOCIALFLOW_NOTEBOOK_PATH = SocialflowConfig.socialflow_silver_notebook

if AppConfig.environment == 'production':
    env = 'prod'
else:
    env = AppConfig.environment

default_args = {
    'owner': 'us_ochoudha',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    }

# ======================================
# DAG and TASK DECLARATION
# ======================================

with DAG("Socialflow_DAG",
         schedule_interval='0 11 * * *',
         catchup=False,
         default_args=default_args,
         on_success_callback=success_alert,
         tags=['custom_connector', 'socialflow'],
         max_active_runs=1
         ) as dag:
    # Dummy Tasks for defining Start and End of all tasks

    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(
        task_id='end_task',
        trigger_rule='all_success')

    # loop through connectors list to create dynamic tasks
    for connector in SocialflowConfig.socialflow_connector_list:
        # Checking for Tardis Status "Data Staged" for source
        check_tardis_complete_status = TardisDataStatusSensor(
            task_id='check_tardis_complete_status_for_{}'.format(connector),
            sources=connector,
            start_logdate='{{ next_ds }}',
            status='Data Staged',
            poke_interval=300,
            timeout=3600,
            on_failure_callback=failure_alert
        )

        # Triggering the Databricks notebook to load silver data and updating Tardis.
        silver_load = DatabricksSubmitRunOperator(task_id="SocialFlow_Silver_Trigger_for_{}".format(connector),
                                                  json={
                                                    'libraries': get_socialflow_lib(),
                                                    'notebook_task': {
                                                        'notebook_path': SOCIALFLOW_NOTEBOOK_PATH,
                                                        'base_parameters': {
                                                            'load_date': '{{ next_ds }}',
                                                            'env': env,
                                                            'connector': connector,
                                                        }
                                                    },
                                                    'new_cluster': get_socialflow_cluster_config(AppConfig.environment)
                                                },
                                                on_failure_callback=failure_alert,
                                                on_success_callback=partial(update_tardis_status,
                                                                            'Data',
                                                                            connector,
                                                                            'next_ds',
                                                                            'Data Complete',
                                                                            'Silver processing completed'),
                                                task_concurrency=1)

        # defining Task Flow
        start_task >> check_tardis_complete_status >> silver_load >> end_task

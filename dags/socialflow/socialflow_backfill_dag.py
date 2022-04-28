""" This DAG is responsible to Trigger and Load Socialflow Silver Data into Databricks Silver Table.
Task Flow:-

Start --> Trigger DataBricks Notebook --> End

"""
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime
from plugins.config import AppConfig, SocialflowConfig
from plugins.utilities.clusters.socialflow import get_socialflow_cluster_config, get_socialflow_backfill_lib

SOCIALFLOW_BACKFILL_NOTEBOOK_PATH = SocialflowConfig.socialflow_backfill_notebook


default_args = {'owner': 'us_ochoudha',
                'depends_on_past': False,
                'start_date': datetime(2020, 10, 16),
                'email_on_failure': False,
                'email_on_retry': False,
                }

if AppConfig.environment == 'production':
    env = 'prod'
else:
    env = AppConfig.environment

# ======================================
# DAG and TASK DECLARATION
# ======================================

with DAG("Socialflow_Backfill_DAG",
         schedule_interval=None,
         catchup=False,
         default_args=default_args,
         tags=['custom_connector'],
         max_active_runs=1
         ) as dag:
    # Dummy Tasks for defining Start and End of all tasks

    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(
        task_id='end_task',
        trigger_rule='all_success')

    notebook_params = {
        'start_date': "{{ dag_run.conf['start_date'] }}",
        'end_date': "{{ dag_run.conf['end_date'] }}",
        'account_list': "{{ dag_run.conf['account_list'] }}",
        'brand_list': "{{ dag_run.conf['brand_list'] }}",
        'env': env
    }

    # Triggering the Databricks notebook to load data.
    silver_load = DatabricksSubmitRunOperator(task_id="SocialFlow_Silver_Backfill",
                                              databricks_conn_id="databricks_default",
                                              libraries=get_socialflow_backfill_lib(),
                                              json={
                                                'notebook_task': {
                                                    'notebook_path': SOCIALFLOW_BACKFILL_NOTEBOOK_PATH,
                                                    'base_parameters': notebook_params
                                                },
                                                'new_cluster': get_socialflow_cluster_config(AppConfig.environment)
                                                },
                                              task_concurrency=1)

    # defining Task Flow
    start_task >> silver_load >> end_task

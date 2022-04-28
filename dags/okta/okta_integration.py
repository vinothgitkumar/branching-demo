from airflow import DAG
from functools import partial
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from plugins.utilities.clusters.okta import get_okta_silver_cluster_config, get_okta_silver_lib
from plugins.utilities.common.dag_utilities import get_default_args
from plugins.config import AppConfig, OktaConfig
from plugins.utilities.slack_service import success_alert, failure_alert
from dags.okta.utilities import update_tardis_status, create_databricks_connection

notebook_task_params = {
    'notebook_task': {
        'notebook_path': OktaConfig.okta_notebook_path,
        'base_parameters': {
            'env': AppConfig.environment,
            'trigger_date': '{{ds}}'
        }
    },
    'new_cluster': get_okta_silver_cluster_config(AppConfig.environment),
    'libraries': get_okta_silver_lib()
}

log_date_time = datetime.now()
log_date = log_date_time.strftime("%Y-%m-%d")

# ======================================
# TASKS
# ======================================


# Run Notebook Using Databricks API

with DAG("OKTA_DATA_INTEGRATION",
         schedule_interval='00 07 * * *',
         catchup=False,
         default_args=get_default_args('us_rreddyb'),
         tags=['okta'],
         max_active_runs=1,
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
        python_callable=create_databricks_connection
    )

    # Trigger the databricks notebook for silver processing
    silver_data_load = DatabricksSubmitRunOperator(
        task_id='Okta_Silver_Process',
        databricks_conn_id='databricks_default',
        json=notebook_task_params,
        on_failure_callback=failure_alert,
        on_success_callback=partial(update_tardis_status,
                                    'Data',
                                    OktaConfig.tardis_data_source,
                                    log_date,
                                    'Data Complete',
                                    'Silver processing completed'),
        task_concurrency=1
    )
    start_task >> create_conn >> silver_data_load >> end_task

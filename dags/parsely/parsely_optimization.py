from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from plugins.config import ParselyConfig, AppConfig
from plugins.utilities.clusters.parsely import get_silver_configuration, get_parsely_lib
from plugins.utilities.slack_service import success_alert, failure_alert

default_args = {
    'owner': 'us_mzhan',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

notebook_task_params = {
    'notebook_task': {
        'notebook_path': ParselyConfig.parsely_optimization_notebook,
        'base_parameters': {
            'start_date': "{{dag_run.conf['start_date']}}",
            'end_date': "{{dag_run.conf['end_date']}}",
            'env': AppConfig.environment
        }
    },
    'new_cluster': get_silver_configuration(instance_profile=ParselyConfig.instance_profile),
    'libraries': get_parsely_lib(),
}

with DAG(
        dag_id='PARSELY_OPTIMIZATION',
        default_args=default_args,
        description='Parsley Silver Layer Optimization',
        schedule_interval='@daily',
        max_active_runs=1,
        catchup=False
) as dag:
    task = DatabricksSubmitRunOperator(
        task_id='optimize_parsely',
        dag=dag,
        json=notebook_task_params,
        on_failure_callback=failure_alert,
        on_success_callback=success_alert
    )

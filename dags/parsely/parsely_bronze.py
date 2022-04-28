from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from plugins.config import ParselyConfig, AppConfig
from plugins.utilities.clusters.parsely import get_bronze_configuration, get_parsely_lib
from plugins.utilities.vault import vault_instance
from plugins.utilities.slack_service import success_alert, failure_alert

parsely_aws_key = vault_instance.get_secret("parsely_aws_key")
parsely_aws_secret = vault_instance.get_secret("parsely_aws_secret")

default_args = {
    'owner': 'us_mzhan',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

notebook_task_params = {
    'notebook_task': {
        'notebook_path': ParselyConfig.parsely_bronze_notebook,
        'base_parameters': {
            'parsely_key': parsely_aws_key,
            'parsely_secret': parsely_aws_secret,
            'env': AppConfig.environment,
            'backfill_flag': "{{dag_run.conf['backfill_flag']}}",
            'backfill_start_date': '2020/12/24',
            'backfill_end_date': '2021/09/30'
        }
    },
    'new_cluster': get_bronze_configuration(instance_profile=ParselyConfig.instance_profile),
    'libraries': get_parsely_lib(),
}

if notebook_task_params['notebook_task']['base_parameters']['backfill_flag'] is True:
    schedule = '@once'
else:
    schedule = '@hourly'

with DAG(
        dag_id='PARSELY_BRONZE',
        default_args=default_args,
        description='Hourly Parsley Bronze Data Ingestion',
        schedule_interval=schedule,
        max_active_runs=1,
        catchup=False
) as dag:
    task = DatabricksSubmitRunOperator(
        task_id='parsely_bronze',
        dag=dag,
        json=notebook_task_params,
        on_failure_callback=failure_alert,
        on_success_callback=success_alert
    )

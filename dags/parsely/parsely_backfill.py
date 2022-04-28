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
    'start_date': datetime(2022, 2, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

notebook_task_params = {
    'notebook_task': {
        'notebook_path': ParselyConfig.parsely_backfill_notebook,
        'base_parameters': {
            'parsely_key': parsely_aws_key,
            'parsely_secret': parsely_aws_secret,
            'env': AppConfig.environment,
            'backfill_start_date': "{{dag_run.conf['backfill_start_date']}}",
            'backfill_end_date': "{{dag_run.conf['backfill_end_date']}}"
        }
    },
    'new_cluster': get_bronze_configuration(instance_profile=ParselyConfig.instance_profile),
    'libraries': get_parsely_lib(),
}


with DAG(
        dag_id='PARSELY_BRONZE_BACKFILL',
        default_args=default_args,
        description='Parsley Bronze Data Backfill',
        schedule_interval='@once',
        max_active_runs=1,
        catchup=False
) as dag:
    task = DatabricksSubmitRunOperator(
        task_id='parsely_bronze_backfill',
        dag=dag,
        json=notebook_task_params,
        on_failure_callback=failure_alert,
        on_success_callback=success_alert
    )

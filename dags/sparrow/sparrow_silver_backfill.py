from airflow import DAG
from datetime import datetime
from plugins.utilities.clusters.sparrow import get_sparrow_silver_backfill_cluster_config, get_sparrow_silver_lib
from plugins.config import AppConfig, SparrowConfig
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from plugins.utilities.common.dag_utilities import get_default_args
from plugins.utilities.slack_service import success_alert, failure_alert


SLACK_TITLE = "Sparrow Silver Backfill [Env: {}]".format(AppConfig.environment.capitalize())

# No schedule as it is a backfill job
dag = DAG(
    dag_id="SPARROW_SILVER_BACKFILL",
    default_args=get_default_args('us_ssekar2'),
    start_date=datetime(2021, 1, 1),
    max_active_runs=3,
    schedule_interval=None,
    catchup=False
)

# start_date and end_date in the format of %Y-%m-%d has to be passed in json config while triggering manual dag run
notebook_task_params = {
    'notebook_task': {
        'notebook_path': SparrowConfig.sparrow_silver_backfill_notebook,
        'base_parameters': {
            'env': AppConfig.environment,
            'start_date': "{{ dag_run.conf['start_date'] }}",
            'end_date': "{{ dag_run.conf['end_date'] }}"
        }
    },
    'new_cluster': get_sparrow_silver_backfill_cluster_config(AppConfig.environment),
    'libraries': get_sparrow_silver_lib(),
    'polling_period_seconds': 1800
}

databricks_task = DatabricksSubmitRunOperator(
    task_id='sparrow_silver_backfill',
    json=notebook_task_params,
    dag=dag,
    on_failure_callback=failure_alert,
    on_success_callback=success_alert
)

databricks_task

from airflow import DAG
from datetime import datetime
from plugins.utilities.clusters.sparrow import get_sparrow_silver_agg_cluster_config, get_sparrow_silver_lib
from plugins.config import AppConfig, SparrowConfig
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from plugins.utilities.common.dag_utilities import get_default_args
from plugins.utilities.slack_service import success_alert, failure_alert


SLACK_TITLE = "Sparrow Silver Hourly Agg [Env: {}]".format(AppConfig.environment.capitalize())

dag = DAG(
    dag_id="SPARROW_SILVER_HOURLY_AGG",
    default_args=get_default_args('us_ssekar2'),
    schedule_interval='@hourly',
    start_date=datetime(2021, 12, 7, 0, 0),
    max_active_runs=1,
    catchup=False
)

notebook_task_params = {
    'notebook_task': {
        'notebook_path': SparrowConfig.sparrow_silver_hourly_agg_notebook,
        'base_parameters': {
            'env': AppConfig.environment,
            'trigger_ts': '{{ts}}'
        }
    },
    'new_cluster': get_sparrow_silver_agg_cluster_config(AppConfig.environment),
    'libraries': get_sparrow_silver_lib()
}

databricks_task = DatabricksSubmitRunOperator(
    task_id='sparrow_silver_hourly_agg',
    json=notebook_task_params,
    dag=dag,
    on_failure_callback=failure_alert,
    on_success_callback=success_alert
)

databricks_task

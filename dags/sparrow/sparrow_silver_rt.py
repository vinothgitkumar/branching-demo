from airflow import DAG
from datetime import datetime
from plugins.utilities.clusters.sparrow import get_sparrow_silver_cluster_config, get_sparrow_silver_lib
from plugins.config import AppConfig, DatabricksConfig, SlackConfig, SparrowConfig
from plugins.utilities.databricks.AlertingDatabricksSubmitRunOperator import AlertingDatabricksSubmitRunOperator
from plugins.utilities.common.dag_utilities import get_default_args


SLACK_TITLE = "Sparrow Silver RT [Env: {}]".format(AppConfig.environment.capitalize())

dag = DAG(
    dag_id="SPARROW_SILVER_RT",
    default_args=get_default_args('us_ssekar2'),
    schedule_interval='@once',
    start_date=datetime(2021, 12, 7, 0, 0),
    max_active_runs=1,
    catchup=False
)


alert_config = {
    "channel": SlackConfig.alert_channel,
    "failure_channel": SlackConfig.failure_channel,
    "token": SlackConfig.slack_de_token,
    "alert_titles": [SLACK_TITLE + " Scheduler", SLACK_TITLE + " Poller"],
    # polling period for slack alert daemon
    "alert_polling_interval": 60 * 60,
    "owner": "@us_ssekar2",
    "databricks_workspace": DatabricksConfig.workspace_conn_id
}

notebook_task_params = {
    'notebook_task': {
        'notebook_path': SparrowConfig.sparrow_silver_rt_notebook,
        'base_parameters': {
            'env': AppConfig.environment,
            'trigger_date': '{{ds}}'
        }
    },
    'new_cluster': get_sparrow_silver_cluster_config(AppConfig.environment),
    'libraries': get_sparrow_silver_lib()
}

databricks_rt_task = AlertingDatabricksSubmitRunOperator(
    task_id='sparrow_silver_streaming',
    json=notebook_task_params,
    dag=dag,
    do_xcom_push=True,
    alert_config=alert_config,
    alert_polling_enabled=True,
    # polling period for super class daemon
    polling_period_seconds=60 * 1
)

databricks_rt_task

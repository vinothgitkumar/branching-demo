from airflow import DAG
from datetime import datetime, timedelta
from plugins.utilities.clusters.parsely import get_silver_configuration, get_parsely_lib
from plugins.config import ParselyConfig, AppConfig, DatabricksConfig, SlackConfig
from marshmellow.airflow.databricks import on_success_callback, \
    on_failure_callback, CondeDatabricksSubmitRunOperator
from functools import partial

SLACK_TITLE = "Parsely Silver [Environment: {}]".format(AppConfig.environment.capitalize())
DAG_ID = "Parsely Silver"

callback_config = {
    "slack_token": SlackConfig.slack_de_token,
    "slack_channel": SlackConfig.alert_channel,
    "slack_failure_channel": SlackConfig.failure_channel,
    "slack_title": SLACK_TITLE,
    "slack_alert_owner": "@us_mzhan",
    "databricks_workspace": DatabricksConfig.workspace_conn_id
}

default_args = {
    'owner': 'us_mzhan',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id="PARSELY_SILVER",
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(2022, 2, 16, 0, 0),
    max_active_runs=1,
    catchup=False,
    on_success_callback=partial(
        on_success_callback,
        SlackConfig.slack_de_token,
        SlackConfig.alert_channel,
        f"{DAG_ID} DAG Success",
        "@us_mzhan",
        DatabricksConfig.workspace_conn_id
    ),
    on_failure_callback=partial(
        on_failure_callback,
        SlackConfig.slack_de_token,
        SlackConfig.failure_channel,
        f"{DAG_ID} DAG Failure",
        "@us_mzhan",
        DatabricksConfig.workspace_conn_id
    )
)

notebook_task_params = {
    'notebook_task': {
        'notebook_path': ParselyConfig.parsely_silver_notebook,
        'base_parameters': {
            'env': AppConfig.environment
        }
    },
    'new_cluster': get_silver_configuration(instance_profile=ParselyConfig.instance_profile),
    'libraries': get_parsely_lib(),
}

databricks_run_task = CondeDatabricksSubmitRunOperator(
    task_id='parsely_silver_streaming',
    json=notebook_task_params,
    do_xcom_push=True,
    poll_mode=False,
    depends_on_past=True,
    xcoms_task_id='parsely_silver_streaming',
    databricks_conn_id=DatabricksConfig.workspace_conn_id,
    dag=dag
)

databricks_run_task

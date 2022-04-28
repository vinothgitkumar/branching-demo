from datetime import datetime, timedelta
from airflow import DAG
from plugins.utilities.clusters.skimlinks_affiliates import get_cluster_config, get_cluster_libs
from plugins.config import SkimlinksUKEmeaAffiliatesConfig as config, AppConfig, VaultConfig, SlackConfig, DatabricksConfig
from marshmellow.airflow.databricks import on_success_callback, \
    on_failure_callback, CondeDatabricksSubmitRunOperator
from functools import partial

SLACK_TITLE = "Skimlinks UK_EMEA Silver [Environment: {}]".format(AppConfig.environment.capitalize())
DAG_ID = "Skimlinks Affiliate UK_Emea Silver"

callback_config = {
    "slack_token": SlackConfig.slack_de_token,
    "slack_channel": SlackConfig.alert_channel,
    "slack_failure_channel": SlackConfig.failure_channel,
    "slack_title": SLACK_TITLE,
    "slack_alert_owner": "@uk_obradley",
    "databricks_workspace": DatabricksConfig.workspace_conn_id
}

# ======================================
# CONFIGURATION
# ======================================

ENV = AppConfig.environment

notebook_task_params = {
    'notebook_task': {
        'notebook_path': config.daily_revenue_ingestion_notebook_name,
        'base_parameters': {
            'env': ENV,
            'token': VaultConfig.token,
            'run_date': '{{ dag_run.conf["run_date"] if dag_run.conf else ds }}'
        }
    },
    'new_cluster': get_cluster_config(instance_profile=config.instance_profile),
    'libraries': get_cluster_libs()
}


# # ======================================
# # DAG DECLARATION
# # ======================================

default_args = {
    'owner': 'uk_obradley',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}


dag = DAG(
    dag_id='skimlinks_affiliate_uk_emea_ingest',
    default_args=default_args,
    schedule_interval="0 9 * * *",  # 2:00 am PST / 9:00am UTC
    max_active_runs=1,
    catchup=False
)


success_callback = partial(
            on_success_callback,
            SlackConfig.slack_de_token,
            SlackConfig.alert_channel,
            f"{DAG_ID} DAG Success",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        )


failure_callback = partial(
            on_failure_callback,
            SlackConfig.slack_de_token,
            SlackConfig.failure_channel,
            f"{DAG_ID} DAG Failure",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        )

# ======================================
# TASKS
# ======================================
ingest_uk_emea_skimlinks_daily_revenue = CondeDatabricksSubmitRunOperator(
        task_id='ingest_uk_emea_skimlinks_daily_revenue',
        dag=dag,
        json=notebook_task_params,
        on_success_callback=success_callback,
        on_failure_callback=failure_callback
)

notebook_task_params['notebook_task']['notebook_path'] = config.commission_ingestion_notebook_name
ingest_uk_emea_skimlinks_commission = CondeDatabricksSubmitRunOperator(
        task_id='ingest_uk_emea_skimlinks_commission',
        dag=dag,
        json=notebook_task_params,
        on_success_callback=success_callback,
        on_failure_callback=failure_callback
)

ingest_uk_emea_skimlinks_daily_revenue
ingest_uk_emea_skimlinks_commission

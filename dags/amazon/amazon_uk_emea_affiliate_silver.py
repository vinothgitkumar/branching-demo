from datetime import datetime, timedelta
from airflow import DAG
from plugins.utilities.clusters.amazon_affiliates import get_cluster_config, get_cluster_libs
from plugins.config import AmazonUKEmeaAffiliatesConfig as config, AppConfig, VaultConfig, SlackConfig, DatabricksConfig
from marshmellow.airflow.databricks import on_success_callback, \
    on_failure_callback, CondeDatabricksSubmitRunOperator
from functools import partial

SLACK_TITLE = "Amazon UK_EMEA Silver [Environment: {}]".format(AppConfig.environment.capitalize())
DAG_ID = "Amazon Affiliate UK_Emea Silver"

callback_config = {
    "slack_token": SlackConfig.slack_de_token,
    "slack_channel": SlackConfig.alert_channel,
    "slack_failure_channel": SlackConfig.failure_channel,
    "slack_title": SLACK_TITLE,
    "slack_alert_owner": "@uk_obradley",
    "databricks_workspace": DatabricksConfig.workspace_conn_id
}

# ======================================
# CONFIGURATION testing7
# ======================================

ENV = AppConfig.environment

new_cluster = get_cluster_config(ENV)

notebook_task_params = {
    'notebook_task': {
        'notebook_path': config.daily_revenue_ingestion_notebook_name,
        'base_parameters': {
            'env': ENV,
            'token': VaultConfig.token
        }
    },
    'new_cluster': get_cluster_config(ENV),
    'libraries': get_cluster_libs()
}

# # ======================================
# # DAG DECLARATION
# # ======================================

default_args = {
    'owner': 'uk_obradley',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(hours=3),
}

dag = DAG(
    dag_id='amazon_affiliate_uk_emea_ingest',
    default_args=default_args,
    schedule_interval="0 9 * * *",  # 2:00 am PST / 9:00am UTC
    max_active_runs=10,
    catchup=False
)

# ======================================
# TASKS
# ======================================

notebook_task_params['notebook_task']['base_parameters']['run_date'] = '{{ yesterday_ds }}'
ingest_uk_emea_amazon_daily_revenue = CondeDatabricksSubmitRunOperator(
        task_id='ingest_uk_emea_amazon_daily_revenue',
        dag=dag,
        json=notebook_task_params,
        on_success_callback=partial(
            on_success_callback,
            SlackConfig.slack_de_token,
            SlackConfig.alert_channel,
            f"{DAG_ID} DAG Success",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        ),
        on_failure_callback=partial(
            on_failure_callback,
            SlackConfig.slack_de_token,
            SlackConfig.failure_channel,
            f"{DAG_ID} DAG Failure",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        )
)

notebook_task_params['notebook_task']['notebook_path'] = config.daily_clicks_ingestion_notebook_name
ingest_uk_emea_amazon_daily_clicks = CondeDatabricksSubmitRunOperator(
        task_id='ingest_uk_emea_amazon_daily_clicks',
        dag=dag,
        json=notebook_task_params,
        on_success_callback=partial(
            on_success_callback,
            SlackConfig.slack_de_token,
            SlackConfig.alert_channel,
            f"{DAG_ID} DAG Success",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        ),
        on_failure_callback=partial(
            on_failure_callback,
            SlackConfig.slack_de_token,
            SlackConfig.failure_channel,
            f"{DAG_ID} DAG Failure",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        )
)

notebook_task_params['notebook_task']['notebook_path'] = config.content_insights_ingestion_notebook_name
ingest_uk_emea_amazon_content_insights = CondeDatabricksSubmitRunOperator(
        task_id='ingest_uk_emea_amazon_content_insights',
        dag=dag,
        json=notebook_task_params,
        on_success_callback=partial(
            on_success_callback,
            SlackConfig.slack_de_token,
            SlackConfig.alert_channel,
            f"{DAG_ID} DAG Success",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        ),
        on_failure_callback=partial(
            on_failure_callback,
            SlackConfig.slack_de_token,
            SlackConfig.failure_channel,
            f"{DAG_ID} DAG Failure",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        )
)


notebook_task_params['notebook_task']['base_parameters']['run_date'] = '{{ macros.ds_add(yesterday_ds, -3) }}'
ingest_uk_emea_amazon_content_insights_three_days_ago = CondeDatabricksSubmitRunOperator(
        task_id='ingest_uk_emea_amazon_content_insights_three_days_ago',
        dag=dag,
        json=notebook_task_params,
        on_success_callback=partial(
            on_success_callback,
            SlackConfig.slack_de_token,
            SlackConfig.alert_channel,
            f"{DAG_ID} DAG Success",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        ),
        on_failure_callback=partial(
            on_failure_callback,
            SlackConfig.slack_de_token,
            SlackConfig.failure_channel,
            f"{DAG_ID} DAG Failure",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        )
)


notebook_task_params['notebook_task']['base_parameters']['run_date'] = '{{ yesterday_ds }}'
notebook_task_params['notebook_task']['notebook_path'] = config.missing_report_notebook_name
process_missing_reports = CondeDatabricksSubmitRunOperator(
        task_id='process_uk_emea_content_insights_missing_reports',
        dag=dag,
        json=notebook_task_params,
        on_success_callback=partial(
            on_success_callback,
            SlackConfig.slack_de_token,
            SlackConfig.alert_channel,
            f"{DAG_ID} DAG Success",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        ),
        on_failure_callback=partial(
            on_failure_callback,
            SlackConfig.slack_de_token,
            SlackConfig.failure_channel,
            f"{DAG_ID} DAG Failure",
            "@uk_obradley",
            DatabricksConfig.workspace_conn_id
        )
)


ingest_uk_emea_amazon_daily_revenue
ingest_uk_emea_amazon_content_insights >> ingest_uk_emea_amazon_content_insights_three_days_ago >> process_missing_reports

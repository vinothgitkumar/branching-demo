from datetime import timedelta, datetime
from functools import partial

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator

from dags.google_analytics.functions import base, bronze
from plugins.config import GoogleAnalyticsGlobal, AppConfig
from plugins.utilities.clusters.google_analytics import get_bronze_cluster_config

# ======================================
# CONFIGURATION
# ======================================


DAG_NAME = "ga_global_pubsub_listener"

GA_PROJECT_ID = "140208876"

SLACK_ALERT_CHANNEL = GoogleAnalyticsGlobal.alert_channel
SLACK_TOKEN = GoogleAnalyticsGlobal.slack_de_token

# ======================================
# DAG DECLARATION
# ======================================
default_args = {
    'owner': 'us_mpiruka',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'retries': 5,

}

dag = DAG(DAG_NAME,
          max_active_runs=1,
          schedule_interval="30 11 * * *",
          default_args=default_args,
          catchup=False,
          start_date=datetime(2021, 1, 1),
          tags=['google_analytics', 'bronze'],
          on_success_callback=partial(base.send_slack_alert,
                                      'Pubsub listener Dag success',
                                      'Success',
                                      SLACK_TOKEN,
                                      SLACK_ALERT_CHANNEL,
                                      None,
                                      None
                                      ),
          )

# ======================================
# TASKS
# ======================================
env = AppConfig.environment

databricks_cluster_ga, databricks_ga_cluster_libs = get_bronze_cluster_config(GoogleAnalyticsGlobal.gcp_bq_cred, env)

notebook_params = {
    "dataset": "{{ task_instance.xcom_pull(task_ids='pull_messages_ga', key='dataset') }}",
    "env": env
}

notebook_task_params = {
    'libraries': databricks_ga_cluster_libs,
    'notebook_task': {
        'notebook_path': GoogleAnalyticsGlobal.notebook_bronze,
        'base_parameters': notebook_params
    },
    'new_cluster': databricks_cluster_ga
}

log_start = PythonOperator(
    task_id="log_start",
    python_callable=bronze.check_update_tardis_start,
    op_kwargs={"TARDIS_SOURCE": GoogleAnalyticsGlobal.tardis_process_source},
    provide_context=True,
    on_failure_callback=partial(base.task_failure,
                                "GA Process Failed at PubSub Listener",
                                GoogleAnalyticsGlobal,
                                "Logging into Tardis as Started"
                                ),

    dag=dag
)

create_conn_db = PythonOperator(
    task_id="create_conn_db",
    python_callable=bronze.create_airflow_connections_ga,
    op_kwargs={
        'gcp_connection_id': GoogleAnalyticsGlobal.gcp_connection_id,
        'gcp_vault_key': GoogleAnalyticsGlobal.gcp_pubsub_cred
    },
    dag=dag
)

pull_messages_ga = PubSubPullSensor(
    gcp_conn_id=GoogleAnalyticsGlobal.gcp_connection_id,
    task_id="pull_messages_ga",
    ack_messages=True,
    poke_interval=300,
    project=GoogleAnalyticsGlobal.project_info_bq_project,
    subscription=GoogleAnalyticsGlobal.project_info_subscription,
    max_messages=1,
    retries=5,
    retry_delay=timedelta(minutes=15),
    on_success_callback=partial(bronze.decrypt_push_xcom,
                                GoogleAnalyticsGlobal,
                                DAG_NAME
                                ),
    on_failure_callback=partial(base.task_failure,
                                "GA Global Process Failed at PubSub Listener",
                                GoogleAnalyticsGlobal,
                                "PubSub Sensor"
                                ),
    on_retry_callback=partial(base.send_slack_alert,
                              "GA Global Process Retrying at PubSub Listener",
                              "Retrying",
                              SLACK_TOKEN,
                              SLACK_ALERT_CHANNEL,
                              GoogleAnalyticsGlobal.additional_alertee,
                              None
                              ),
    dag=dag

)

# Run Notebook Using Databricks API
bronze_data = DatabricksSubmitRunOperator(
    task_id="bronze_data",
    json=notebook_task_params,
    on_failure_callback=partial(base.task_failure,
                                "GA Global Process Failed at Bronze Data load",
                                GoogleAnalyticsGlobal,
                                "Bronze Databricks Run",
                                ),
    dag=dag
)

silver_dag = TriggerDagRunOperator(
    task_id="trigger_silver_dag",
    trigger_dag_id="ga_global_silver_dag",
    python_callable=bronze.get_run_conf,
    on_failure_callback=partial(base.task_failure,
                                "GA Global Process Failed at Trigger Silver Dag",
                                GoogleAnalyticsGlobal,
                                "Triggering Silver Dag",
                                ),
    dag=dag
)

log_start >> create_conn_db >> pull_messages_ga >> bronze_data >> silver_dag

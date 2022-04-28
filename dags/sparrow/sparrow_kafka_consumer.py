import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
from datetime import datetime
from plugins.utilities.airflow_connections import create_airflow_connection, list_connections
from airflow import settings
import json
from plugins.utilities.clusters.sparrow import get_sparrow_kafka_consumer_cluster_config
from plugins.utilities.artifacts.spark_artifacts.jars.enterprise_workstream_jars import SPARROW_KAFKA_CONSUMER_JAR
from plugins.utilities.databricks.AlertingDatabricksSubmitRunOperator import AlertingDatabricksSubmitRunOperator
from plugins.utilities.vault import vault_instance

ASTRONOMER_ENV = os.environ.get("ENV", "STAGING")
cluster_configuration = get_sparrow_kafka_consumer_cluster_config(env=ASTRONOMER_ENV)
streaming_job_task_id = 'SPARROW_STREAMING_TASK'
DAG_ID = "SPARROW_KAFKA_CONSUMER_DAG"
SLACK_TITLE = "Sparrow Kafka Consumer [Environment: {}]".format(ASTRONOMER_ENV.capitalize())

"""
ToDo Updating Vault location w/ Service Principals
"""
evergreen_staging_workspace_token = vault_instance.get_secret("marquis_pat_staging_evergreen_workspace")
evergreen_production_workspace_token = vault_instance.get_secret("marquis_pat_production_evergreen_workspace")

if ASTRONOMER_ENV.lower() == "staging":
    WORKSPACE_TOKEN = evergreen_staging_workspace_token
    WORKSPACE_HOST = "https://condenast-stg.cloud.databricks.com"
    WORKSPACE_CONN_ID = "databricks_stg_workspace"
    SLACK_ALERT_CHANNEL = "cm-alerts-test-an"
elif ASTRONOMER_ENV.lower() == "production":
    WORKSPACE_TOKEN = evergreen_production_workspace_token
    WORKSPACE_HOST = "https://condenast-prod.cloud.databricks.com"
    WORKSPACE_CONN_ID = "databricks_prod_workspace"
    SLACK_ALERT_CHANNEL = "de-alerts"


def my_callable():
    logger = logging.getLogger("airflow.task")
    TOKEN = WORKSPACE_TOKEN
    HOST = WORKSPACE_HOST
    CONN_ID = WORKSPACE_CONN_ID
    logger.info("Attempting to Create Airflow Connection")
    create_airflow_connection(conn_id=CONN_ID,
                              conn_type="databricks",
                              host=HOST,
                              login=None,
                              password=None,
                              port=None,
                              extra=json.dumps({"token": TOKEN, "host": HOST.split("//")[1]}),
                              uri=None
                              )

    for index, item in enumerate(list_connections(settings.Session)):
        logger.info("Connections Index: [{}] Item: [{}]".format(str(index), str(item)))


rt_dag = DAG(
    dag_id=DAG_ID,
    default_args={
        "owner": "mchamberlain"
    },
    schedule_interval='@once',
    start_date=datetime(2020, 12, 8, 0, 0),
    max_active_runs=1,
    catchup=False
)

SLACK_TOKEN = os.environ.get("SLACK_DE_TOKEN", None)

alert_config = {
    "channel": SLACK_ALERT_CHANNEL,
    "failure_channel": "de-alert-fails",
    "token": SLACK_TOKEN,
    "alert_titles": [SLACK_TITLE + " Scheduler", SLACK_TITLE + " Poller"],
    # polling period for slack alert daemon
    "alert_polling_interval": 60 * 60,
    "owner": "@us_mchamber",
    "databricks_workspace": WORKSPACE_CONN_ID
}

databricks_run_task = AlertingDatabricksSubmitRunOperator(
    task_id=streaming_job_task_id,
    new_cluster=cluster_configuration,
    spark_jar_task={'main_class_name': SPARROW_KAFKA_CONSUMER_JAR.main_class_path,
                    'parameters': SPARROW_KAFKA_CONSUMER_JAR.parameters.get(ASTRONOMER_ENV.lower())},
    dag=rt_dag,
    do_xcom_push=True,
    alert_config=alert_config,
    alert_polling_enabled=True,
    databricks_conn_id=WORKSPACE_CONN_ID,
    libraries=SPARROW_KAFKA_CONSUMER_JAR.libraries,
    # polling period for super class daemon
    polling_period_seconds=60 * 1
)

conn_ops = PythonOperator(
    task_id='SetConnections',
    dag=rt_dag,
    python_callable=my_callable,
)

conn_ops >> databricks_run_task

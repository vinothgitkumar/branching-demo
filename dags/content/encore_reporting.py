import json
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow import DAG
from airflow import AirflowException
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta
from plugins.utilities.clusters.encore import get_existing_dbt_cluster
from marshmellow.airflow.databricks import on_success_callback, \
    on_failure_callback, CondeDatabricksSubmitRunOperator
from plugins.utilities.dbt import DbtUtils
from plugins.config import AppConfig, DatabricksConfig, EncoreConfig, SlackConfig
from functools import partial

SLACK_TITLE = "Encore Reporting Data Silver + Gold [Env: {}]".format(AppConfig.environment.capitalize())
SLACK_OWNER = "@dinmk"

callback_config = {
    "slack_token": SlackConfig.slack_de_token,
    "slack_channel": SlackConfig.alert_channel,
    "slack_failure_channel": SlackConfig.failure_channel,
    "slack_title": SLACK_TITLE,
    "slack_alert_owner": SLACK_OWNER,
    "databricks_workspace": DatabricksConfig.workspace_conn_id
}

default_args = {
    'owner': 'dinmk',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def getDbtjobRunID(**kwargs):
    task_instance = kwargs['task_instance']
    response_xcom = task_instance.xcom_pull(dag_id="encore_reporting", task_ids="dbt_encore_run", key="return_value")
    json_xcom = json.loads(response_xcom)
    jobRunID = json_xcom["data"]["id"]
    return jobRunID


with DAG(
        dag_id='encore_reporting',
        default_args=default_args,
        description='Daily K2D ingest into Silver & Gold using Dbt ',
        catchup=False,
        schedule_interval='00 21 * * *',
        tags=['content', 'encore'],
        on_success_callback=partial(
            on_success_callback,
            SlackConfig.slack_de_token,
            SlackConfig.alert_channel,
            "Encore Reporting DAG Success",
            SLACK_OWNER,
            DatabricksConfig.workspace_conn_id
        ),
        on_failure_callback=partial(
            on_failure_callback,
            SlackConfig.slack_de_token,
            SlackConfig.failure_channel,
            "Encore Reporting DAG Failure",
            SLACK_OWNER,
            DatabricksConfig.workspace_conn_id
        )
) as dag:
    notebook_task_param = {
        'notebook_task': {
            'notebook_path': EncoreConfig.mongo_notebook,
            'base_parameters': {
                'env': AppConfig.environment,
            }
        },
        'existing_cluster_id': get_existing_dbt_cluster(AppConfig.environment.lower()),
    }

    ingest_mongoapi_task = CondeDatabricksSubmitRunOperator(
        task_id='ingest_mongoapi_task',
        json=notebook_task_param,
        databricks_conn_id=DatabricksConfig.workspace_conn_id
    )

    def check(response):
        if response.json()['status']['code'] == 200:
            print("Returning True")
            return True
        else:
            raise AirflowException("DbtCloud job failed !")

    kickoff_task = SimpleHttpOperator(
        task_id='dbt_encore_run',
        method='POST',
        data=json.dumps({
            "cause": "Triggered by Astronomer!",
            "threads_override": 16,
            "steps_override": ["dbt compile --select tag:encore --vars '{\"rep_date\": \"{{ds}}\"}'",
                               "dbt run --fail-fast --select tag:encore --vars '{\"rep_date\": \"{{ds}}\"}'",
                               "dbt test --fail-fast --select tag:encore --vars '{\"rep_date\": \"{{ds}}\"}'"]
        }),
        response_check=lambda response: check(response),
        http_conn_id='dbt_api',
        endpoint=DbtUtils.getDbtApiLink(DbtUtils.getDbtJobId('encore', AppConfig.environment.lower()),
                                        DbtUtils.getDbtAccntId()),
        headers={'Content-Type': 'application/json',
                 'Authorization': 'Token ' + DbtUtils.getDbtToken()},
        xcom_push=True,
        log_response=True
    )

    get_job_runid = PythonOperator(
        task_id='dbt_encore_jobrunID',
        python_callable=getDbtjobRunID,
        provide_context=True
    )

    def check_runjob(response):
        if response.json()['data']['status'] == 1:
            print("Returning False")
            return False
        elif response.json()['data']['status'] == 3:
            print("Returning False")
            return False
        elif response.json()['data']['status'] == 10:
            print("Returning True")
            return True
        else:
            raise AirflowException("DbtCloud job failed !")

    kickoff_task_check = HttpSensor(
        task_id='dbt_encore_run_check',
        http_conn_id='dbt_api',
        endpoint=DbtUtils.getDbtApiLinkforRun(
            DbtUtils.getDbtAccntId()) + "{{ ti.xcom_pull(key='return_value',task_ids='dbt_encore_jobrunID') }}",
        headers={'Content-Type': 'application/json',
                 'Authorization': 'Token ' + DbtUtils.getDbtToken()},
        log_response=True,
        response_check=lambda response: check_runjob(response),
        poke_interval=120,
        timeout=7200,
        xcom_push=False,
        provide_context=True
    )

    notebook_task_param = {
        'notebook_task': {
            'notebook_path': EncoreConfig.bq_notebook,
            'base_parameters': {
                'env': AppConfig.environment,
                'rep_date_no_ds': '{{ ds_nodash }}',
                'rep_date': '{{ ds }}',
            }
        },
        'existing_cluster_id': get_existing_dbt_cluster(AppConfig.environment.lower()),
    }
    bq_task = CondeDatabricksSubmitRunOperator(
        task_id='bq_export_task',
        json=notebook_task_param,
        databricks_conn_id=DatabricksConfig.workspace_conn_id
    )
    ingest_mongoapi_task >> kickoff_task >> get_job_runid >> kickoff_task_check >> bq_task
    # bq_task will be deactivated once the Dashboard/Reporting is ready in Evergreen

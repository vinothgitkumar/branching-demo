import json
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow import DAG
from airflow import AirflowException
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta
from plugins.utilities.dbt import DbtUtils
from plugins.config import AppConfig
from plugins.utilities.slack_service import success_alert, failure_alert
#testing5
SLACK_TITLE = "Copilot Data Silver + Gold [Env: {}]".format(AppConfig.environment.capitalize())
SLACK_OWNER = "@dinmk"

default_args = {
    'owner': 'dinmk',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def getDbtjobRunID(**kwargs):
    task_instance = kwargs['task_instance']
    response_xcom = task_instance.xcom_pull(dag_id="copilot", task_ids="dbt_copilot_run", key="return_value")
    json_xcom = json.loads(response_xcom)
    jobRunID = json_xcom["data"]["id"]
    return jobRunID


with DAG(
        dag_id='copilot',
        default_args=default_args,
        description='Daily K2D ingest into Silver & Gold using Dbt ',
        catchup=False,
        schedule_interval='00 00,12 * * *',
        tags=['content'],
        on_failure_callback=failure_alert,
        on_success_callback=success_alert
) as dag:
    def check(response):
        if response.json()['status']['code'] == 200:
            print("Returning True")
            return True
        else:
            raise AirflowException("DbtCloud job failed !")

    kickoff_task = SimpleHttpOperator(
        task_id='dbt_copilot_run',
        method='POST',
        data=json.dumps({
            "cause": "Triggered by Astronomer!",
            "threads_override": 16
        }),
        response_check=lambda response: check(response),
        http_conn_id='dbt_api',
        endpoint=DbtUtils.getDbtApiLink(DbtUtils.getDbtJobId('copilot', AppConfig.environment.lower()),
                                        DbtUtils.getDbtAccntId()),
        headers={'Content-Type': 'application/json',
                 'Authorization': 'Token ' + DbtUtils.getDbtToken()},
        xcom_push=True,
        log_response=True,
        on_failure_callback=failure_alert
    )

    get_job_runid = PythonOperator(
        task_id='dbt_copilot_jobrunID',
        python_callable=getDbtjobRunID,
        provide_context=True,
        on_failure_callback=failure_alert
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
        task_id='dbt_copilot_run_check',
        http_conn_id='dbt_api',
        endpoint=DbtUtils.getDbtApiLinkforRun(
            DbtUtils.getDbtAccntId()) + "{{ ti.xcom_pull(key='return_value',task_ids='dbt_copilot_jobrunID') }}",
        headers={'Content-Type': 'application/json',
                 'Authorization': 'Token ' + DbtUtils.getDbtToken()},
        log_response=True,
        response_check=lambda response: check_runjob(response),
        poke_interval=120,
        timeout=7200,
        xcom_push=False,
        provide_context=True,
        on_failure_callback=failure_alert,
        on_success_callback=success_alert
    )
kickoff_task >> get_job_runid >> kickoff_task_check

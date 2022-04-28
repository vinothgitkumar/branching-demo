from airflow.operators import DummyOperator, PythonOperator, BashOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime
from airflow import DAG
import copy
from dags.youtube.functions.YoutubePullSensor import YoutubePullSensor
from plugins.utilities.clusters.youtube import get_cluster_config
from plugins.config import YoutubeGlobal as config, AppConfig
import dags.youtube.functions.utils as utils
from functools import partial


def get_job_params(report, market):
    notebook_job_params = copy.deepcopy(notebook_task_params)
    notebook_job_params['notebook_task']['base_parameters']['market'] = market
    notebook_job_params['notebook_task']['base_parameters']['report_name'] = report
    print(notebook_job_params)
    return notebook_job_params


search_config = config.search_config
reports = config.reports
env = AppConfig.environment
load_date = "{{ task_instance.xcom_pull(task_ids='yt_pubsub_sensor', key='_message_value') }}"
databricks_cluster_yt, databricks_yt_cluster_libs = get_cluster_config(config.YOUTUBE_CREDS_PATH, env)
current_date = utils.get_load_date()
actual_date = utils.get_actual_process_date()
notebook_task_params = {
    'libraries': databricks_yt_cluster_libs,
    'notebook_task': {
        'notebook_path': config.yt_silver_notebook,
        'base_parameters': {
            "env": env,
            "market": "",
            "load_date": load_date,
            "report_name": ""
        }
    },
    'new_cluster': databricks_cluster_yt
}

default_args = {
    'owner': config.YOUTUBE_GLOBAL_PROCESS_OWNER,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2021, 10, 10)
}

for market in search_config.keys():
    dag_name = f'youtube_global_sensor_{market}'
    with DAG(dag_name,
             default_args=default_args,
             description='Ingest Enriched Global youtube data to silver layer',
             max_active_runs=10,
             schedule_interval="45 14 * * *",
             tags=['youtube', 'global'],
             catchup=False
             ) as dag:
        start = BashOperator(
            task_id="start",
            bash_command='echo start Youtube Sensor process',
            do_xcom_push=False,
            dag=dag
        )

        create_conn = PythonOperator(
            task_id="create_conn",
            python_callable=utils.create_airflow_connections_yt,
            op_kwargs={
                'gcp_connection_id': config.gcp_connection_id,
                'gcp_vault_key': config.gcp_pubsub_cred
            },
            dag=dag
        )

        initialize_tardis = PythonOperator(
            task_id="initialize_tardis",
            python_callable=utils.get_tardis_process_status,
            op_kwargs={
                'process_source': f"youtube_sensor_{market}",
                'log_date': current_date
            },
            dag=dag
        )

        yt_pubsub_sensor = YoutubePullSensor(
            task_id="yt_pubsub_sensor",
            keys=list(search_config[market].keys()),
            values=list(search_config[market].values()),
            retries=0,
            poke_interval=300,
            timeout=60 * 540,
            mode='reschedule',
            soft_fail=False,
            project=config.project_info_bq_project,
            subscription=config.project_info_subscription[market],
            max_messages=1,
            ack_messages=True,
            gcp_conn_id=config.YOUTUBE_CONNECTION,
            provide_context=True,
            on_success_callback=partial(utils.process_pubsub_message,
                                        reports,
                                        market,
                                        current_date,
                                        dag_name,
                                        actual_date),
            on_failure_callback=partial(utils.log_process_failure,
                                        market,
                                        current_date,
                                        "FAILED",
                                        f"youtube_sensor_{market} process failed."),
            dag=dag
        )

        trigger_databricks_report = [DatabricksSubmitRunOperator(
            task_id=f"trigger_databricks_{report}",
            json=get_job_params(report, market),
            provide_context=True,
            on_failure_callback=partial(utils.log_tardis_callback,
                                        report,
                                        market,
                                        "Data Validation Failed",
                                        "Error: Databricks job Failure"
                                        ),
            dag=dag
        ) for report in reports]

        end_task = DummyOperator(
            task_id='end_task',
            trigger_rule='all_success')

        start >> create_conn >> initialize_tardis >> yt_pubsub_sensor >> trigger_databricks_report >> end_task

    globals()[dag_name] = dag

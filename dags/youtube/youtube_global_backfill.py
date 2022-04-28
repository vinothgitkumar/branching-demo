from airflow import DAG
from datetime import datetime
import dags.youtube.functions.backfill_utils as backfill
from airflow.operators import PythonOperator, BashOperator
from plugins.config import YoutubeGlobal as config, AppConfig
from plugins.utilities.clusters.youtube import get_cluster_config


DAG_NAME = 'youtube_global_backfill'
env = AppConfig.environment

databricks_cluster_yt, databricks_yt_cluster_libs = get_cluster_config(config.YOUTUBE_CREDS_PATH, env)

default_args = {
    'owner': config.YOUTUBE_GLOBAL_PROCESS_OWNER,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2021, 12, 1),
    'retries': 0
}

dag = DAG(DAG_NAME,
          default_args=default_args,
          description='Backfill youtube global data to silver layer',
          max_active_runs=15,
          schedule_interval=None,
          tags=['youtube', 'backfill'],
          catchup=False
          )

start = BashOperator(
    task_id="start",
    bash_command='echo start Youtube Global Backfill',
    do_xcom_push=False,
    dag=dag
)

validate_params = PythonOperator(task_id="validate_params",
                                 python_callable=backfill.validate_params,
                                 op_kwargs={
                                     'start_date': "{{ dag_run.conf['start_date'] }}",
                                     'end_date': "{{ dag_run.conf['end_date'] }}",
                                     'market': "{{ dag_run.conf['market'] }}",
                                     'report_name': "{{ dag_run.conf['report_name'] }}"
                                 },
                                 dag=dag
                                 )

trigger_task = PythonOperator(task_id="trigger_task",
                              python_callable=backfill.trigger_databricks_job,
                              op_kwargs={
                                  'start_date': "{{ dag_run.conf['start_date'] }}",
                                  'end_date': "{{ dag_run.conf['end_date'] }}",
                                  'market': "{{ dag_run.conf['market'] }}",
                                  'report_name': "{{ dag_run.conf['report_name'] }}"
                              },
                              dag=dag
                              )

start >> validate_params >> trigger_task

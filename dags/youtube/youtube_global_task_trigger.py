from airflow import DAG
from datetime import datetime
from airflow.operators import BashOperator, DummyOperator
from plugins.config import YoutubeGlobal as config, AppConfig
from plugins.utilities.clusters.youtube import get_cluster_config
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import BranchPythonOperator
import dags.youtube.functions.backfill_utils as utils


env = AppConfig.environment
databricks_cluster_yt, databricks_yt_cluster_libs = get_cluster_config(config.YOUTUBE_CREDS_PATH, env)
load_dates = "{{dag_run.conf['load_dates']}}"
market = "{{dag_run.conf['market']}}"
report_name = "{{dag_run.conf['report_name']}}"
DAG_OWNER = config.YOUTUBE_GLOBAL_PROCESS_OWNER

notebook_task_params_new = {
    'libraries': databricks_yt_cluster_libs,
    'notebook_task': {
        'notebook_path': config.yt_backfill_notebook,
        'base_parameters': {
            "env": env,
            "market": market,
            "load_dates": load_dates,
            "report_name": report_name
        }
    },
    'new_cluster': databricks_cluster_yt
}

notebook_task_params_old = {
    'libraries': databricks_yt_cluster_libs,
    'notebook_task': {
        'notebook_path': config.yt_missing_data_backfill_notebook,
        'base_parameters': {
            "env": env,
            "market": market,
            "load_dates": load_dates,
            "report_name": report_name
        }
    },
    'new_cluster': databricks_cluster_yt
}


default_args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2021, 12, 1)
}

dag = DAG('youtube_global_task_trigger',
          max_active_runs=15,
          default_args=default_args,
          tags=['backfill'],
          schedule_interval=None
          )

start = BashOperator(
  task_id="start",
  bash_command='echo start Backfill trigger',
  dag=dag
)

choose_backfill_run = BranchPythonOperator(
    task_id="choose_backfill_run",
    python_callable=utils.check_reports,
    op_kwargs={"report_name": report_name, "load_dates": load_dates, "market": market},
    do_xcom_push=False,
    provide_context=True,
    dag=dag
)

submit_backfill_databricks_run = DatabricksSubmitRunOperator(
            databricks_conn_id='databricks_default',
            task_id="submit_backfill_databricks_run",
            run_name="{{ test_run }}",
            json=notebook_task_params_new,
            polling_period_seconds=100,
            retries=1,
            dag=dag
)

submit_missing_data_databricks_run = DatabricksSubmitRunOperator(
            databricks_conn_id='databricks_default',
            task_id="submit_missing_data_databricks_run",
            run_name="{{ test_run }}",
            json=notebook_task_params_old,
            polling_period_seconds=100,
            retries=1,
            dag=dag
)

end = DummyOperator(
    task_id='end',
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

start >> choose_backfill_run >> [submit_backfill_databricks_run, submit_missing_data_databricks_run] >> end

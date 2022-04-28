from airflow import DAG
from datetime import datetime
from functools import partial
from plugins.config import YoutubeGlobal as config, AppConfig
from plugins.utilities.clusters.youtube import get_cluster_config
from airflow.operators import BashOperator, DummyOperator
import dags.youtube.functions.utils as utils
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

DAG_NAME = 'youtube_trending_feed'
env = AppConfig.environment
current_date = utils.get_load_date()
trending_time = utils.fetch_trending_time()
datasource = config.supplementary_reports['trending']
databricks_cluster_yt, databricks_yt_cluster_libs = get_cluster_config(config.YOUTUBE_CREDS_PATH, env)
trending_schedule = utils.get_trending_schedule(config.trending_feed_schedule)
bronze_notebook_task_params = {
    'libraries': databricks_yt_cluster_libs,
    'notebook_task': {
        'notebook_path': config.yt_trending_feed_bronze_notebook,
        'base_parameters': {
            "env": env
        }
    },
    'new_cluster': databricks_cluster_yt
}
silver_notebook_task_params = {
    'libraries': databricks_yt_cluster_libs,
    'notebook_task': {
        'notebook_path': config.yt_trending_feed_silver_notebook,
        'base_parameters': {
            "env": env
        }
    },
    'new_cluster': databricks_cluster_yt
}

default_args = {
    'owner': config.YOUTUBE_GLOBAL_PROCESS_OWNER,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2021, 12, 1)
}

with DAG(DAG_NAME,
         description='Fetch Youtube Trending feeds',
         max_active_runs=1,
         default_args=default_args,
         tags=['youtube', 'trending_feed'],
         schedule_interval=config.trending_feed_schedule,
         catchup=False
         ) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo Start Youtube Trending Feed process',
        do_xcom_push=False,
        dag=dag
    )

    submit_databricks_bronze_run = DatabricksSubmitRunOperator(
        task_id="submit_databricks_bronze_run",
        json=bronze_notebook_task_params,
        provide_context=False,
        retries=1,
        on_failure_callback=partial(utils.log_tardis_failure_callback,
                                    current_date,
                                    "Data Not Received",
                                    datasource,
                                    f"{datasource} Bronze data load failed for trending time {trending_time}."),
        dag=dag
    )

    start_silver_load = BashOperator(
        task_id="start_silver_load",
        bash_command='echo Start Youtube Trending Feed silver load process',
        do_xcom_push=False,
        dag=dag
    )

    submit_databricks_silver_run = DatabricksSubmitRunOperator(
        task_id="submit_databricks_silver_run",
        json=silver_notebook_task_params,
        provide_context=False,
        retries=1,
        on_success_callback=partial(utils.update_audit_data_status,
                                    current_date,
                                    "Data Complete",
                                    datasource,
                                    trending_schedule,
                                    f"{datasource} data loaded successfully!."),
        on_failure_callback=partial(utils.log_tardis_failure_callback,
                                    current_date,
                                    "Data Validation Failed",
                                    datasource,
                                    f"{datasource} data load failed for trending time {trending_time}."),
        dag=dag
    )

    end_task = DummyOperator(
        task_id='end_task',
        trigger_rule='all_success')

    start >> submit_databricks_bronze_run >> start_silver_load >> submit_databricks_silver_run >> end_task

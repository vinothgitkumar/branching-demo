from airflow import DAG
from datetime import datetime
from functools import partial
import dags.youtube.functions.utils as utils
from plugins.config import YoutubeGlobal as config, AppConfig
from plugins.utilities.clusters.youtube import get_cluster_config
from airflow.operators import BashOperator, DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

DAG_NAME = 'youtube_group_contents'
env = AppConfig.environment
current_date = utils.get_load_date()
datasource = config.supplementary_reports['group_content']
encrypted_content_owner_id = "{{dag_run.conf['vault_string']}}"
databricks_cluster_yt, databricks_yt_cluster_libs = get_cluster_config(config.YOUTUBE_CREDS_PATH, env)

group_content_bronze_notebook_task_params = {
    'libraries': databricks_yt_cluster_libs,
    'notebook_task': {
        'notebook_path': config.yt_group_content_bronze_notebook,
        'base_parameters': {
            "env": env,
            "vault_string": encrypted_content_owner_id
        }
    },
    'new_cluster': databricks_cluster_yt
}
group_content_silver_notebook_task_params = {
    'libraries': databricks_yt_cluster_libs,
    'notebook_task': {
        'notebook_path': config.yt_group_content_silver_notebook,
        'base_parameters': {
            "env": env,
            "vault_string": encrypted_content_owner_id
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
         description='Fetch Youtube Group Contents data',
         max_active_runs=1,
         default_args=default_args,
         tags=['youtube', 'groups_contents'],
         schedule_interval=None,
         catchup=False
         ) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo Start Youtube Group content process',
        do_xcom_push=False,
        dag=dag
    )

    submit_group_content_bronze_run = DatabricksSubmitRunOperator(
        databricks_conn_id='databricks_default',
        task_id="submit_group_content_bronze_databricks_run",
        run_name="{{ test_run }}",
        json=group_content_bronze_notebook_task_params,
        polling_period_seconds=30,
        retries=1,
        on_failure_callback=partial(utils.log_tardis_failure_callback,
                                    current_date,
                                    "Data Not Received",
                                    datasource,
                                    f"{datasource} bronze data load failed."),
        dag=dag
    )

    submit_group_content_silver_run = DatabricksSubmitRunOperator(
        databricks_conn_id='databricks_default',
        task_id="submit_group_content_silver_databricks_run",
        run_name="{{ test_run }}",
        json=group_content_silver_notebook_task_params,
        polling_period_seconds=30,
        retries=1,
        on_success_callback=partial(utils.log_tardis_success_callback,
                                    current_date,
                                    "Data Complete",
                                    datasource,
                                    f"{datasource} silver data loaded successfully!."),
        on_failure_callback=partial(utils.log_tardis_failure_callback,
                                    current_date,
                                    "Data Validation Failed",
                                    datasource,
                                    f"{datasource} Silver data load failed."),
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='all_success')

    start >> submit_group_content_bronze_run >> submit_group_content_silver_run >> end

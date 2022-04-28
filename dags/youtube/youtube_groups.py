from airflow import DAG
from functools import partial
from datetime import datetime
import dags.youtube.functions.utils as utils
from plugins.config import YoutubeGlobal as config, AppConfig
from plugins.utilities.clusters.youtube import get_cluster_config
from airflow.operators import PythonOperator, BashOperator, DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

DAG_NAME = 'youtube_groups'
env = AppConfig.environment
current_date = utils.get_load_date()
datasource = config.supplementary_reports['groups']
vault_data = config.CONTENT_OWNER_ID_US
encrypted_content_owner_id = utils.encrypt_vault_data(vault_data)
databricks_cluster_yt, databricks_yt_cluster_libs = get_cluster_config(config.YOUTUBE_CREDS_PATH, env)

groups_bronze_notebook_task_params = {
    'libraries': databricks_yt_cluster_libs,
    'notebook_task': {
        'notebook_path': config.yt_groups_bronze_notebook,
        'base_parameters': {
            "env": env,
            "vault_string": encrypted_content_owner_id
        }
    },
    'new_cluster': databricks_cluster_yt
}
groups_silver_notebook_task_params = {
    'libraries': databricks_yt_cluster_libs,
    'notebook_task': {
        'notebook_path': config.yt_groups_silver_notebook,
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
         description='Fetch Youtube Groups data',
         max_active_runs=1,
         default_args=default_args,
         tags=['youtube', 'groups'],
         schedule_interval="0 15 * * *",
         catchup=False
         ) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo Start Youtube Groups process',
        do_xcom_push=False,
        dag=dag
    )

    submit_bronze_run = DatabricksSubmitRunOperator(
        task_id="submit_groups_bronze_run",
        json=groups_bronze_notebook_task_params,
        provide_context=False,
        retries=1,
        on_failure_callback=partial(utils.log_tardis_failure_callback,
                                    current_date,
                                    "Data Not Received",
                                    datasource,
                                    f"{datasource} Bronze data load failed."),
        dag=dag
    )

    submit_silver_run = DatabricksSubmitRunOperator(
        task_id="submit_groups_silver_run",
        json=groups_silver_notebook_task_params,
        provide_context=False,
        retries=1,
        on_success_callback=partial(utils.log_tardis_success_callback,
                                    current_date,
                                    "Data Complete",
                                    datasource,
                                    f"{datasource} Silver data loaded successfully!."),
        on_failure_callback=partial(utils.log_tardis_failure_callback,
                                    current_date,
                                    "Data Validation Failed",
                                    datasource,
                                    f"{datasource} Silver data load failed."),
        dag=dag
    )

    complete_silver = BashOperator(
        task_id="complete_groups_silver",
        bash_command='echo Youtube Groups Silver data load completed',
        do_xcom_push=False,
        dag=dag
    )

    trigger_group_contents = PythonOperator(task_id="trigger_group_contents",
                                            python_callable=utils.trigger_group_content,
                                            op_kwargs={
                                                'vault_string': encrypted_content_owner_id
                                            },
                                            dag=dag
                                            )

    end = DummyOperator(
        task_id='end_task',
        trigger_rule='all_success')

    start >> submit_bronze_run >> submit_silver_run >> complete_silver >> trigger_group_contents >> end

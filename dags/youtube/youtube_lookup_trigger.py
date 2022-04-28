from airflow import DAG
from datetime import datetime
from plugins.config import VaultConfig
import dags.youtube.functions.utils as utils
from airflow.operators import BashOperator, DummyOperator
from plugins.config import YoutubeGlobal as config, AppConfig
from plugins.utilities.clusters.youtube import get_cluster_config
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

env = AppConfig.environment
databricks_cluster_yt, databricks_yt_cluster_libs = get_cluster_config(config.YOUTUBE_CREDS_PATH, env)
vault_data = VaultConfig.token
encrypted_vault_token = utils.encrypt_vault_data(vault_data)
DAG_OWNER = config.YOUTUBE_GLOBAL_PROCESS_OWNER

notebook_task_params = {
    'libraries': databricks_yt_cluster_libs,
    'notebook_task': {
        'notebook_path': config.yt_lookup_load_notebook,
        'base_parameters': {
            "env": env,
            "vault_string": encrypted_vault_token
        }
    },
    'new_cluster': databricks_cluster_yt
}

default_args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2021, 12, 1),
    'retries': 0
}

with DAG('youtube_lookup_load',
         description='Load lookup data from RDS to Delta',
         max_active_runs=1,
         default_args=default_args,
         tags=['youtube', 'global'],
         schedule_interval="0 0 * * 3",
         catchup=False
         ) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo start lookup load trigger',
        dag=dag
    )

    submit_databricks_run = DatabricksSubmitRunOperator(
        task_id="databricks_lookup_load",
        json=notebook_task_params,
        provide_context=True,
        retries=0,
        dag=dag
    )

    end_task = DummyOperator(
        task_id='end_task',
        trigger_rule='all_success')

    start >> submit_databricks_run >> end_task

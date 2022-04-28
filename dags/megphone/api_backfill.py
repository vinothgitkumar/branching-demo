from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime
from plugins.config import AppConfig, MegaphoneAPIConfig
from plugins.utilities.clusters.megaphone_api import getMegaphoneAPIBronzeCluster, getMegaphoneAPIClusterLibs

DB_API_BACKFILL_NOTEBOOK_PATH = MegaphoneAPIConfig.megaphone_API_back_fill_notebook

default_args = {'owner': 'karan_mudaliar',
                'depends_on_past': False,
                'start_date': datetime(2020, 10, 16),
                'email_on_failure': False,
                'email_on_retry': False,
                }

API_backfill_params = {
    'libraries': getMegaphoneAPIClusterLibs(AppConfig.environment),
    'notebook_task': {
        'notebook_path': DB_API_BACKFILL_NOTEBOOK_PATH,
        'base_parameters': {
            'start_date': '{{ dag_run.conf["start_date"] if dag_run else "" }}',
            'end_date': '{{ dag_run.conf["end_date"] if dag_run else "" }}',
            'env': AppConfig.environment,
        }
    },
    'new_cluster': getMegaphoneAPIBronzeCluster(AppConfig.environment)
}

with DAG("megaphone_api_backfill",
         schedule_interval='1 0 * * *',
         catchup=False,
         default_args=default_args
         ) as dag:
    API_backfill = DatabricksSubmitRunOperator(task_id="Megaphone_api_backfill_run",
                                               json=API_backfill_params)

from datetime import datetime
from airflow import DAG
from marshmellow.airflow.databricks import CondeDatabricksSubmitRunOperator
from plugins.utilities.clusters.youtube_revshare import get_cluster_config
from plugins.config import YoutubeRevShareConfig as config, AppConfig, DatabricksConfig

# ======================================
# CONFIGURATION
# ======================================
ENV = AppConfig.environment

new_cluster = get_cluster_config(ENV)

notebook_task_params = {
    'notebook_task': {
        'notebook_path': config.notebook_name,
        'base_parameters': {
            "env": ENV
        }
    },
}


# # ======================================
# # DAG DECLARATION
# # ======================================

dag = DAG(
    dag_id='youtube-revshare',
    default_args={'owner': 'us_ad2',
                  'depends_on_past': False,
                  'email_on_failure': False,
                  'email_on_retry': False
                  },
    schedule_interval=None,
    start_date=datetime(2021, 10, 1, 0, 0),
    max_active_runs=1,
    catchup=False
)

databricks_run_task = CondeDatabricksSubmitRunOperator(
    task_id='yt-revshare-ingest',
    dag=dag,
    new_cluster=new_cluster,
    json=notebook_task_params,
    depends_on_past=False,
    databricks_conn_id=DatabricksConfig.workspace_conn_id
)

databricks_run_task

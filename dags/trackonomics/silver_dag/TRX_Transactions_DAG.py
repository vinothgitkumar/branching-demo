from functools import partial
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime
# from plugins.trackonomics.silver_layer.transactions import config
# from plugins.trackonomics.silver_layer.transactions import clusters
# from plugins.trackonomics.silver_layer.transactions.utilities import update_tardis_status,check_s3_key
from airflow.operators.python_operator import PythonOperator
from plugins.config import AppConfig, TrackonomicsConfig
from plugins.utilities.clusters.trackonomics import get_trackonomics_silver_cluster_config, get_trackonomics_silver_lib
from dags.trackonomics.silver_dag.utilities import update_tardis_status, check_s3_key

# DB_BRONZE_NOTEBOOK_PATH = config.databricks_notebook_path['transactions']['bronze'][config.env]
# DB_SILVER_NOTEBOOK_PATH = config.databricks_notebook_path['transactions']['silver'][config.env]
DB_BRONZE_NOTEBOOK_PATH = TrackonomicsConfig.trns_bronze_notebook
DB_SILVER_NOTEBOOK_PATH = TrackonomicsConfig.trns_silver_notebook
# VAULT_HASH = encrypt_vault_data(client_data)

arg_env = AppConfig.environment

default_args = {
    'owner': 'us_ochoudha',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 16),
    'email_on_failure': False,
    'email_on_retry': False,
}

bronze_data_load_params = {
    'libraries': get_trackonomics_silver_lib(AppConfig.environment),
    'notebook_task': {
        'notebook_path': DB_BRONZE_NOTEBOOK_PATH,
        'base_parameters': {
            'load_date': '{{ ds }}',
            'env': AppConfig.environment
        }
    },
    'new_cluster': get_trackonomics_silver_cluster_config(arg_env)
}

silver_data_load_params = {
    'libraries': get_trackonomics_silver_lib(AppConfig.environment),
    'notebook_task': {
        'notebook_path': DB_SILVER_NOTEBOOK_PATH,
        'base_parameters': {
            'load_date': '{{ ds }}',
            'env': AppConfig.environment
        }
    },
    'new_cluster': get_trackonomics_silver_cluster_config(arg_env)
}

obj_key = TrackonomicsConfig.trans_src_s3

with DAG(
        "trx_transactions",
        schedule_interval='15 18 * * *',
        catchup=False,
        default_args=default_args
) as dag:
    s3_key_check = PythonOperator(
        task_id="s3_file_check",
        python_callable=check_s3_key,
        op_kwargs={'key': obj_key.format('{{ ds }}')},
        provide_context=True,
        on_failure_callback=partial(update_tardis_status, 'Data', TrackonomicsConfig.trans_tardis_source,
                                    ['Data Not Received'],
                                    'Latest Data Not Loaded in Source Bucket')
    )

    trx_stg = DatabricksSubmitRunOperator(
        task_id='trx_stg_task',
        json=bronze_data_load_params,
        on_success_callback=partial(update_tardis_status, 'Data', TrackonomicsConfig.trans_tardis_source,
                                    ['Data Received', 'Data Staged'],
                                    'Data Loaded in S3 Bronze Location'),
        on_failure_callback=partial(update_tardis_status, 'Data', TrackonomicsConfig.trans_tardis_source,
                                    ['Data Not Received'],
                                    'Bronze Data Load Failed.')
    )

    trx_silver = DatabricksSubmitRunOperator(
        task_id='trx_silver_task',
        json=silver_data_load_params,
        on_success_callback=partial(update_tardis_status, 'Data', TrackonomicsConfig.trans_tardis_source,
                                    ['Data Complete'],
                                    'Data Loaded in Silver Layer and table is updated'),
        on_failure_callback=partial(update_tardis_status, 'Data', TrackonomicsConfig.trans_tardis_source,
                                    ['Data Validation Failed'],
                                    'Silver Data Load Failed.')
    )

s3_key_check >> trx_stg >> trx_silver

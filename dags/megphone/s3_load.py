from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime
from plugins.config import AppConfig, MegaphoneS3Config
from functools import partial
from dags.megphone.utilities import update_tardis_status
from plugins.utilities.clusters.megaphone_s3 import getMegaphoneS3BronzeCluster, getMegaphoneS3SilverCluster, \
    getMegaphoneS3ClusterLibs
from tardis_utils.tardis_sensor.tardis_sensor import Tardis_Sensor


# =======================================
# CONFIGURATION
# =======================================

DAG_NAME = 'megaphone_s3_load'
DB_BRONZE_NOTEBOOK_PATH = MegaphoneS3Config.megaphone_s3_bronze_notebook
DB_SILVER_NOTEBOOK_PATH = MegaphoneS3Config.megaphone_s3_silver_notebook

# ======================================
# DAG DECLARATION
# ======================================

default_args = {
    'owner': 'us_achoudha',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

metrics_bronze_data_load_params = {
    'libraries': getMegaphoneS3ClusterLibs(AppConfig.environment),
    'notebook_task': {
        'notebook_path': DB_BRONZE_NOTEBOOK_PATH,
        'base_parameters': {
            'load_date': '{{ ds }}',
            'file_type': 'metrics',
            'env': AppConfig.environment
        }
    },
    'new_cluster': getMegaphoneS3BronzeCluster(AppConfig.environment)
}

metrics_silver_data_load_params = {
    'libraries': getMegaphoneS3ClusterLibs(AppConfig.environment),
    'notebook_task': {
        'notebook_path': DB_SILVER_NOTEBOOK_PATH,
        'base_parameters': {
            'load_date': '{{ ds }}',
            'file_type': 'metrics',
            'env': AppConfig.environment
        }
    },
    'new_cluster': getMegaphoneS3SilverCluster(AppConfig.environment)
}

impression_bronze_data_load_params = {
    'libraries': getMegaphoneS3ClusterLibs(),
    'notebook_task': {
        'notebook_path': DB_BRONZE_NOTEBOOK_PATH,
        'base_parameters': {
            'load_date': '{{ ds }}',
            'file_type': 'impression',
            'env': AppConfig.environment
        }
    },
    'new_cluster': getMegaphoneS3BronzeCluster(AppConfig.environment)
}

impression_silver_data_load_params = {
    'libraries': getMegaphoneS3ClusterLibs(),
    'notebook_task': {
        'notebook_path': DB_SILVER_NOTEBOOK_PATH,
        'base_parameters': {
            'load_date': '{{ ds }}',
            'file_type': 'impression',
            'env': AppConfig.environment
        }
    },
    'new_cluster': getMegaphoneS3SilverCluster(AppConfig.environment)
}


with DAG(DAG_NAME,
         start_date=datetime(2021, 6, 22),
         max_active_runs=1,
         schedule_interval='30 06 * * *',
         default_args=default_args,
         catchup=False,
         on_success_callback=partial(update_tardis_status, 'Process', 'megaphone_s3_load', 'COMPLETE',
                                     'Megaphone S3 load completed')
         ) as dag:
    check_tardis_complete_status = Tardis_Sensor(
        task_id='check_tardis_complete_status',
        sources='megaphone_s3_load',
        start_logdate='{{ yesterday_ds }}',
        status='Complete',
        poke_interval=5,
        timeout=10,
        on_success_callback=partial(update_tardis_status, 'Process', 'megaphone_s3_load', 'STARTED',
                                    'Tardis updated as started'),
        on_failure_callback=partial(update_tardis_status, 'Process', 'megaphone_s3_load', 'UNFULFILLED DEPENDENCY',
                                    'Previous day run not completed or tardis is down')
    )

    metrics_bronze_data_load = DatabricksSubmitRunOperator(
        task_id='metrics_bronze_data_load',
        json=metrics_bronze_data_load_params,
        on_failure_callback=partial(update_tardis_status, 'Process', 'megaphone_s3_load', 'FAILED',
                                    'Process failed at metrics bronze load')
    )

    metrics_silver_data_load = DatabricksSubmitRunOperator(
        task_id='metrics_silver_data_load',
        json=metrics_silver_data_load_params,
        on_failure_callback=partial(update_tardis_status, 'Process', 'megaphone_s3_load', 'FAILED',
                                    'Process failed at metrics silver load')
    )

    impression_bronze_data_load = DatabricksSubmitRunOperator(
        task_id='impression_bronze_data_load',
        json=impression_bronze_data_load_params,
        on_failure_callback=partial(update_tardis_status, 'Process', 'megaphone_s3_load', 'FAILED',
                                    'Process failed at impression bronze load')
    )

    impression_silver_data_load = DatabricksSubmitRunOperator(
        task_id='impression_silver_data_load',
        json=impression_silver_data_load_params,
        on_failure_callback=partial(update_tardis_status, 'Process', 'megaphone_s3_load', 'FAILED',
                                    'Process failed at impression silver load')
    )

check_tardis_complete_status >> metrics_bronze_data_load >> metrics_silver_data_load
check_tardis_complete_status >> impression_bronze_data_load >> impression_silver_data_load

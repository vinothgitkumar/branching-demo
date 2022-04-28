from functools import partial
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime
from tardis_utils.tardis_sensor.tardis_sensor import Tardis_Sensor
from dags.megphone.utilities import update_tardis_status
from plugins.config import AppConfig, MegaphoneAPIConfig
from plugins.utilities.clusters.megaphone_api import getMegaphoneAPIBronzeCluster, getMegaphoneAPISilverCluster, \
    getMegaphoneAPIClusterLibs

DB_BRONZE_NOTEBOOK_PATH = MegaphoneAPIConfig.megaphone_API_bronze_notebook
DB_SILVER_NOTEBOOK_PATH = MegaphoneAPIConfig.megaphone_API_silver_notebook

default_args = {'owner': 'us_ochoudha',
                'depends_on_past': False,
                'start_date': datetime(2020, 10, 16),
                'email_on_failure': False,
                'email_on_retry': False,
                }

bronze_data_load_params = {
    'libraries': getMegaphoneAPIClusterLibs(AppConfig.environment),
    'notebook_task': {
        'notebook_path': DB_BRONZE_NOTEBOOK_PATH,
        'base_parameters': {
            'load_date': '{{ ds }}',
            'env': AppConfig.environment,
        }
    },
    'new_cluster': getMegaphoneAPIBronzeCluster(AppConfig.environment)
}

silver_data_load_params = {
    'libraries': getMegaphoneAPIClusterLibs(AppConfig.environment),
    'notebook_task': {
        'notebook_path': DB_SILVER_NOTEBOOK_PATH,
        'base_parameters': {
            'load_date': '{{ ds }}',
            'env': AppConfig.environment
        }
    },
    'new_cluster': getMegaphoneAPISilverCluster(AppConfig.environment)
}

with DAG("megaphone_api_load",
         schedule_interval='1 0 * * *',
         catchup=False,
         default_args=default_args
         ) as dag:
    check_tardis_complete_status = Tardis_Sensor(
        task_id='check_tardis_complete_status',
        sources='megaphone_api_load',
        start_logdate='{{ yesterday_ds }}',
        status='Complete',
        poke_interval=5,
        timeout=10,
        on_success_callback=partial(update_tardis_status, 'Process', 'megaphone_api_load', 'STARTED',
                                    'Tardis updated as started'),
        on_failure_callback=partial(update_tardis_status, 'Process', 'megaphone_api_load', 'UNFULFILLED DEPENDENCY',
                                    'Previous day run not completed or tardis is down')
    )

    bronze_load = DatabricksSubmitRunOperator(task_id="Megaphone_bronze_load",
                                              json=bronze_data_load_params,
                                              on_failure_callback=partial(update_tardis_status, 'Process',
                                                                          'megaphone_api_load', 'FAILED',
                                                                          'Process failed at Bronze load'))

    silver_load = DatabricksSubmitRunOperator(task_id="Megaphone_silver_load",
                                              json=silver_data_load_params,
                                              on_failure_callback=partial(update_tardis_status, 'Process',
                                                                          'megaphone_api_load', 'FAILED',
                                                                          'Process failed at Silver load'),
                                              on_success_callback=partial(update_tardis_status, 'Process',
                                                                          'megaphone_api_load', 'COMPLETE',
                                                                          'Megaphone API load completed'))

check_tardis_complete_status >> bronze_load >> silver_load

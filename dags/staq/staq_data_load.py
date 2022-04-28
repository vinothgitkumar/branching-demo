from airflow import DAG
from datetime import datetime
from marshmellow.airflow.databricks import CondeDatabricksSubmitRunOperator

from plugins.config import AppConfig, StaqConfig
from plugins.utilities.slack_service import success_alert, failure_alert
from plugins.utilities.common.dag_utilities import get_default_args
from plugins.utilities.clusters.staq import get_staq_data_load_cluster, get_staq_cluster_lib

# ======================================
# CONFIGURATION
# ======================================

current_day = datetime.today().strftime("%Y-%m-%d")

notebook_task_params = {
    'notebook_task': {
        'notebook_path': StaqConfig.staq_raw_data_load_notebook,
        'base_parameters': {
            'env': AppConfig.environment,
            'run_date': "{{ params.run_date }}",
            'mode': "batch_autoloader"
        }
    },
    'new_cluster': get_staq_data_load_cluster(AppConfig.environment),
    'libraries': get_staq_cluster_lib()
}

# ======================================
# DAG DECLARATION
# ======================================

dag = DAG(
    dag_id='STAQ_DATA_LOAD',
    default_args=get_default_args("us_ssekar2"),
    max_active_runs=1,
    schedule_interval='10 * * * *',
    catchup=False,
    start_date=datetime(2022, 3, 15, 0, 0),
    params={"run_date": current_day}
)

data_load_notebook = CondeDatabricksSubmitRunOperator(
    task_id='data_load_notebook',
    dag=dag,
    json=notebook_task_params,
    depends_on_past=False,
    on_failure_callback=failure_alert,
    on_success_callback=success_alert
)

data_load_notebook

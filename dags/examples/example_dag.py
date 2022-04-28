from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import PythonOperator

from plugins.config import ExampleConfig, AppConfig
from plugins.utilities.clusters.default_cluster import get_default_cluster_config
from plugins.utilities.common.dag_utilities import get_default_args
from plugins.utilities.databricks.DatabricksUtils import create_databricks_connection
from plugins.utilities.slack_service import success_alert

dag = DAG(
    dag_id='a_descriptive_name_for_dag',
    default_args=get_default_args('slack_username_should_used_here'),
    schedule_interval='@once',
    start_date=datetime(2020, 12, 8, 0, 0),
    max_active_runs=1,
    catchup=False,
    on_success_callback=success_alert,
)

"""
References:
    https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators.html
"""
databricks_run_task = DatabricksSubmitRunOperator(
    task_id='a_descriptive_name_for_task',
    dag=dag,
    new_cluster=get_default_cluster_config(project_name='project_name_here', instance_profile=ExampleConfig.instance_profile),
    notebook_task={
        'notebook_path': ExampleConfig.notebook_name,
        'base_parameters': {
            'env': AppConfig.environment,
        },
    },
)

conn_ops = PythonOperator(
    task_id='SetConnections',
    dag=dag,
    python_callable=create_databricks_connection,
)

conn_ops >> databricks_run_task

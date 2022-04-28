from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from plugins.config import NarrativConfig, AppConfig
from plugins.utilities.clusters.default_cluster import get_default_cluster_config


def clicks_notebook_task(dag):
    ingest_clicks = DatabricksSubmitRunOperator(
        task_id='ingest_clicks',
        dag=dag,
        notebook_task={
            'notebook_path': NarrativConfig.clicks_notebook,
            'base_parameters': {
                'env': AppConfig.environment,
            },
        },
        new_cluster=get_default_cluster_config(project_name='narrativ_ingest_clicks', instance_profile=NarrativConfig.instance_profile),
    )
    return ingest_clicks


def orders_notebook_task(dag):
    ingest_orders = DatabricksSubmitRunOperator(
        task_id='ingest_orders',
        dag=dag,
        notebook_task={
            'notebook_path': NarrativConfig.orders_notebook,
            'base_parameters': {
                'env': AppConfig.environment,
            },
        },
        new_cluster=get_default_cluster_config(project_name='narrativ_ingest_orders', instance_profile=NarrativConfig.instance_profile),
    )
    return ingest_orders

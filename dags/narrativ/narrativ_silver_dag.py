from datetime import datetime

from airflow import DAG

from dags.narrativ.tasks.narrative_silver_tasks import clicks_notebook_task, \
    orders_notebook_task
from plugins.utilities.common.dag_utilities import get_default_args
from plugins.utilities.slack_service import success_alert

with DAG(
        dag_id='NARRATIV_SILVER',
        default_args=get_default_args('jstrode'),
        description='Daily Narrativ ingest into silver table.',
        schedule_interval='@daily',
        start_date=datetime(2020, 7, 1, 0, 0),
        catchup=False,
        tags=['narrativ', 'silver'],
        on_success_callback=success_alert
) as dag:
    ingest_clicks_task = clicks_notebook_task(dag=dag)
    ingest_orders_task = orders_notebook_task(dag=dag)

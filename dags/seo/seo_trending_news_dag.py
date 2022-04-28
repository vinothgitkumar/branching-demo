from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime
from plugins.config import AppConfig, SeoConfig
from plugins.utilities.clusters.seo import get_seo_cluster_config, get_seo_lib
from plugins.utilities.slack_service import failure_alert

from dags.seo.utilities import seo_trending_slack_alert

TRENDING_NEWS_NOTEBOOK_PATH = SeoConfig.seo_trending_news_notebook


default_args = {
    'owner': 'us_ochoudha',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    }


data_load_params = {
    'libraries': get_seo_lib(),
    'notebook_task': {
        'notebook_path': TRENDING_NEWS_NOTEBOOK_PATH,
        'base_parameters': {
            'env': AppConfig.environment
        }
    },
    'new_cluster': get_seo_cluster_config(AppConfig.environment)
}

with DAG(
    "SEO_Trending_News_DAG",
    schedule_interval='*/10 * * * *',
    catchup=False,
    default_args=default_args
    ) \
        as dag:

    start = DummyOperator(task_id="Start")

    end = DummyOperator(task_id="End")

    seo_prod = DatabricksSubmitRunOperator(
        task_id='seo_news_prod_task',
        json=data_load_params,
        on_failure_callback=failure_alert,
        on_success_callback=seo_trending_slack_alert
    )

start >> seo_prod >> end

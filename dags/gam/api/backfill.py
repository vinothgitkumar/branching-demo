from plugins.config import AppConfig, GamAPIConfig
import plugins.utilities.clusters.gam as gam_cluster
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import PythonOperator
from dags.gam.api.functions.gam_utils import validate_params
from datetime import datetime, timedelta
from airflow import DAG
import dags.gam.api.functions.gam_utils as helper

DAG_NAME = 'gam_api_backfill'


# ======================================
# Mandatory parameters - start_logdate, end_logdate, market, api_name
# If only one logdate, pass it to end_logdate and pass None for start_logdate
# end_logdate should be equal or greater than start_logdate
# Understand the databricks code for the specific api on how start and end logdates would be handled
# For line_item enter any start_logdate prior to end_logdate - start_logdate would any way be ignored.
# ======================================
# Example config:
# {"start_logdate":"2020-04-02","end_logdate":"2021-04-02","market":"3379","api_name":"company"}


def get_gam_api_backfill_cluster():
    cluster = gam_cluster.get_gam_api_backfill_cluster(GamAPIConfig.GAM_CREDS_PATH, AppConfig.environment)
    cluster['spark_conf']['spark.databricks.acl.dfAclsEnabled'] = "{{ dag_run.conf['do_orc_load'] }}"
    return cluster


default_args = {
    'owner': GamAPIConfig.API_DAG_OWNER_NAME,
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(dag_id=DAG_NAME,
          max_active_runs=10,
          schedule_interval=None,
          default_args=default_args,
          catchup=False,
          )

validate_params = PythonOperator(task_id="validate_params",
                                 python_callable=validate_params,
                                 op_kwargs={
                                     'start_logdate': "{{ dag_run.conf['start_logdate'] }}",
                                     'end_logdate': "{{ dag_run.conf['end_logdate'] }}",
                                     'market': "{{ dag_run.conf['market'] }}",
                                     'api_name': "{{ dag_run.conf['api_name'] }}"
                                 },
                                 dag=dag
                                 )

notebook_params = {
    'start_logdate': "{{ dag_run.conf['start_logdate'] }}",
    'end_logdate': "{{ dag_run.conf['end_logdate'] }}",
    'env': helper.getDatabricksEnv(AppConfig.environment),
    'market': "{{ dag_run.conf['market'] }}",
    'api_name': "{{ dag_run.conf['api_name'] }}",
    'version': GamAPIConfig.GAM_API_VERSION,
    'do_orc_load': "{{ dag_run.conf['do_orc_load'] }}",
    'backfill_from_api': "{{ dag_run.conf['backfill_from_api'] }}"
}

notebook_task_params = {'libraries': gam_cluster.get_gam_api_backfill_clusterLibs(),
                        'notebook_task': {
                            'notebook_path': GamAPIConfig.backfill_notebook,
                            'base_parameters': notebook_params
                        },
                        'new_cluster': get_gam_api_backfill_cluster()
                        }

backfill_task = DatabricksSubmitRunOperator(
    task_id='gam_api_backfill',
    json=notebook_task_params,
    retries=0,
    dag=dag)

validate_params >> backfill_task

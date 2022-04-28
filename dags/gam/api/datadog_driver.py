import time
from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from plugins.config import AppConfig, GamAPIdataDog
import dags.gam.api.functions.gam_utils as helper

# ########## Getting configuration
ENV = AppConfig.environment
DAG_NAME = 'gam_dd_hourly_ingest'


# ======================================
# DAG DECLARATION
# ======================================
default_args = {
    'owner': GamAPIdataDog.API_DAG_OWNER_NAME,
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(DAG_NAME,
          description='Report GAM API to DataDog',
          max_active_runs=1,
          schedule_interval='0 * * * *',
          default_args=default_args,
          catchup=False,
          on_success_callback=partial(helper.send_slack_alert,
                                      'GAM DataDog Dag success',
                                      'Success',
                                      GamAPIdataDog.slack_de_token,
                                      GamAPIdataDog.alert_channel,
                                      None,
                                      None
                                      )
          )


# ======================================
# TASKS
# ======================================

# Dummy Start
start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag)

report = 'datadog_reports'
hr = time.strftime("%H")
report_detail = GamAPIdataDog.GAM_DATADOG_CONFIG_JSON


# Run Notebook Using Databricks API for Bronze
ingest_dd_bronze = DatabricksSubmitRunOperator(
    task_id='ingest_dd_bronze_' + report,
    json=helper.getDataDogBronzeJson(ENV, report, report_detail, hr),
    retries=1,
    dag=dag,
    on_failure_callback=partial(helper.send_slack_alert,
                                "GAM DataDog Bronze: Error in Databricks Job",
                                "Failed",
                                GamAPIdataDog.slack_de_token,
                                GamAPIdataDog.failure_channel,
                                GamAPIdataDog.additional_alertee,
                                None
                                )

)

# Run Notebook Using Databricks API for Silver
ingest_dd_silver = DatabricksSubmitRunOperator(
    task_id='ingest_dd_silver_' + report,
    json=helper.getDataDogSilverJson(ENV, report, report_detail, hr),
    retries=1,
    dag=dag,
    on_failure_callback=partial(helper.send_slack_alert,
                                "GAM DataDog Silver: Error in Databricks Job",
                                "Failed",
                                GamAPIdataDog.slack_de_token,
                                GamAPIdataDog.failure_channel,
                                GamAPIdataDog.additional_alertee,
                                None
                                )

)

start_dag >> ingest_dd_bronze >> ingest_dd_silver >> end_dag

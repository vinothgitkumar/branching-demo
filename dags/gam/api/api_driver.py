from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from plugins.config import AppConfig, GamAPIConfig
import dags.gam.api.functions.gam_utils as helper

from tardis_utils.tardis_sensor.tardis_sensor import Tardis_Sensor

# ======================================
# CONFIGURATION
# ======================================
ENV = AppConfig.environment
DAG_NAME_SUFFIX = '_daily_ingest'

default_args = {
    'owner': GamAPIConfig.API_DAG_OWNER_NAME,
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}


def create_preprocessing_task(dag, report_detail, dt):
    if 'dependent_log_type' in report_detail.keys():
        dependent_source = report_detail['dependent_log_type']
        markets = report_detail['market']
        tardis_source = ['gam.' + x + "_" + y for x in dependent_source for y in markets]
        preprocessing = Tardis_Sensor(task_id="check_tardis_complete",
                                      sources=tardis_source,
                                      start_logdate=dt,
                                      status='Complete',
                                      poke_interval=10,
                                      dag=dag,
                                      )
    else:
        preprocessing = DummyOperator(
            task_id='preprocessing',
            dag=dag,
            )
    return preprocessing


def create_dag(dag_id, api_name):
    report_detail = helper.getAPIReportDetails(api_name)
    print(report_detail)
    dag = DAG(dag_id,
              schedule_interval=report_detail['schedule'],
              catchup=False,
              default_args=default_args,
              description=f'GAM Daily report for {api_name}',
              on_success_callback=partial(helper.send_slack_alert,
                                          f'GAM API {api_name} Dag success',
                                          'Success',
                                          GamAPIConfig.slack_de_token,
                                          GamAPIConfig.alert_channel,
                                          None,
                                          None
                                          )
              )
    dt = helper.get_load_date(api_name)

    with dag:
        start_dag = DummyOperator(
            task_id='start_dag'
        )
        end_dag = DummyOperator(
            task_id='end_dag'
        )
        preprocessing = create_preprocessing_task(dag, report_detail, dt)
        ingest_api_bronze = DatabricksSubmitRunOperator(
                task_id=f'ingest_{report}_bronze',
                json=helper.getAPIBronzeParam(ENV, dt, report, report_detail['market']),
                retries=2,
                on_failure_callback=partial(helper.send_slack_alert,
                                            f"GAM API {api_name} Bronze: Error in Databricks Job",
                                            "Failed",
                                            GamAPIConfig.slack_de_token,
                                            GamAPIConfig.failure_channel,
                                            GamAPIConfig.additional_alertee,
                                            None
                                            )
            )
        ingest_api_silver = DatabricksSubmitRunOperator(
            task_id=f'ingest_{report}_silver',
            json=helper.getAPISilverParam(ENV, dt, report, report_detail['market']),
            retries=2,
            on_failure_callback=partial(helper.send_slack_alert,
                                        f"GAM API {api_name} Silver : Error in Databricks Job",
                                        "Failed",
                                        GamAPIConfig.slack_de_token,
                                        GamAPIConfig.failure_channel,
                                        GamAPIConfig.additional_alertee,
                                        None
                                        )
        )
        start_dag >> preprocessing >> ingest_api_bronze >> ingest_api_silver >> end_dag

    return dag


reports = GamAPIConfig.api_reports
for report in reports:
    print("report:", report)
    dag_id = 'gam_' + report + DAG_NAME_SUFFIX
    globals()[dag_id] = create_dag(dag_id,
                                   report
                                   )

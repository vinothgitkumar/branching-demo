from airflow.exceptions import AirflowException

from dags.google_analytics.functions.base import logger, get_class, update_tardis
from plugins.config import AppConfig

env = AppConfig.environment


def check_update_tardis_complete(params=None, context=None):
    # print(params)
    if params["dag_run"].conf is not None:
        if "dag_dataset" in params["dag_run"].conf and params["dag_run"].conf["dag_dataset"] is not None:
            load_date = params["dag_run"].conf["dag_dataset"]["load_date"]
            log_date = params["dag_run"].conf["dag_dataset"]["log_date"]
            logger.debug("Class Name " + params['dag_run'].conf['dag_dataset']['class_name'])
            logger.debug(load_date, log_date)
            GoogleAnalyticClass = get_class(params["dag_run"].conf["dag_dataset"]["class_name"])
            if load_date == log_date:
                update_tardis(post_type=["Process"],
                              logdate=log_date,
                              tardis_source=GoogleAnalyticClass.tardis_process_source,
                              status="Complete",
                              comments="Completed with the GA load for the day..!"
                              )
        else:
            raise AirflowException("Dag Dataset is Empty..!!")


def check_post_silver_data_loads(context=None, **kwargs):
    if 'dag_dataset' in kwargs.keys():
        if hasattr(get_class(eval(kwargs['dag_dataset'])['class_name']), 'brand_mapping_notebook'):
            return 'brand_mapping'
        else:
            return 'end'
    else:
        AirflowException('DAG Dataset missing..!!')

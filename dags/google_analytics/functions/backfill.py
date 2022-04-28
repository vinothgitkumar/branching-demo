import random
import string
from time import sleep

from airflow.api.common.experimental.trigger_dag import trigger_dag

from plugins.config import AppConfig

env = AppConfig.environment

RANDOM_STRING_LENGTH = 10
RANDOM_STRING = ''.join(random.choices(string.ascii_lowercase +
                                       string.digits, k=RANDOM_STRING_LENGTH))


def orchestrate(**context):
    if "orchestrate" not in context['dag_run'].conf:
        return "create_conn_db"
    else:
        return "load_bronze_tasks"


def trigger_dags(DAG_NAME, **context):
    # print(context)
    # print(context['dag_run'].conf)
    if "orchestrate" not in context['dag_run'].conf:
        conf = {
            'bronze': bool(context['dag_run'].conf['bronze']) if "bronze" in context['dag_run'].conf.keys() else False,
            'silver': bool(context['dag_run'].conf['silver']) if "silver" in context['dag_run'].conf.keys() else False,

        }
        print(list(context['dag_run'].conf['logdates']))

        if True not in conf.values():
            conf['bronze'] = conf['silver'] = True

        for dt in list(context['dag_run'].conf['logdates']):
            sleep(2)
            print(dt)
            trigger_dag(DAG_NAME,
                        conf={"logdate": dt,
                              "orchestrate": True,
                              "bronze": conf['bronze'],
                              "silver": conf['silver'],
                              },
                        run_id="run_" + RANDOM_STRING + "_" + dt)


def check_func(**context):
    # print(context)
    # print(context['logdate'])

    if context['layer'] in context['dag_run'].conf.keys() and context['dag_run'].conf[context['layer']]:
        return "create_conn_db"
    else:
        return "end_task"

import base64
from datetime import datetime
import json
from airflow.api.client.local_client import Client
from airflow.exceptions import AirflowException
from dags.google_analytics.functions.base import update_tardis, get_class
from plugins.utilities.common.dag_utilities import get_logger
from plugins.utilities.json_hierarchy_finder import find_hierarchy
from plugins.utilities.airflow_connections import create_gcp_connection, create_databricks_connection

logger = get_logger()


def validate_inputs(severity_heir, rows_inserted_heir, resource_name_heir):
    if not severity_heir or list(severity_heir.values())[0] != "INFO":
        raise Exception(f"Wrong Msg type received ({severity_heir})..!! Kindly clear the Dag Run from Pull Msg task.!")

    if not rows_inserted_heir:
        raise Exception("Something wrong with the PubSub message(insertedRowsCount)..Kindly check the message..!!")
    else:
        rows_inserted = int(list(rows_inserted_heir.values())[0])

    if not resource_name_heir:
        raise Exception("Something wrong with the PubSub message(resource_name)..Kindly check the message..!!")
    else:
        resource_name = list(resource_name_heir.values())[0]

    return rows_inserted, resource_name


def decrypt_push_xcom(GoogleAnalyticsClass, DAG_ID, context=None):
    try:
        print(context)
        # print(TARDIS_SOURCE, DATASET_ID, DAG_ID, SILVER_DB_NOTEBOOK_PATH)
        dataset = {}
        value = context["ti"].xcom_pull(key="return_value",
                                        task_ids="pull_messages_ga")

        if value is None:
            raise AirflowException("No Value passed")
        logger.info(value[0])
        print("****" * 10)
        print(value[0]["message"]["data"])
        msg = json.loads(base64.b64decode(value[0]["message"]["data"])
                         .decode("utf-8"))
        print("****" * 10, "\nPubSub msg: ")
        logger.info(json.dumps(msg, indent=2))
        print("****" * 10)

        dataset_id_heir = find_hierarchy(input_json=json.dumps(msg),
                                         search_key="dataset_id",
                                         occurrence=1)

        if not dataset_id_heir:
            raise Exception("Something wrong with the PubSub message(dataset_id)..Kindly check the message..!!")
        else:
            DATASET_ID = int(list(dataset_id_heir.values())[0])
            if DATASET_ID != GoogleAnalyticsClass.project_info_project_id:
                raise AirflowException(
                    f"Received {DATASET_ID} message in {GoogleAnalyticsClass.project_info_project_id}")

        severity_heir = find_hierarchy(input_json=json.dumps(msg),
                                       search_key="severity",
                                       occurrence=1)

        rows_inserted_heir = find_hierarchy(input_json=json.dumps(msg),
                                            search_key="insertedRowsCount",
                                            occurrence=1)

        resource_name_regex = "^projects/" + GoogleAnalyticsClass.project_info_bq_project + "/datasets/" + str(
            DATASET_ID) + "/.*$"

        resource_name_heir = find_hierarchy(input_json=json.dumps(msg),
                                            search_value=resource_name_regex,
                                            occurrence=1,
                                            regex=True)

        rows_inserted, resource_name = validate_inputs(severity_heir, rows_inserted_heir,
                                                       resource_name_heir)

        logger.info(str(rows_inserted) + " " + resource_name)
        load_date = datetime.strftime(
            datetime.strptime((resource_name.split("/")[5].split("_")[2]), "%Y%m%d"), "%Y-%m-%d")
        logger.info(load_date)

        if context["run_id"].split("_")[0] == "scheduled":
            log_date = context["ds"]
        else:
            log_date = context["dag_run"].conf["log_date"]

        logger.info(f"load_date: {load_date} \n log_date: {log_date}")

        dataset["dataset_id"] = DATASET_ID
        dataset["log_date"] = log_date
        dataset["tardis_source"] = GoogleAnalyticsClass.tardis_data_source
        dataset['load_date'] = load_date

        dataset['class_name'] = get_class(GoogleAnalyticsClass).__name__

        context["ti"].xcom_push(key="dataset", value=dataset)
        logger.info("Dataset" + str(dataset))
        print(load_date, log_date)

        if log_date != load_date:
            retrigger_dag(GoogleAnalyticsClass.tardis_data_source,
                          DAG_ID,
                          load_date=load_date,
                          rows_inserted=rows_inserted,
                          log_date=log_date)
        else:
            update_tardis(post_type=["Data", "AuditLog"],
                          tardis_source=GoogleAnalyticsClass.tardis_data_source,
                          status="Data Received",
                          logdate=load_date,
                          numRecords=rows_inserted,
                          comments="Messaged received by PubSub Listener..!! ")
    except Exception as e:
        raise AirflowException(e)


def retrigger_dag(TARDIS_DATA_SOURCE, DAG_ID, load_date, rows_inserted, log_date=None):
    '''
    load_date: data in the msg
    log_date: date of run
    '''
    update_tardis(post_type=["Data"],
                  tardis_source=TARDIS_DATA_SOURCE,
                  status="Data Reload",
                  logdate=load_date,
                  comments="Data reloaded because data refreshed message received..!!")
    update_tardis(post_type=["Data", "AuditLog"],
                  tardis_source=TARDIS_DATA_SOURCE,
                  status="Data Received",
                  numRecords=rows_inserted,
                  logdate=load_date,
                  comments="Data refreshed message received by PubSub Listener..!!")
    c = Client(None, None)
    c.trigger_dag(dag_id=DAG_ID, conf={"log_date": str(log_date)})


def check_update_tardis_start(TARDIS_SOURCE, **kwargs):
    if kwargs["run_id"].split("_")[0] == "scheduled":
        update_tardis(post_type=["Process"],
                      logdate=kwargs["ds"],
                      tardis_source=TARDIS_SOURCE,
                      status="STARTED",
                      comments="Starting with the GA load"
                      )


def get_run_conf(context, dag_obj):
    dataset = context["ti"].xcom_pull(task_ids="pull_messages_ga",
                                      key="dataset")

    print(dataset['class_name'])

    load_date = context["ti"].xcom_pull(task_ids="pull_messages_ga",
                                        key="load_date")

    run_id = context["run_id"]

    logger.info(str(dataset))
    logger.info(str(run_id))

    dag_obj.payload = {
        "dag_dataset": dataset,
        "load_date": load_date
    }
    dag_obj.run_id = run_id + "_" + str(dataset['dataset_id'])
    return dag_obj


def create_airflow_connections_ga(gcp_connection_id, gcp_vault_key):
    logger.info('Creating Airflow Connection for Databricks')
    create_databricks_connection()
    logger.info('Creating Airflow Connection for GCP')
    create_gcp_connection(connection_id=gcp_connection_id, vault_key=gcp_vault_key, force_create=False)

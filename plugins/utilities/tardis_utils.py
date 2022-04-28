import logging
import warnings
from airflow.exceptions import AirflowException
import os

if os.getenv("ENV") in ['prod', 'production']:
    os.environ['ENVIRONMENT'] = 'production'

from tardis import client

logging.basicConfig(format='{%(asctime)s} %(levelname)s : {%(filename)s: %(funcName)s } %(message)s ',
                    level=logging.DEBUG)

tardis_logger = logging.getLogger("TardisService")


def update_tardis(**kwargs):
    warnings.warn("This package will move to tardis_utils in future..!!", category=DeprecationWarning)

    print(kwargs)
    parameters = {
        'additionalInfo': '',
        'numRecords': 0,
        'comments': '',
        'tardis_source': '',
        'status': '',
        'logdate': ''
    }
    parameters.update(kwargs)
    tardis_logger.info("Parameters :" + str(parameters))
    try:
        if "Data" in kwargs['post_type']:
            data_response = client.Post(post_type="Data",
                                        source=f"{parameters['tardis_source']}",
                                        logdate=f"{parameters['logdate']}",
                                        status=f"{parameters['status']}",
                                        comments=f"{parameters['comments']}").read()

            if "errors" in data_response.keys():
                tardis_logger.error(data_response)
                raise AirflowException("Tardis Data Log Failed. Reason: " + data_response['errors'][0]['message'])
            tardis_logger.info("Data Response: " + str(data_response['data']))

        if "AuditLog" in kwargs['post_type']:
            audit_response = client.Post(post_type='AuditLog',
                                         source=f"{parameters['tardis_source']}",
                                         logdate=f"{parameters['logdate']}",
                                         status=f"{parameters['status']}",
                                         additionalInfo=f"{parameters['additionalInfo']}",
                                         numRecords=f"{parameters['numRecords']}").read()
            if "errors" in audit_response.keys():
                tardis_logger.error(audit_response)
                raise AirflowException("Tardis Audit Log Failed. Reason: " + audit_response['errors'][0]['message'])
            tardis_logger.info("Audit Response: " + str(audit_response['data']))

        if "Process" in kwargs['post_type']:
            process_response = client.Post(post_type='Process',
                                           source=f"{parameters['tardis_source']}",
                                           logdate=f"{parameters['logdate']}",
                                           status=f"{parameters['status']}",
                                           comments=f"{parameters['comments']}").read()

            if "errors" in process_response.keys():
                tardis_logger.error(process_response)
                raise AirflowException("Tardis Process Log Failed. Reason: " + process_response['errors'][0]['message'])
            print(process_response)
            tardis_logger.info("Process Response: " + str(process_response['data']))

    except Exception as e:
        tardis_logger.error(e)
        raise AirflowException(e)


def read_tardis(**kwargs):
    print(kwargs)
    parameters = {
        'tardis_source': '',
        'logdate': ''
    }
    parameters.update(kwargs)
    tardis_logger.info("Parameters :" + str(parameters))
    try:
        if "AuditLog" in kwargs['get_type']:
            audit_response = client.Get(get_type='AuditLog',
                                        sourceName=f"{parameters['tardis_source']}",
                                        startLogdate=f"{parameters['logdate']}",
                                        endLogdate=f"{parameters['logdate']}").read()
            if "errors" in audit_response.keys():
                tardis_logger.error(audit_response)
                raise AirflowException(
                    "Tardis Audit Log Read Failed. Reason: " + audit_response['errors'][0]['message'])
            tardis_logger.info("Audit Response: " + str(audit_response['data']))
            if len(audit_response["data"]["auditLog"]["results"]) >= 1:
                return audit_response["data"]
            else:
                return ''
        if "Data" in kwargs['get_type']:
            data_response = client.Get(get_type='Data',
                                       sourceName=f"{parameters['tardis_source']}",
                                       startLogdate=f"{parameters['logdate']}",
                                       endLogdate=f"{parameters['logdate']}").read()
            if "errors" in data_response.keys():
                tardis_logger.error(data_response)
                raise AirflowException(
                    "Tardis dataStatus Log Read Failed. Reason: " + data_response['errors'][0]['message'])
            tardis_logger.info("dataStatus Response: " + str(data_response['data']))
            if len(data_response["data"]["dataStatus"]["results"]) == 1:
                return data_response["data"]
            else:
                return ''
    except Exception as e:
        tardis_logger.error(e)
        raise AirflowException()

from datetime import timedelta, datetime

from plugins.utilities.slack_service import failure_alert
import logging

# This should be used to as the default args for a standard dag. If a dag requires other defaults or functions require a different configuration,
# feel free to skip this.
# Owner should be supplied to allow slack alerts to tag the appropriate person in case of failure. Owner value should be the text after `@` used when tagging
# someone in slack


def get_default_args(owner: str):
    return {
        'owner': owner,
        'depends_on_past': False,
        'start_date': datetime(2020, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': failure_alert,
        'provide_context': True,
    }


def get_logger():
    logging.basicConfig(format='{%(asctime)s} %(levelname)s : {%(filename)s: %(funcName)s } %(message)s ',
                        level=logging.DEBUG)
    logger = logging.getLogger("airflow.task")
    return logger

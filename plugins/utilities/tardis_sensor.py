import logging
from datetime import datetime as dt
from airflow.exceptions import AirflowException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
import os

# tardis environment setup
if os.getenv("ENV") in ['prod', 'production']:
    os.environ['ENVIRONMENT'] = 'production'


try:
    from tardis import client
except ImportError as error:
    print(error + "Error while importing Tardis client module.\n Download & install pre-requisite \
    from https://tardis.conde.io/download/tardis-{version}-py3-none-any.whl")

logging.basicConfig(format='{%(asctime)s} %(levelname)s : {%(filename)s: %(funcName)s } %(message)s ',
                    level=logging.DEBUG)
logger = logging.getLogger("airflow.task")


class TardisDataStatusSensor(BaseSensorOperator):
    ui_color = '#50aaff'
    # valid_modes = ['poke']

    template_fields = ['sources', 'start_logdate', 'end_logdate', 'status', 'updateTs']

    # valid_statuses = ['Data Complete', 'Data Not Received', 'Data Received', 'Data Prepared', 'Data Staged',
    # 'Data Loaded', 'Data Validation Failed', 'Data Complete', 'Data Reload']

    def __init__(self,
                 sources,
                 start_logdate,
                 end_logdate=None,
                 updateTs=None,
                 status='Data Complete',
                 xcom_push=False,
                 *args,
                 **kwargs
                 ):
        super(TardisDataStatusSensor, self).__init__(*args, **kwargs)
        self.sources = [sources] if type(sources) != list else sources
        self.start_logdate = start_logdate
        self.end_logdate = self.start_logdate if end_logdate is None else end_logdate
        # self.mode = mode
        self.updateTs = updateTs
        self._message = None
        self.status = status
        self.do_xcom_push = xcom_push

    def execute(self, context):
        super(TardisDataStatusSensor, self).execute(context)
        return self._message

    def poke(self, context):
        try:
            response = client.Get(get_type='Data',
                                  sourceName=self.sources,
                                  startLogdate=self.start_logdate,
                                  endLogdate=self.end_logdate,
                                  status=self.status,
                                  updateTs=self.updateTs
                                  )

            response_dict = response.read()
            logger.info(response_dict)
            if self.updateTs is not None:
                self._message = response_dict
            else:
                start_logdt = dt.strptime(self.start_logdate, '%Y-%m-%d')
                end_logdt = dt.strptime(self.end_logdate, '%Y-%m-%d')
                date_range = end_logdt - start_logdt
                days_range = int(date_range.days) + 1
                if len(response_dict['data']['dataStatus']['results']) == (days_range * len(self.sources)):
                    self._message = response_dict

            return self._message
        except Exception as e:
            raise AirflowException(e)

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.decorators import apply_defaults
from datetime import timedelta, datetime
from plugins.config import GAMLogConfig
from dags.gam.logs.functions.utils import hour_exists, tardis_update_for_corrected_files, file_processed


class GoogleCloudStorageCustomHook(GoogleCloudStorageHook):
    def __init__(self,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GoogleCloudStorageHook, self).__init__(google_cloud_storage_conn_id,
                                                     delegate_to)

    def get_last_updated_time(self, bucket, object):
        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.get_blob(blob_name=object)
        blob_update_time = blob.updated
        return blob_update_time


class GAMLogsSensor(BaseSensorOperator):
    """
    Checks for the existence of files in Google Cloud Storage.

    :param bucket: The Google cloud storage bucket where the objects are.
    :type bucket: str
    :param report_name: The report to check in the Google cloud
        storage bucket.
    :type report_name: str
    :param network_id: The network_id for which report has to be checked in the Google cloud
        storage bucket.
    :type network_id: int
    :param log_date: The logdate for which report has to be checked in the Google cloud
        storage bucket.
    :type log_date: str
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ['log_date']

    @apply_defaults
    def __init__(self,
                 bucket,
                 network_id,
                 network_code,
                 report_name,
                 log_date,
                 google_cloud_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):
        super(GAMLogsSensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.network_id = network_id
        self.network_code = network_code
        self.report_name = report_name
        self.log_date = log_date
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to
        self.sensed_files = {}

    def execute(self, context):
        """Overridden to allow sensed objects to be passed"""
        super(GAMLogsSensor, self).execute(context)
        return self.sensed_files

    def poke(self, context):
        hook = GoogleCloudStorageCustomHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)

        gam_files = {}
        for hr in range(0, 24):
            if hour_exists(f"{self.log_date}{str(hr).zfill(2)}"):
                file = GAMLogConfig.GAM_FILE_FORMAT.format(self.report_name,
                                                           self.network_id,
                                                           self.log_date,
                                                           str(hr).zfill(2))
                corrected_file = GAMLogConfig.GAM_CORRECTED_FILE_FORMAT.format(self.report_name,
                                                                               self.network_id,
                                                                               self.log_date,
                                                                               str(hr).zfill(2))
                gam_files[file] = corrected_file

        self.log.info('Bucket: {0}, Objects:{1}'.format(self.bucket, gam_files))

        daily_files = []
        for obj in gam_files.keys():
            if not hook.exists(self.bucket, obj):
                daily_files = None
                break
            else:
                if hook.exists(self.bucket, gam_files[obj]):
                    daily_files.append(gam_files[obj])
                else:
                    daily_files.append(obj)

        if daily_files is not None:
            self.sensed_files[self.log_date] = daily_files
            log_date_p = datetime.strptime(self.log_date, "%Y%m%d")
            corrected_date = log_date_p - timedelta(days=2)
            blob_names = hook.list(bucket=self.bucket,
                                   prefix=self.report_name)

            corrected_files = []
            for blob_name in blob_names:
                if blob_name.endswith("_corrected.gz"):
                    last_updated_time = hook.get_last_updated_time(bucket=self.bucket,
                                                                   object=blob_name)
                    if last_updated_time.date() >= corrected_date.date() and not file_processed(blob_name,
                                                                                                last_updated_time,
                                                                                                self.network_code):
                        tardis_update_for_corrected_files(blob_name, self.network_code, last_updated_time)
                        corrected_files.append(blob_name)

            if len(corrected_files) > 0:
                for file in corrected_files:
                    log_date = file.split("_")[2]
                    if log_date in self.sensed_files.keys():
                        if file not in self.sensed_files[log_date]:
                            key_files = self.sensed_files[log_date]
                            key_files.append(file)
                    else:
                        self.sensed_files[log_date] = [file]

        return self.sensed_files

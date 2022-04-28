from plugins.config import GAMLogConfig
from datetime import datetime, timedelta
from dags.gam.logs.functions.utils import hour_exists
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from dags.gam.logs.functions.utils import trigger_bronze_dag, initialise_tardis_data_status


def validate_report(params):
    market = params.get('market')
    if market is None or market not in GAMLogConfig.network_id_and_bucket_name_by_market.keys():
        raise Exception("Invalid market")

    network_id = GAMLogConfig.network_id_and_bucket_name_by_market[market]['network_id']
    report_name = params.get('report_name')

    if report_name is None or report_name not in GAMLogConfig.gam_log_types[network_id]:
        raise Exception("Invalid Report Name")

    if GAMLogConfig.gam_log_types[network_id][report_name] != 'active':
        raise Exception(report_name, "Report is not active for market ", market)


def parse_date(date_text):
    try:
        return datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD :", date_text)


def validate_date(params):
    start_logdate = params.get('start_logdate')
    end_logdate = params.get('end_logdate')

    p_start_logdate = parse_date(start_logdate)
    p_end_logdate = parse_date(end_logdate)

    if p_start_logdate > p_end_logdate:
        raise Exception("Start date must be before End date")


def validate_params(**params):
    validate_report(params)
    validate_date(params)


def get_gcs_hook(google_cloud_conn_id, delegate_to=None):
    return GoogleCloudStorageHook(
        google_cloud_storage_conn_id=google_cloud_conn_id,
        delegate_to=delegate_to)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def trigger_job(**params):
    start_logdate = parse_date(params.get('start_logdate'))
    end_logdate = parse_date(params.get('end_logdate'))

    market = params.get('market')
    market_code = GAMLogConfig.network_id_and_bucket_name_by_market[market]['network_code']
    network_id = GAMLogConfig.network_id_and_bucket_name_by_market[market]['network_id']
    report_name = params.get('report_name')
    logdate_gcs_files_dict = {}
    read_bkp_bucket = params.get('read_bkp_bucket')
    bronze_notebook = params.get('bronze_notebook')

    for log_date in daterange(start_logdate, end_logdate):
        log_date_str = log_date.strftime("%Y%m%d")
        logdate_gcs_files_dict[log_date_str] = get_files_in_gcs(report_name, network_id, log_date_str, market,
                                                                read_bkp_bucket=read_bkp_bucket)
    print('Triggering bronze dag for following date and files: ', logdate_gcs_files_dict)
    initialise_tardis_status(logdate_gcs_files_dict, report_name, market_code)
    trigger_bronze_dag(logdate_gcs_files_dict, report_name, network_id, market_code, read_bkp_bucket=read_bkp_bucket,
                       bronze_notebook=bronze_notebook)


def initialise_tardis_status(logdate_gcs_files_dict, report_name, market_code):
    for log_date in logdate_gcs_files_dict.keys():
        try:
            print(f'Initialising tardis status for report : {report_name}, market: {market_code} and log date: {log_date}')
            initialise_tardis_data_status(report_name=report_name, network_code=market_code, logdate=log_date)
        except Exception as ex:
            print(f'Unable to initialise tardis status  for report : {report_name}, market: {market_code} and log date: {log_date} with error', ex)


def get_files_in_gcs(report_name, network_id, log_date, market, read_bkp_bucket='false'):
    hook = GoogleCloudStorageHook(GAMLogConfig.GAM_CONNECTION)
    gcs_bucket = GAMLogConfig.network_id_and_bucket_name_by_market[market]['gam_source_bucket']
    if read_bkp_bucket == 'true':
        gcs_bucket += "-bkp"
    daily_files = []
    for hr in range(0, 24):
        if hour_exists(f"{log_date}{str(hr).zfill(2)}"):
            file_name, corrected_file = get_file_name(report_name, network_id, log_date, hr)

            if hook.exists(gcs_bucket, corrected_file):
                daily_files.append(corrected_file)
            else:
                daily_files.append(file_name)

    return daily_files


def get_file_name(report_name, network_id, log_date, hr):
    file = GAMLogConfig.GAM_FILE_FORMAT.format(report_name,
                                               network_id,
                                               log_date,
                                               str(hr).zfill(2))
    corrected_file = GAMLogConfig.GAM_CORRECTED_FILE_FORMAT.format(report_name,
                                                                   network_id,
                                                                   log_date,
                                                                   str(hr).zfill(2))

    return file, corrected_file

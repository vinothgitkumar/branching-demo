import os
from abc import ABCMeta

from airflow.models import Variable


class GamCommon(metaclass=ABCMeta):

    API_DAG_OWNER_NAME = os.getenv("GAM_API_DAG_OWNER", Variable.get('GAM_API_DAG_OWNER', 'us_tara'))
    GAM_API_VERSION = os.getenv("GAM_VERSION", Variable.get('GAM_VERSION', 'v202108'))
    GAM_CREDS_PATH = Variable.get("GAM_CREDS_PATH", "/dbfs/FileStore/data_eng/creds/gam-creds.json")
    GAM_CRED_SECRET_PATH = "secret/data-services/astronomer/prod/gam_global"

    # TODO with slack integration
    GAM_ERROR_TEMPLATE = ''


class TardisConfig():
    CLIENT_JAR_PATH = Variable.get('TARDIS_CLIENT_JAR_PATH', 'https://tardis.conde.io/download/tardis-0.1.3'
                                                             '-py3-none-any.whl')


class GamAPICommon(GamCommon):

    pql_query_list = ['pql_ad_category',
                      'pql_ad_unit',
                      'pql_audience_segment',
                      'pql_audience_segment_category',
                      'pql_bandwidth_group',
                      'pql_bandwidth_group',
                      'pql_browser',
                      'pql_browser_language',
                      'pql_device_capability',
                      'pql_device_category',
                      'pql_device_manufacturer',
                      'pql_geo_target',
                      'pql_line_item',
                      'pql_mobile_carrier',
                      'pql_mobile_device',
                      'pql_mobile_device_submodel',
                      'pql_operating_system',
                      'pql_operating_system_version',
                      'pql_programmatic_buyer',
                      'pql_third_party_company',
                      'pql_time_zone']

    api_reports = {
        'pql': {
            'market': {
                # market_id : [query_list]
                # '3379': pql_query_list,
                '5574': pql_query_list,
                # '45577845': pql_query_list,
            },
            'schedule': '30 8 * * *',
        }, 'company': {
            'market': ["3379", "5574"],
            'schedule': '30 5 * * *'
        }, 'ad_units': {
            'market': ["3379", "5574"],
            'schedule': '25 8 * * *'
        }, 'placement': {
            'market': ["3379", "5574"],
            'schedule': '30 8 * * *'
        }, 'orders': {
            'market': ["3379", "5574"],
            'schedule': '30 5 * * *'
        }, 'line_items': {
            'market': ["3379", "5574"],
            'schedule': '10 8 * * *'
        }, 'forecasts': {
            'market': ["3379"],
            'schedule': '15 7 * * *',
            'run_date': 'today'
        }, 'custom_targeting': {
            'market': ["3379"],
            'schedule': '30 12 * * *',
            'dependent_log_type': ['line_items']
        }, 'audience_segment': {
            'market': ["3379", "5574"],
            'schedule': '20 5 * * *'
        }, 'video_content_map': {
            'market': ["3379", "5574"],
            'schedule': '5 7 * * *'
        }, 'deals': {
            'market': ["3379", "5574"],
            'schedule': '15 5 * * *'
        }, 'creative': {
            'market': ["3379", "5574"],
            'schedule': '45 5 * * *'
        }, 'order_delivery_summary': {
            'market': ["3379", "5574"],
            'schedule': '5 7 * * *'
        }, 'video_content': {
            'market': ["3379", "5574"],
            'schedule': '10 15 * * *',
            'dependent_log_type': ['NetworkRequests', 'NetworkBackfillRequests']
        }, 'gam_revenue': {
            'market': ["5574"],
            'schedule': '0 0 * * *'
        }, 'gam_pfp_revenue': {
            'market': ["5574"],
            'schedule': '0 0 * * *'
        }, 'gam_adx_historical': {
            'market': ["5574"],
            'schedule': '0 0 * * *'
        }, 'user': {
            'market': ["3379", "5574"],
            'schedule': '40 5 * * *',
            'run_date': 'today'
        }, 'buyer_info_adserver': {
            'market': ["3379", "5574"],
            'schedule': '30 13 * * *'
        }, 'buyer_info_adx': {
            'market': ["3379", "5574"],
            'schedule': '40 13 * * *'
        }, 'line_item_currency_code': {
            'market': ["3379", "5574"],
            'schedule': '50 13 * * *'
        }

    }
    GAM_API_CONFIG_JSON = Variable.get("GAM_API_CONFIG_JSON", api_reports)


class GamAPIDataDogCommon(GamCommon):

    datadog_reports = {
        'market': ["5574"],
        'schedule': '0 0 * * *',
    }
    GAM_DATADOG_CONFIG_JSON = Variable.get("GAM_DATADOG_CONFIG_JSON", datadog_reports)


class GAMLogCommon(GamCommon):
    GAM_GLOBAL_SENSOR_DAG_SCHEDULE = '30 9 * * *'  # 3pm IST daily
    GAM_GLOBAL_PROCESS_OWNER = 'us_msridhar'

    GAM_CONNECTION = 'gam_connection'

    # GAM Logs File Format: [Type]_[Network ID]_[YYYYMMDD]_[HH].gz
    GAM_FILE_FORMAT = '{0}_{1}_{2}_{3}.gz'
    GAM_CORRECTED_FILE_FORMAT = '{0}_{1}_{2}_{3}_corrected.gz'

    network_id_and_bucket_name_by_market = {
        'US': {
            'gam_source_bucket': 'gdfp-3379',
            'network_id': 266335,
            'network_code': 3379
        },
        'Russia': {
            'gam_source_bucket': 'gdfp-45577845',
            'network_id': 625085,
            'network_code': 45577845
        },
        'International': {
            'gam_source_bucket': 'gdfp-5574',
            'network_id': 281237,
            'network_code': 5574
        }
    }

    gam_log_types = {
        network_id_and_bucket_name_by_market['International']['network_id']: {
            'NetworkClicks': 'active',
            'NetworkBackfillClicks': 'active',
            'NetworkImpressions': 'active',
            'NetworkBackfillImpressions': 'active',
            'NetworkRequests': 'active',
            'NetworkBackfillRequests': 'active',
            'NetworkCodeServes': 'active',
            'NetworkBackfillCodeServes': 'active',
            'NetworkBackfillBids': 'active',
            'NetworkActiveViews': 'active',
            'NetworkBackfillActiveViews': 'active'
        },
        network_id_and_bucket_name_by_market['US']['network_id']: {
            'NetworkClicks': 'active',
            'NetworkBackfillClicks': 'active',
            'NetworkImpressions': 'active',
            'NetworkBackfillImpressions': 'active',
            'NetworkRequests': 'active',
            'NetworkBackfillRequests': 'active',
            'NetworkCodeServes': 'active',
            'NetworkBackfillCodeServes': 'active',
            'NetworkVideoConversions': 'active',
            'NetworkBackfillVideoConversions': 'active',
            'NetworkActiveViews': 'active',
            'NetworkBackfillActiveViews': 'active',
            'NetworkActivities': 'active'

        },
        network_id_and_bucket_name_by_market['Russia']['network_id']: {
            'NetworkClicks': 'active',
            'NetworkBackfillClicks': 'active',
            'NetworkImpressions': 'active',
            'NetworkBackfillImpressions': 'active',
            'NetworkRequests': 'active',
            'NetworkBackfillRequests': 'active',
            'NetworkCodeServes': 'active',
            'NetworkBackfillCodeServes': 'active',
            'NetworkBackfillBids': 'active',
            'NetworkActiveViews': 'active',
            'NetworkBackfillActiveViews': 'active'
        }
    }

    databricks_instance_types = {
        625085: {
            'NetworkClicks': "r5.large",
            'NetworkBackfillClicks': "r5.large",
            'NetworkImpressions': "r5.large",
            'NetworkBackfillImpressions': "r5.large",
            'NetworkRequests': "r5.large",
            'NetworkBackfillRequests': "r5.large",
            'NetworkCodeServes': "r5.large",
            'NetworkBackfillCodeServes': "r5.large",
            'NetworkActiveViews': "r5.large",
            'NetworkBackfillActiveViews': "r5.large",
            'NetworkBackfillBids': "r5.large"
        },
        266335: {
            'NetworkClicks': "r5.large",
            'NetworkBackfillClicks': "r5.large",
            'NetworkImpressions': "r5.4xlarge",
            'NetworkBackfillImpressions': "r5.large",
            'NetworkRequests': "r5.4xlarge",
            'NetworkBackfillRequests': "r5.large",
            'NetworkCodeServes': "r5.4xlarge",
            'NetworkBackfillCodeServes': "r5.large",
            'NetworkVideoConversions': "r5.2xlarge",
            'NetworkBackfillVideoConversions': "r5.large",
            'NetworkActiveViews': "r5.2xlarge",
            'NetworkBackfillActiveViews': "r5.large",
            'NetworkActivities': "r5.large"
        },
        281237: {
            'NetworkClicks': "r5.large",
            'NetworkBackfillClicks': "r5.large",
            'NetworkImpressions': "r5.2xlarge",
            'NetworkBackfillImpressions': "r5.large",
            'NetworkRequests': "r5.2xlarge",
            'NetworkBackfillRequests': "r5.large",
            'NetworkCodeServes': "r5.2xlarge",
            'NetworkBackfillCodeServes': "r5.large",
            'NetworkActiveViews': "r5.large",
            'NetworkBackfillActiveViews': "r5.large",
            'NetworkBackfillBids': "r5.large"
        }
    }

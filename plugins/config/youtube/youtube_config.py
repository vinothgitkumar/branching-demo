import os
from abc import ABCMeta
from airflow.models import Variable


class YoutubeCommon(metaclass=ABCMeta):
    YOUTUBE_DAG_OWNER_NAME = os.getenv("YOUTUBE_DAG_OWNER_NAME", Variable.get('YOUTUBE_DAG_OWNER_NAME', 'us_ashok'))
    YOUTUBE_CREDS_PATH = Variable.get("YOUTUBE_CREDS_PATH", "/dbfs/FileStore/data_eng/creds/yt_cni-ca-dfp.json")
    YOUTUBE_CRED_SECRET_PATH = "secret/data-services/astronomer/prod/yt_global/rds/dev"

    # TODO with slack integration


class TardisConfig():
    CLIENT_JAR_PATH = Variable.get('TARDIS_CLIENT_JAR_PATH', 'https://tardis.conde.io/download/tardis-0.1.3'
                                                             '-py3-none-any.whl')


class YoutubeSilverCommon(YoutubeCommon):
    YOUTUBE_GLOBAL_PROCESS_OWNER = 'us_ashok'
    YOUTUBE_CONNECTION = 'youtube_gcp_connection'
    CONTENT_OWNER_ID_US = Variable.get('CONTENT_OWNER_ID_US')
    CONTENT_OWNER_ID_ALL = Variable.get('CONTENT_OWNER_ID_ALL')
    ENCRYPTION_KEY = Variable.get('ENCRYPTION_KEY')

    search_config = {
        "us": {'dataSourceId': 'youtube_content_owner', 'content_owner_id': CONTENT_OWNER_ID_US,
               'table_suffix': 'us'},
        "all": {'dataSourceId': 'youtube_content_owner', 'content_owner_id': CONTENT_OWNER_ID_ALL,
                'table_suffix': 'all'}
    }

    reports = ["content_owner_ad_rates",
               "content_owner_basic",
               "content_owner_demographics",
               "content_owner_combined",
               "content_owner_province",
               "content_owner_device_os",
               "content_owner_traffic_source",
               "content_owner_video_metadata",
               "content_owner_playlist_basic"]

    supplementary_reports = {
        "trending": "yt_trending_feed",
        "groups": "yt_groups",
        "group_content": "yt_group_contents"
    }

    old_reports = ["ad_rates_rpt",
                   "content_owner_basic_v3",
                   "content_owner_demographics",
                   "content_owner_device_rpt",
                   "content_owner_province_rpt"]

    trending_feed_schedule = "0 2,8,14,20 * * *"
    encryption_key = ENCRYPTION_KEY
    groups_cms = {"us": CONTENT_OWNER_ID_US, "all": CONTENT_OWNER_ID_ALL}
    backfill_date_range = 14
    transfer_actual_date = '2020-04-13'

import os
from airflow.models import Variable
from plugins.config.gam.gam_config import GAMLogCommon, GamAPICommon, GamAPIDataDogCommon
from plugins.config.youtube.youtube_config import YoutubeSilverCommon


# Values should be stored here when it may change and may be accessed in multiple places through the project. Values
# can be overridden from environment variables but may be given a sensible default

class AppConfig:
    environment = os.getenv("ENV", "default").lower()


class DatabricksConfig:
    host = os.getenv("DATABRICKS_HOST", "https://condenast-dev.cloud.databricks.com")
    token_key = os.getenv("DATABRICKS_TOKEN_KEY", "marquis_pat_dev_evergreen_workspace")
    workspace_conn_id = os.getenv("WORKSPACE_CONN_ID", "local_dev_workspace")


class DbtConfig:
    host = os.getenv("DBT_HOST", "https://cloud.getdbt.com/api/v2/")


class ExampleConfig:
    notebook_name = "/some/path/to/a/notebook"
    instance_profile = "this-may-need-to-be-generated-via-terraform-or-devops"


class NarrativConfig:
    clicks_notebook = os.getenv("NARRATIV_CLICKS_NOTEBOOK",
                                "/Repos/Staging/affiliate-data-integration/src/notebooks/narrativ/ingest_clicks")
    orders_notebook = os.getenv("NARRATIV_ORDERS_NOTEBOOK",
                                "/Repos/Staging/affiliate-data-integration/src/notebooks/narrativ/ingest_orders")
    instance_profile = os.getenv("NARRATIV_INSTANCE_PROFILE", "place-holder:not-created")


class SlackConfig:
    is_enabled = os.getenv("SLACK_IS_ENABLED", False)
    alert_channel = os.getenv("SLACK_ALERT_CHANNEL", "jesse_bot_testing_spectacular")
    failure_channel = os.getenv("SLACK_FAILURE_CHANNEL", "jesse_bot_testing_spectacular")
    token_key = os.getenv("SLACK_TOKEN_KEY", "slack_de_token")
    slack_de_token = os.environ.get("SLACK_DE_TOKEN", None)
    additional_alertee = []


class VaultConfig:
    token = os.getenv("VAULT_TOKEN")
    url = os.getenv("VAULT_ADDR")
    secrets_path = os.getenv("VAULT_SECRETS_PATH", "secret/data-services/astronomer/prod/data-eng")


class GoogleAnalytics(SlackConfig):
    notebook_bronze = Variable.get('GA_BRONZE_NOTEBOOK', '/Repos/mehulkumar_piruka@condenast.com/dev-google-data'
                                                         '-integration/src/notebooks/google_analytics/ingestion_bronze')
    notebook_silver = Variable.get('GA_SILVER_NOTEBOOK', '/Repos/mehulkumar_piruka@condenast.com/dev-google-data'
                                                         '-integration/src/notebooks/google_analytics/ingestion_silver')
    notebook_backfill = Variable.get('GA_BACKFILL_NOTEBOOK', '/Repos/mehulkumar_piruka@condenast.com/dev-stats-google'
                                                             '-data-integration/src/notebooks/google_analytics'
                                                             '/backfill_scripts/count_update_tardis')
    load_stats = Variable.get('LOAD_STATS_NOTEBOOK', '/Repos/mehulkumar_piruka@condenast.com/dev-stats-google-data'
                                                     '-integration/src/notebooks/google_analytics'
                                                     '/load_stats')

    alert_channel = '#de-google-alerts-fail'
    failure_channel = '#de-google-alerts-fail'
    additional_alertee = Variable.get('GA_ADDITIONAL_ALERTEE', [])


class GoogleAnalyticsOIDC(GoogleAnalytics):
    project_info_project_id = 229339819
    project_info_bq_project = "titanium-cacao-204116"
    project_info_subscription = "GA_oidc_service_dev_subscription"
    tardis_data_source = 'google_analytics_oidc_service_data'
    tardis_process_source = 'google_analytics_oidc_service_process'
    gcp_bq_cred = Variable.get('GA_BQ_OIDC_CRED_FILE', '/dbfs/FileStore/data_eng/creds/ga_bq_creds.json')
    gcp_pubsub_cred = Variable.get('GA_PUBSUB_OIDC_CRED_FILE', 'secret/data-services/astronomer/prod/ga_oidc_pubsub')
    gcp_connection_id = Variable.get('GA_OIDC_CONNECTION_ID', 'ga_oidc_pubsub_connection')


class GoogleAnalyticsGlobal(GoogleAnalytics):
    project_info_project_id = 140208876
    project_info_bq_project = "cni-ca-dfp"
    project_info_subscription = "GA_Global_subscription_dev"
    tardis_data_source = 'ga_global_web_data'
    tardis_process_source = 'ga_global_web_process'
    gcp_bq_cred = Variable.get('GA_BQ_GLOBAL_CRED_FILE', "/dbfs/FileStore/data_eng/creds/ga-global-cni-ca-dfp.json")
    gcp_pubsub_cred = Variable.get('GA_PUBSUB_GLOBAL_CRED_FILE',
                                   'secret/data-services/astronomer/prod/ga_global_pubsub')
    gcp_connection_id = Variable.get('GA_GLOBAL_CONNECTION_ID', 'ga_global_pubsub_connection')
    brand_mapping_notebook = Variable.get('BRAND_MAPPING_NOTEBOOK',
                                          '/Repos/mehulkumar_piruka@condenast.com/dev-stats-google-data-integration/src/notebooks/google_analytics'
                                          '/brand_mapping')
    mgmt_api_tardis_process_source = 'ga_custom_metadata_process'
    mgmt_api_notebook_bronze = Variable.get('GA_MGMT_API_BRONZE_NOTEBOOK', '/Repos/mehulkumar_piruka@condenast.com'
                                                                           '/dev-google-data-integration/src'
                                                                           '/notebooks/google_analytics/management_api/ga_custom_bronze')
    mgmt_api_notebook_silver = Variable.get('GA_MGMT_API_SILVER_NOTEBOOK', '/Repos/mehulkumar_piruka@condenast.com'
                                                                           '/dev-google-data-integration/src'
                                                                           '/notebooks/google_analytics/management_api/ga_custom_silver')


class ParselyConfig:
    parsely_bronze_notebook = os.getenv("PARSELY_BRONZE_NOTEBOOK", "/Repos/minjia_zhan@condenast.com/engagement-data"
                                                                   "-integration/src/notebooks/parsely/ingest_parsely")
    parsely_silver_notebook = os.getenv("PARSELY_SILVER_NOTEBOOK", "/Repos/minjia_zhan@condenast.com/engagement-data"
                                                                   "-integration/src/notebooks/parsely"
                                                                   "/ingest_parsely_silver")
    parsely_optimization_notebook = os.getenv("PARSELY_OPTIMIZATION_NOTEBOOK",
                                              "/Repos/minjia_zhan@condenast.com/engagement-data-integration/src"
                                              "/notebooks/parsely "
                                              "/optimize_parsely")
    parsely_backfill_notebook = os.getenv("PARSELY_BACKFILL_NOTEBOOK",
                                          "/Repos/minjia_zhan@condenast.com/engagement-data-integration/src"
                                          "/notebooks/parsely"
                                          "/backfill_parsely")
    instance_profile = os.getenv("PARSELY_INSTANCE_PROFILE", "arn:aws:iam::930908212222:instance-profile/data-eng"
                                                             "-team-role-instance-profile")


class ChartableConfig:
    chartable_notebook = os.getenv("CHARTABLE_NOTEBOOK", "/Repos/minjia_zhan@condenast.com/enterprise-data-integration"
                                                         "/src/notebooks/chartable"
                                                         "/daily_ingest")
    instance_profile = os.getenv("CHARTABLE_INSTANCE_PROFILE", "arn:aws:iam::930908212222:instance-profile/data-eng"
                                                               "-team-role-instance-profile")


class GamAPIConfig(GamAPICommon, SlackConfig):
    alert_channel = '#de-google-alerts-fail'
    failure_channel = '#de-google-alerts-fail'
    additional_alertee = Variable.get("GAM_ADDITIONAL_ALERTEE", [])
    api_bronze_notebook = Variable.get("GAM_API_BRONZE_NOTEBOOK", "/Repos/Tara_Devi@condenast.com/"
                                                                  "google-data-integration/src/notebooks/gam_global/api/"
                                                                  "driver_bronze")

    api_silver_notebook = Variable.get("GAM_API_SILVER_NOTEBOOK", "/Repos/Tara_Devi@condenast.com/"
                                                                  "google-data-integration/src/notebooks/gam_global/api/"
                                                                  "driver_silver")

    backfill_notebook = Variable.get("GAM_API_BACKFILL_NOTEBOOK", "/Repos/Sivanandhini_Kumar@condenast.com/google-data"
                                                                  "-integration/src/notebooks/gam_global/api"
                                                                  "/backfill_driver")
    line_item_mapping_notebook = Variable.get("GAM_LINE_ITEM_MAPPING_NOTEBOOK",
                                              "/Repos/Manoj_Sridhar@condenast.com/google-data-integration/src/notebooks"
                                              "/gam_global/gold/custom_targeting_mapping")


class GamAPIdataDog(GamAPIDataDogCommon, SlackConfig):
    alert_channel = '#de-google-alerts-fail'
    failure_channel = '#de-google-alerts-fail'
    additional_alertee = Variable.get("GAM_ADDITIONAL_ALERTEE", [])
    datadog_bronze_notebook = Variable.get("GAM_API_DD_BRONZE_NOTEBOOK", "/Repos/Tara_Devi@condenast.com/"
                                                                         "google-data-integration/src/notebooks/gam_global/api/driver_datadog_bronze")

    datadog_silver_notebook = Variable.get("GAM_API_DD_SILVER_NOTEBOOK", "/Repos/Tara_Devi@condenast.com/"
                                                                         "google-data-integration/src/notebooks/gam_global/api/driver_datadog_silver")


class GAMLogConfig(GAMLogCommon, SlackConfig):
    logs_bronze_notebook = Variable.get("GAM_LOG_BRONZE_NOTEBOOK",
                                        "/Repos/Manoj_Sridhar@condenast.com/google-data-integration/"
                                        "src/notebooks/gam_global/logs/driver_bronze")

    logs_silver_notebook = Variable.get("GAM_LOG_SILVER_NOTEBOOK",
                                        "/Repos/Manoj_Sridhar@condenast.com/google-data-integration/"
                                        "src/notebooks/gam_global/logs/ingestion_silver")
    additional_alertee = ['@us_skumar2']


class SparrowConfig():
    sparrow_silver_rt_notebook = Variable.get("SPARROW_SILVER_RT_NOTEBOOK",
                                              "/Repos/Sooriyadeeban_Sekar@condenast.com/engagement-data-integration/src/notebooks/"
                                              "sparrow_silver/sparrow_silver_rt")
    sparrow_silver_hourly_agg_notebook = Variable.get("SPARROW_SILVER_HOURLY_AGG_NOTEBOOK",
                                                      "/Repos/Sooriyadeeban_Sekar@condenast.com/engagement-data-integration/src/notebooks/"
                                                      "sparrow_silver/sparrow_silver_hourly_agg")
    sparrow_silver_backfill_notebook = Variable.get("SPARROW_SILVER_BACKFILL_NOTEBOOK",
                                                    "/Repos/Sooriyadeeban_Sekar@condenast.com/engagement-data-integration/src/notebooks/"
                                                    "sparrow_silver/sparrow_silver_backfill")


class TrackonomicsConfig():
    instance_profile = os.getenv("TRACKONOMICS_INSTANCE_PROFILE",
                                 "arn:aws:iam::930908212222:instance-profile/"
                                 "data-eng-team-role-instance-profile")

    daily_bronze_notebook = os.getenv("DAILY_BRONZE_NOTEBOOK",
                                      "/Repos/Dinakar_Sundar@condenast.com/affiliate-data-integration/src/notebooks/"
                                      "trx/daily/ingest_bronze")

    daily_silver_notebook = os.getenv("DAILY_SILVER_NOTEBOOK",
                                      "/Repos/Dinakar_Sundar@condenast.com/affiliate-data-integration/src/notebooks/"
                                      "trx/daily/ingest_silver")

    funnel_bronze_notebook = os.getenv("FUNNEL_BRONZE_NOTEBOOK",
                                       "/Repos/Dinakar_Sundar@condenast.com/affiliate-data-integration/src/notebooks/"
                                       "trx/funnel_relay/ingest_bronze")

    funnel_silver_notebook = os.getenv("FUNNEL_SILVER_NOTEBOOK",
                                       "/Repos/Dinakar_Sundar@condenast.com/affiliate-data-integration/src/notebooks/"
                                       "trx/funnel_relay/ingest_silver")

    trns_bronze_notebook = os.getenv("TRNS_BRONZE_NOTEBOOK",
                                     "/Repos/Dinakar_Sundar@condenast.com/affiliate-data-integration/src/notebooks/"
                                     "trx/transactions/ingest_bronze")

    trns_silver_notebook = os.getenv("TRNS_SILVER_NOTEBOOK",
                                     "/Repos/Dinakar_Sundar@condenast.com/affiliate-data-integration/src/notebooks/"
                                     "trx/transactions/ingest_silver")

    vendor_s3_bucket = 'trx-cl-condenast'

    funnel_rly_tardis_source = 'affiliate.trx_funnel_relay'

    trans_tardis_source = 'affiliate.trx_transactions'

    daily_tardis_source = 'affiliate.trx_daily_by_merchant'

    daily_src_s3 = 'daily/daily_by_merchant_{}.csv'

    funnel_relay_src_s3 = 'reports/funnel_relay_{}.csv'

    trans_src_s3 = 'transactions/transactions_{}.csv'


class YoutubeRevShareConfig:
    notebook_name = os.getenv('YOUTUBE_REVSHARE_NOTEBOOK',
                              '/Repos/Development/yt-revshare-data-integration/src/yt_revshare_ingest')


class MegaphoneS3Config:
    megaphone_s3_bronze_notebook = os.getenv('MEGAPHONE_S3_BRONZE_NOTEBOOK',
                                             '/Repos/Karan_ShridharMudaliar@condenast.com/enterprise-data-integration/src/'
                                             'notebooks/cne_megaphone/megaphone_s3_load/ingest_bronze')

    megaphone_s3_silver_notebook = os.getenv('MEGAPHONE_S3_SILVER_NOTEBOOK',
                                             '/Repos/Karan_ShridharMudaliar@condenast.com/enterprise-data-integration/src/'
                                             'notebooks/cne_megaphone/megaphone_s3_load/ingest_silver')

    file_prefix = ['impression', 'metrics']

    file_suffix = '.json.gz'


class MegaphoneAPIConfig:
    megaphone_API_bronze_notebook = os.getenv('MEGAPHONE_S3_BRONZE_NOTEBOOK',
                                              '/Repos/Karan_ShridharMudaliar@condenast.com/enterprise-data-integration/src/'
                                              'notebooks/cne_megaphone/megaphone_api_load/ingest_bronze')

    megaphone_API_silver_notebook = os.getenv('MEGAPHONE_S3_SILVER_NOTEBOOK',
                                              '/Repos/Karan_ShridharMudaliar@condenast.com/enterprise-data-integration/src/'
                                              'notebooks/cne_megaphone/megaphone_api_load/ingest_silver')

    megaphone_API_back_fill_notebook = os.getenv('MEGAPHONE_API_BACKFILL_NOTEBOOK',
                                                 '/Repos/Karan_ShridharMudaliar@condenast.com/enterprise-data'
                                                 '-integration/src/notebooks/cne_megaphone/megaphone_api_load'
                                                 '/megaphone_api_backfill')


class AmazonAffiliatesConfig:
    ingestion_notebook_name = os.getenv('AMAZON_AFFILIATE_BRONZE_NOTEBOOK',
                                        '/Repos/anandhakumar_r@condenast.com/affiliate-data-integration/src/notebooks/amazon/ingest')

    missing_report_notebook_name = os.getenv('AMAZON_AFFILIATE_MISSING_REPORT_NOTEBOOK',
                                             '/Repos/anandhakumar_r@condenast.com/affiliate-data-integration/src/notebooks/amazon/process_missing_reports')

    date_migration_notebook_name = os.getenv('AMAZON_AFFILIATE_MIGRATION_NOTEBOOK',
                                             '/Repos/anandhakumar_r@condenast.com/affiliate-data-integration/src/notebooks/amazon/affiliates_data_migration')


class CneVideosConfig:
    cne_videos_notebook = os.getenv("CNE_VIDEOS_NOTEBOOK", "/Repos/aman_choudhary@condenast.com/"
                                                           "enterprise-data-integration/src/"
                                                           "notebooks/cne/daily_report_ingestion")
    instance_profile = os.getenv("CNE_VIDEOS_INSTANCE_PROFILE", "arn:aws:iam::930908212222:instance-profile/"
                                                                "data-eng-team-role-instance-profile")


class GoogleSeoConfig:
    seo_silver_notebook = os.getenv("SEO_SILVER_NOTEBOOK", "/Repos/rajendar_reddybaradhi@condenast.com/"
                                                           "enterprise-data-integration/src/notebooks/"
                                                           "seo/seo_silver")

    seo_connectors = ['cn_dse_seo_gqindia', 'cn_dse_seo_pitchfork']


class TwitterConfig:
    twitter_notebook_path = Variable.get("TWITTER_NOTEBOOK_PATH",
                                         "/Repos/Rajendar_ReddyBaradhi@condenast.com/enterprise-data-integration/"
                                         "src/notebooks/"
                                         "twitter_follower_report/followers_report")


class OktaConfig:
    okta_notebook_path = Variable.get("OKTA_NOTEBOOK_PATH",
                                      "/Repos/Rajendar_ReddyBaradhi@condenast.com/enterprise-data-integration"
                                      "/src/notebooks/okta/okta_silver_process")
    tardis_data_source = 'EDW_LOAD.LD_SF_AD_ALL_USERS'


class YoutubeGlobal(YoutubeSilverCommon, SlackConfig):
    project_info_bq_project = "cni-ca-dfp"
    project_info_subscription = {"us": "yt_global_dev", "all": "yt_global_all_dev"}
    gcp_connection_id = Variable.get('YOUTUBE_GLOBAL_CONNECTION_ID', 'youtube_gcp_connection')
    yt_silver_notebook = Variable.get("YOUTUBE_SILVER_NOTEBOOK",
                                      "/Repos/AshokKumar_Baluchamy@condenast.com/google-data-integration_1/"
                                      "src/notebooks/youtube/ingestion_silver")
    gcp_pubsub_cred = "secret/data-services/astronomer/prod/yt_global/gcp"
    yt_backfill_notebook = Variable.get("YOUTUBE_BACKFILL_NOTEBOOK",
                                        "/Repos/AshokKumar_Baluchamy@condenast.com/google-data-integration_1/"
                                        "src/notebooks/youtube/backfill/youtube_backfill")
    yt_lookup_load_notebook = Variable.get("YOUTUBE_LOOKUP_NOTEBOOK",
                                           "/Repos/AshokKumar_Baluchamy@condenast.com/google-data-integration_1/"
                                           "src/notebooks/youtube/lookup_load")
    yt_trending_feed_bronze_notebook = Variable.get("YOUTUBE_TRENDING_FEED_NOTEBOOK",
                                                    "/Repos/AshokKumar_Baluchamy@condenast.com/google-data-integration_1/"
                                                    "src/notebooks/youtube/trending_feed/yt_trending_feed")
    yt_trending_feed_silver_notebook = Variable.get("YOUTUBE_TRENDING_FEED_NOTEBOOK",
                                                    "/Repos/AshokKumar_Baluchamy@condenast.com/google-data-integration_1/"
                                                    "src/notebooks/youtube/trending_feed/yt_trending_feed_silver")
    yt_groups_bronze_notebook = Variable.get("YOUTUBE_GROUPS_NOTEBOOK",
                                             "/Repos/AshokKumar_Baluchamy@condenast.com/google-data-integration_1/"
                                             "src/notebooks/youtube/group_content/yt_groups")
    yt_groups_silver_notebook = Variable.get("YOUTUBE_GROUPS_NOTEBOOK",
                                             "/Repos/AshokKumar_Baluchamy@condenast.com/google-data-integration_1/"
                                             "src/notebooks/youtube/group_content/yt_groups_silver")
    yt_group_content_bronze_notebook = Variable.get("YOUTUBE_GROUP_CONTENT_NOTEBOOK",
                                                    "/Repos/AshokKumar_Baluchamy@condenast.com/google-data-integration_1/"
                                                    "src/notebooks/youtube/group_content/yt_group_contents")
    yt_group_content_silver_notebook = Variable.get("YOUTUBE_GROUP_CONTENT_NOTEBOOK",
                                                    "/Repos/AshokKumar_Baluchamy@condenast.com/google-data-integration_1/"
                                                    "src/notebooks/youtube/group_content/yt_group_contents_silver")
    yt_missing_data_backfill_notebook = Variable.get("YOUTUBE_MISSING_DATA_BACKFILL_NOTEBOOK",
                                                     "/Repos/AshokKumar_Baluchamy@condenast.com/google-data-integration_1/"
                                                     "src/notebooks/youtube/backfill/missing_data_backfill")
    CONTENT_OWNER_ID_US = ""
    CONTENT_OWNER_ID_ALL = ""
    ENCRYPTION_KEY = ""


class AmazonUKEmeaAffiliatesConfig:
    daily_revenue_ingestion_notebook_name = os.getenv('AMAZON_UK_EMEA_AFFILIATE_SILVER_NOTEBOOK',
                                                      '/Repos/Oliver.Bradley@condenast.com/affiliate-data-integration'
                                                      '/src/notebooks/amazon/ingest_uk_emea_daily_revenue')

    content_insights_ingestion_notebook_name = os.getenv('AMAZON_UK_EMEA_AFFILIATE_CONTENT_INSIGHTS_SILVER_NOTEBOOK',
                                                         '/Repos/Oliver.Bradley@condenast.com/affiliate-data-integration'
                                                         '/src/notebooks/amazon/ingest_uk_emea_content_insights')

    daily_clicks_ingestion_notebook_name = os.getenv('AMAZON_UK_EMEA_AFFILIATE_CONTENT_INSIGHTS_SILVER_NOTEBOOK',
                                                     '/Repos/Oliver.Bradley@condenast.com/affiliate-data-integration'
                                                     '/src/notebooks/amazon/ingest_uk_emea_daily_clicks')

    missing_report_notebook_name = os.getenv('AMAZON_UK_EMEA_AFFILIATE_MISSING_REPORT_NOTEBOOK',
                                             '/Repos/Oliver.Bradley@condenast.com/affiliate-data-integration'
                                             '/src/notebooks/amazon/process_uk_emea_missing_content_insights_reports')


class ImpactAffiliatesConfig:
    ingestion_notebook_name = os.getenv('IMPACT_AFFILIATE_SILVER_NOTEBOOK',
                                        '/Repos/Oliver.Bradley@condenast.com/affiliate-data-integration/src/notebooks/impact/ingest_impact')


class StaqConfig:
    aws_s3_access_key_name = 'evergreen_airflow_s3_access_key'
    aws_s3_secret_key_name = 'evergreen_airflow_s3_secret_access_key'
    vendor_conf = {"triple-lift": "triple_lift", "ValidationFiles": "staq_pmp_push", "spotx": "spotx", "rubicon": "rubicon",
                   "IndexExchangeV2": "index_exchange", "app-nexus": "appnexus"}
    src_bucket = "cn-dse-staq-dev"
    tgt_bucket = "cn-dse-staq-dev"
    staq_raw_files_copy_notebook = '/Users/Sooriyadeeban_Sekar@condenast.com/Staq/src/staq_copy_data'
    staq_raw_data_load_notebook = '/Users/Sooriyadeeban_Sekar@condenast.com/Staq/src/staq_driver'
    staq_backfill_notebook = '/Users/Sooriyadeeban_Sekar@condenast.com/Staq/src/staq_backfill'


class EncoreConfig:
    mongo_notebook = os.getenv("mongo_notebook", "/Repos/Dinesh.Murugesan@condenast.com/"
                                                 "dbt-content/"
                                                 "databricks/notebooks/encore/mongo_api_ingest")
    bq_notebook = os.getenv("mongo_notebook", "/Repos/Dinesh.Murugesan@condenast.com/"
                                              "dbt-content/"
                                              "databricks/notebooks/encore/bq_encore_export")
    instance_profile = os.getenv("encore_profile", "arn:aws:iam::930908212222:instance-profile/"
                                                   "data-eng-team-role-instance-profile")


class SkimlinksUKEmeaAffiliatesConfig:
    daily_revenue_ingestion_notebook_name = os.getenv('SKIMLINKS_UK_EMEA_DAILY_REVENUE_SILVER_NOTEBOOK',
                                                      '/Repos/Oliver.Bradley@condenast.com/affiliate-data-integration'
                                                      '/src/notebooks/skimlinks/ingest_uk_emea_daily_revenue')
    commission_ingestion_notebook_name = os.getenv('SKIMLINKS_UK_EMEA_COMMISSION_SILVER_NOTEBOOK',
                                                   '/Repos/Oliver.Bradley@condenast.com/affiliate-data-integration'
                                                   '/src/notebooks/skimlinks/ingest_uk_emea_daily_commission')
    instance_profile = os.getenv("SKIMLINKS_PROFILE", "arn:aws:iam::930908212222:instance-profile/"
                                                      "data-eng-team-role-instance-profile")


class WebgearsUKEmeaAffiliatesConfig:
    ingestion_notebook_name = os.getenv("WEBGEARS_UK_EMEA_COMMISSION_SILVER_NOTEBOOK",
                                        "/Repos/Oliver.Bradley@condenast.com/affiliate-data-integration"
                                        "/src/notebooks/webgears/ingest_uk_emea")

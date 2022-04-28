import os
from airflow.models import Variable
from plugins.config.gam.gam_config import GAMLogCommon, GamAPICommon, GamAPIDataDogCommon
from plugins.config.youtube.youtube_config import YoutubeSilverCommon


# Values should be stored here when it may change and may be accessed in multiple places through the project. Values
# can be overridden from environment variables but may be given a sensible default

class AppConfig:
    environment = os.getenv("ENV", "default").lower()


class DatabricksConfig:
    host = os.getenv("DATABRICKS_HOST", "https://condenast-prod.cloud.databricks.com")
    token_key = os.getenv("DATABRICKS_TOKEN_KEY", "marquis_pat_production_evergreen_workspace")
    workspace_conn_id = os.getenv("WORKSPACE_CONN_ID", "databricks_prod_workspace")


class DbtConfig:
    host = os.getenv("DBT_HOST", "https://cloud.getdbt.com/api/v2/")


class ExampleConfig:
    notebook_name = "/some/path/to/a/notebook"
    instance_profile = "this-may-need-to-be-generated-via-terraform-or-devops"


class NarrativConfig:
    clicks_notebook = os.getenv("NARRATIV_CLICKS_NOTEBOOK", "/Repos/Production/affiliate-data-integration/src/notebooks/narrativ/ingest_clicks")
    orders_notebook = os.getenv("NARRATIV_ORDERS_NOTEBOOK", "/Repos/Production/affiliate-data-integration/src/notebooks/narrativ/ingest_orders")
    instance_profile = os.getenv("NARRATIV_INSTANCE_PROFILE", "arn:aws:iam::930908212222:instance-profile/narrative-prod-deployment-role-instance-profile")


class SlackConfig:
    is_enabled = os.getenv("SLACK_IS_ENABLED", True)
    alert_channel = os.getenv("SLACK_ALERT_CHANNEL", "de-alerts")
    failure_channel = os.getenv("SLACK_FAILURE_CHANNEL", "de-alert-fails")
    token_key = os.getenv("SLACK_TOKEN_KEY", "slack_de_token")
    slack_de_token = os.environ.get("SLACK_DE_TOKEN", None)
    additional_alertee = []


class VaultConfig:
    token = os.getenv("VAULT_TOKEN")
    url = os.getenv("VAULT_ADDR")
    secrets_path = os.getenv("VAULT_SECRETS_PATH", "secret/data-services/astronomer/prod/data-eng")


class GoogleAnalytics(SlackConfig):
    notebook_bronze = Variable.get('GA_BRONZE_NOTEBOOK', '/Repos/Production/google-data-integration/src/notebooks'
                                                         '/google_analytics/ingestion_bronze')
    notebook_silver = Variable.get('GA_SILVER_NOTEBOOK', '/Repos/Production/google-data-integration/src/notebooks'
                                                         '/google_analytics/ingestion_silver')
    notebook_backfill = Variable.get('GA_BACKFILL_NOTEBOOK', '/Repos/Production/google-data-integration/src/notebooks'
                                                             '/google_analytics/backfill_scripts/count_update_tardis')
    load_stats = Variable.get('LOAD_STATS_NOTEBOOK', '/Repos/Production/google-data-integration/src/notebooks'
                                                     '/google_analytics/load_stats')
    alert_channel = '#de-alerts'
    failure_channel = '#de-alert-fails'
    additional_alertee = Variable.get('GA_ADDITIONAL_ALERTEE', ['@us_ktm', '<!subteam^S01G4D9Q6HE>'])


class GoogleAnalyticsOIDC(GoogleAnalytics):
    project_info_project_id = 229339819
    project_info_bq_project = "titanium-cacao-204116"
    project_info_subscription = "GA_oidc_service_E2_subscription"
    tardis_data_source = 'google_analytics_oidc_service_data'
    tardis_process_source = 'google_analytics_oidc_service_process'
    gcp_bq_cred = Variable.get('GA_BQ_OIDC_CRED_FILE', '/dbfs/FileStore/data_eng/creds/ga_bq_creds.json')
    gcp_pubsub_cred = Variable.get('GA_PUBSUB_OIDC_CRED_FILE', 'secret/data-services/astronomer/prod/ga_oidc_pubsub')
    gcp_connection_id = Variable.get('GA_OIDC_CONNECTION_ID', 'ga_oidc_pubsub_connection')


class GoogleAnalyticsGlobal(GoogleAnalytics):
    project_info_project_id = 140208876
    project_info_bq_project = "cni-ca-dfp"
    project_info_subscription = "GA_Global_subscription"
    tardis_data_source = 'ga_global_web_data'
    tardis_process_source = 'ga_global_web_process'
    gcp_bq_cred = Variable.get('GA_BQ_GLOBAL_CRED_FILE', '/dbfs/FileStore/data_eng/creds/ga-global-cni-ca-dfp.json')
    gcp_pubsub_cred = Variable.get('GA_PUBSUB_GLOBAL_CRED_FILE', 'secret/data-services/astronomer/prod/ga_global_pubsub')
    gcp_connection_id = Variable.get('GA_GLOBAL_CONNECTION_ID', 'ga_global_pubsub_connection')
    mgmt_api_tardis_process_source = 'ga_custom_metadata_process'
    brand_mapping_notebook = Variable.get('BRAND_MAPPING_NOTEBOOK',
                                          '/Repos/Production/google-data-integration/src/notebooks/google_analytics'
                                          '/brand_mapping')
    mgmt_api_notebook_bronze = Variable.get('GA_MGMT_API_BRONZE_NOTEBOOK', '/Repos/Production/google-data-integration'
                                                                           '/src/notebooks/google_analytics'
                                                                           '/management_api/ga_custom_bronze')
    mgmt_api_notebook_silver = Variable.get('GA_MGMT_API_SILVER_NOTEBOOK', '/Repos/Production/google-data-integration'
                                                                           '/src/notebooks/google_analytics'
                                                                           '/management_api/ga_custom_silver')


class ParselyConfig:
    parsely_bronze_notebook = os.getenv("PARSELY_BRONZE_NOTEBOOK", "/Repos/Production/engagement-data"
                                                                   "-integration/src/notebooks/parsely/ingest_parsely")
    parsely_silver_notebook = os.getenv("PARSELY_SILVER_NOTEBOOK", "/Repos/Production/engagement-data"
                                                                   "-integration/src/notebooks/parsely"
                                                                   "/ingest_parsely_silver")
    parsely_optimization_notebook = os.getenv("PARSELY_OPTIMIZATION_NOTEBOOK",
                                              "/Repos/Production/engagement-data-integration/src/notebooks/parsely"
                                              "/optimize_parsely")
    parsely_backfill_notebook = os.getenv("PARSELY_BACKFILL_NOTEBOOK",
                                          "/Repos/Production/engagement-data-integration/src/notebooks/parsely"
                                          "/backfill_parsely")
    instance_profile = os.getenv("PARSELY_INSTANCE_PROFILE", "arn:aws:iam::930908212222:instance-profile/parsely-prod"
                                                             "-deployment-role-instance-profile")


class ChartableConfig:
    chartable_notebook = os.getenv("CHARTABLE_NOTEBOOK", "/Repos/Production/enterprise-data-integration"
                                                         "/src/notebooks/chartable"
                                                         "/daily_ingest")
    instance_profile = os.getenv("CHARTABLE_INSTANCE_PROFILE", "arn:aws:iam::930908212222:instance-profile/chartable"
                                                               "-prod-deployment-role-instance-profile")


class GamAPIConfig(GamAPICommon, SlackConfig):

    alert_channel = '#de-alerts'
    failure_channel = '#de-alert-fails'
    additional_alertee = Variable.get("GAM_ADDITIONAL_ALERTEE",
                                      ['@us_ktm', '@us_tdevi', '@us_gsakthi', '<!subteam^S01G4D9Q6HE>'])

    api_bronze_notebook = Variable.get("GAM_API_BRONZE_NOTEBOOK", "/Repos/Production/google-data-integration/src/notebooks/"
                                                                  "gam_global/api/driver_bronze")

    api_silver_notebook = Variable.get("GAM_API_SILVER_NOTEBOOK", "/Repos/Production/google-data-integration/src/notebooks/"
                                                                  "gam_global/api/driver_silver")

    backfill_notebook = Variable.get("GAM_API_BACKFILL_NOTEBOOK", "/Repos/Production/google-data"
                                                                  "-integration/src/notebooks/gam_global/api"
                                                                  "/backfill_driver")
    line_item_mapping_notebook = Variable.get("GAM_LINE_ITEM_MAPPING_NOTEBOOK",
                                              "/Repos/Production/google-data-integration/src/notebooks"
                                              "/gam_global/gold/custom_targeting_mapping")


class GamAPIdataDog(GamAPIDataDogCommon, SlackConfig):
    alert_channel = '#de-alerts'
    failure_channel = '#de-alert-fails'
    additional_alertee = Variable.get("GAM_ADDITIONAL_ALERTEE", ['@us_ktm', '@us_tdevi', '<!subteam^S01G4D9Q6HE>'])
    datadog_bronze_notebook = Variable.get("GAM_API_DD_BRONZE_NOTEBOOK", "/Repos/Production/google-data-integration/src/"
                                                                         "notebooks/gam_global/api/driver_datadog_bronze")

    datadog_silver_notebook = Variable.get("GAM_API_DD_SILVER_NOTEBOOK", "/Repos/Production/google-data-integration/src/"
                                                                         "notebooks/gam_global/api/driver_datadog_silver")


class GAMLogConfig(GAMLogCommon, SlackConfig):
    logs_bronze_notebook = Variable.get("GAM_LOG_BRONZE_NOTEBOOK",
                                        "/Repos/Production/google-data-integration/"
                                        "src/notebooks/gam_global/logs/driver_bronze")

    logs_silver_notebook = Variable.get("GAM_LOG_SILVER_NOTEBOOK",
                                        "/Repos/Production/google-data-integration/"
                                        "src/notebooks/gam_global/logs/ingestion_silver")
    alert_channel = '#de-alerts'
    failure_channel = '#de-alert-fails'
    additional_alertee = ['@us_ktm', '<!subteam^S01G4D9Q6HE>']


class SparrowConfig():
    sparrow_silver_rt_notebook = Variable.get("SPARROW_SILVER_RT_NOTEBOOK",
                                              "/Repos/Production/engagement-data-integration/src/notebooks/"
                                              "sparrow_silver/sparrow_silver_rt")
    sparrow_silver_hourly_agg_notebook = Variable.get("SPARROW_SILVER_HOURLY_AGG_NOTEBOOK",
                                                      "/Repos/Production/engagement-data-integration/src/notebooks/"
                                                      "sparrow_silver/sparrow_silver_hourly_agg")
    sparrow_silver_backfill_notebook = Variable.get("SPARROW_SILVER_BACKFILL_NOTEBOOK",
                                                    "/Repos/Production/engagement-data-integration/src/notebooks/"
                                                    "sparrow_silver/sparrow_silver_backfill")


class SocialflowConfig:
    socialflow_silver_notebook = os.getenv("SOCIALFLOW_SILVER_NOTEBOOK",
                                           "/Repos/Production/enterprise-data-integration/src/notebooks/"
                                           "socialflow/socialflow_silver")

    socialflow_backfill_notebook = os.getenv("SOCIALFLOW_BACKFILL_NOTEBOOK",
                                             "/Repos/Production/enterprise-data-integration/src/notebooks/"
                                             "socialflow/backfill")

    socialflow_connector_list = ['cn_dse_custom_socialflow_facebook',
                                 'cn_dse_custom_socialflow_instagram',
                                 'cn_dse_custom_socialflow_linkedin',
                                 'cn_dse_custom_socialflow_pinterest',
                                 'cn_dse_custom_socialflow_twitter'
                                 ]


class TrackonomicsConfig:
    instance_profile = os.getenv("TRACKONOMICS_INSTANCE_PROFILE",
                                 "arn:aws:iam::930908212222:instance-profile/"
                                 "affiliate-prod-deployment-role-instance-profile")

    daily_bronze_notebook = os.getenv("DAILY_BRONZE_NOTEBOOK",
                                      "/Repos/Production/affiliate-data-integration/src/notebooks/"
                                      "trx/daily/ingest_bronze")

    daily_silver_notebook = os.getenv("DAILY_SILVER_NOTEBOOK",
                                      "/Repos/Production/affiliate-data-integration/src/notebooks/"
                                      "trx/daily/ingest_silver")

    funnel_bronze_notebook = os.getenv("FUNNEL_BRONZE_NOTEBOOK",
                                       "/Repos/Production/affiliate-data-integration/src/notebooks/"
                                       "trx/funnel_relay/ingest_bronze")

    funnel_silver_notebook = os.getenv("FUNNEL_SILVER_NOTEBOOK",
                                       "/Repos/Production/affiliate-data-integration/src/notebooks/"
                                       "trx/funnel_relay/ingest_silver")

    trns_bronze_notebook = os.getenv("TRNS_BRONZE_NOTEBOOK",
                                     "/Repos/Production/affiliate-data-integration/src/notebooks/"
                                     "trx/transactions/ingest_bronze")

    trns_silver_notebook = os.getenv("TRNS_SILVER_NOTEBOOK",
                                     "/Repos/Production/affiliate-data-integration/src/notebooks/"
                                     "trx/transactions/ingest_silver")

    vendor_s3_bucket = 'trx-cl-condenast'

    funnel_rly_tardis_source = 'affiliate.trx_funnel_relay'

    trans_tardis_source = 'affiliate.trx_transactions'

    daily_tardis_source = 'affiliate.trx_daily_by_merchant'

    daily_src_s3 = 'daily/daily_by_merchant_{}.csv'

    funnel_relay_src_s3 = 'reports/funnel_relay_{}.csv'

    trans_src_s3 = 'transactions/transactions_{}.csv'


class YoutubeRevShareConfig:
    notebook_name = os.getenv('YOUTUBE_REVSHARE_NOTEBOOK', '/Repos/Production/yt-revshare-data-integration/src/yt_revshare_ingest')


class SeoConfig:
    seo_trending_news_notebook = os.getenv("SEO_TRENDING_NEWS_NOTEBOOK",
                                           "/Repos/Production/enterprise-data-integration/src/notebooks/"
                                           "seo_trending/trending_news")

    seo_trending_dash_notebook = os.getenv("SEO_TRENDING_DASH_NOTEBOOK",
                                           "/Repos/Production/enterprise-data-integration/src/notebooks/"
                                           "seo_trending/trending_dash")


class FacebookMarketingConfig:
    marketing_silver_notebook = os.getenv("MARKETING_SILVER_NOTEBOOK",
                                          "/Repos/Production/enterprise-data-integration/src/notebooks/"
                                          "facebook/facebook_ads_silver")

    marketing_connectors = ['cn_dse_fb_ads_abb', 'cn_dse_fb_ads_ad', 'cn_dse_fb_ads_adpro', 'cn_dse_fb_ads_ba',
                            'cn_dse_fb_ads_gq', 'cn_dse_fb_ads_master', 'cn_dse_fb_ads_tny_dom',
                            'cn_dse_fb_ads_tny_int',
                            'cn_dse_fb_ads_tny_sub', 'cn_dse_fb_ads_vf', 'cn_dse_fb_ads_vf2', 'cn_dse_fb_ads_wired',
                            'cn_dse_fb_ads_vogue', 'cn_dse_fb_ads_wired2']


class MegaphoneS3Config:
    megaphone_s3_bronze_notebook = os.getenv('MEGAPHONE_S3_BRONZE_NOTEBOOK',
                                             '/Repos/Production/enterprise-data-integration/src/'
                                             'notebooks/cne_megaphone/megaphone_s3_load/ingest_bronze')

    megaphone_s3_silver_notebook = os.getenv('MEGAPHONE_S3_SILVER_NOTEBOOK',
                                             '/Repos/Production/enterprise-data-integration/src/'
                                             'notebooks/cne_megaphone/megaphone_s3_load/ingest_silver')

    file_prefix = ['impression', 'metrics']

    file_suffix = '.json.gz'


class MegaphoneAPIConfig:
    megaphone_API_bronze_notebook = os.getenv('MEGAPHONE_API_BRONZE_NOTEBOOK',
                                              '/Repos/Production/enterprise-data-integration/src/'
                                              'notebooks/cne_megaphone/megaphone_api_load/ingest_bronze')

    megaphone_API_silver_notebook = os.getenv('MEGAPHONE_API_SILVER_NOTEBOOK',
                                              '/Repos/Production/enterprise-data-integration/src/'
                                              'notebooks/cne_megaphone/megaphone_api_load/ingest_silver')

    megaphone_API_back_fill_notebook = os.getenv('MEGAPHONE_API_BACKFILL_NOTEBOOK',
                                                 '/Repos/Production/enterprise-data-integration/src/'
                                                 'notebooks/cne_megaphone/megaphone_api_load/megaphone_api_backfill')


class AmazonAffiliatesConfig:
    ingestion_notebook_name = os.getenv('AMAZON_AFFILIATE_BRONZE_NOTEBOOK',
                                        '/Repos/Production/affiliate-data-integration/src/notebooks/amazon/ingest')

    missing_report_notebook_name = os.getenv('AMAZON_AFFILIATE_MISSING_REPORT_NOTEBOOK',
                                             '/Repos/Production/affiliate-data-integration/src/notebooks/amazon/process_missing_reports')

    date_migration_notebook_name = os.getenv('AMAZON_AFFILIATE_MIGRATION_NOTEBOOK',
                                             '/Repos/Production/affiliate-data-integration/src/notebooks/amazon/affiliates_data_migration')


class CneVideosConfig:
    cne_videos_notebook = os.getenv("CNE_VIDEOS_NOTEBOOK", "/Repos/Production/"
                                                           "enterprise-data-integration/src/"
                                                           "notebooks/cne_videos/daily_report_ingestion")
    instance_profile = os.getenv("CNE_VIDEOS_INSTANCE_PROFILE", "arn:aws:iam::930908212222:instance-profile/"
                                                                "cne-prod-deployment-role-instance-profile")


class GoogleSeoConfig:
    seo_silver_notebook = os.getenv("SEO_SILVER_NOTEBOOK", "/Repos/Production/"
                                                           "enterprise-data-integration/src/notebooks/seo/seo_silver")

    seo_connectors = ['cn_dse_seo_gl', 'cn_dse_seo_ad_italia_it', 'cn_dse_seo_ad_de',
                      'cn_dse_seo_admagazine', 'cn_dse_seo_admagazine_fr', 'cn_dse_seo_admagazine_ru',
                      'cn_dse_seo_ad_mx', 'cn_dse_seo_adpro_architecturaldigest', 'cn_dse_seo_allure',
                      'cn_dse_seo_beautybox_allure', 'cn_dse_seo_beststuffbox_gq', 'cn_dse_seo_bonappetit',
                      'cn_dse_seo_cnspotlight', 'cn_dse_seo_cntraveler', 'cn_dse_seo_cntraveller_in',
                      'cn_dse_seo_epicurious', 'cn_dse_seo_epicurious_recipes', 'cn_dse_seo_epicurious_recipes_member',
                      'cn_dse_seo_glamour_de', 'cn_dse_seo_glamour_es', 'cn_dse_seo_glamourmagazine_uk',
                      'cn_dse_seo_glamour_mx', 'cn_dse_seo_gq', 'cn_dse_seo_gq_de', 'cn_dse_seo_gqindia',
                      'cn_dse_seo_gqitalia_it', 'cn_dse_seo_gqjapan_jp', 'cn_dse_seo_gq_magazine_uk',
                      'cn_dse_seo_gq_mx', 'cn_dse_seo_gq_ru', 'cn_dse_seo_gq_tw',
                      'cn_dse_seo_houseandgarden_uk', 'cn_dse_seo_lacucinaitaliana', 'cn_dse_seo_newyorker',
                      'cn_dse_seo_pitchfork', 'cn_dse_seo_revistaad_es', 'cn_dse_seo_revistagq', 'cn_dse_seo_wired_uk',
                      'cn_dse_seo_revistavanityfair_es', 'cn_dse_seo_self', 'cn_dse_seo_tatler',
                      'cn_dse_seo_teenvogue', 'cn_dse_seo_tatler_ru', 'cn_dse_seo_thelovemagazine_uk',
                      'cn_dse_seo_them_us', 'cn_dse_seo_traveler_es',
                      'cn_dse_seo_vanityfair', 'cn_dse_seo_vanityfair_fr', 'cn_dse_seo_vanityfair_it',
                      'cn_dse_seo_vogue_de', 'cn_dse_seo_vogue_es', 'cn_dse_seo_vogue_fr', 'cn_dse_seo_vogue_in',
                      'cn_dse_seo_vogue_it', 'cn_dse_seo_vogue_jp', 'cn_dse_seo_vogue_mx', 'cn_dse_seo_vogue_ru',
                      'cn_dse_seo_vogue_tw', 'cn_dse_seo_vogue_uk', 'cn_dse_seo_wired', 'cn_dse_seo_wired_it',
                      'cn_dse_seo_vogue']


class TwitterConfig:
    twitter_notebook_path = Variable.get("TWITTER_NOTEBOOK_PATH",
                                         "/Repos/Production/enterprise-data-integration/"
                                         "src/notebooks/"
                                         "twitter_follower_report/followers_report")


class OktaConfig:
    okta_notebook_path = Variable.get("OKTA_NOTEBOOK_PATH",
                                      "/Repos/Production/enterprise-data-integration"
                                      "/src/notebooks/okta/okta_silver_process")
    tardis_data_source = 'EDW_LOAD.LD_SF_AD_ALL_USERS'


class YoutubeGlobal(YoutubeSilverCommon, SlackConfig):
    project_info_bq_project = "cni-ca-dfp"
    project_info_subscription = {"us": "yt_global_us_prod", "all": "yt_global_all_prod"}
    gcp_connection_id = Variable.get('YOUTUBE_GLOBAL_CONNECTION_ID', 'youtube_gcp_connection')
    yt_silver_notebook = Variable.get("YOUTUBE_SILVER_NOTEBOOK",
                                      "/Repos/Production/google-data-integration/"
                                      "src/notebooks/youtube/ingestion_silver")
    gcp_pubsub_cred = "secret/data-services/astronomer/prod/yt_global/gcp"
    yt_backfill_notebook = Variable.get("YOUTUBE_BACKFILL_NOTEBOOK",
                                        "/Repos/Production/google-data-integration/"
                                        "src/notebooks/youtube/backfill/youtube_backfill")
    yt_lookup_load_notebook = Variable.get("YOUTUBE_LOOKUP_NOTEBOOK",
                                           "/Repos/Production/google-data-integration/"
                                           "src/notebooks/youtube/lookup_load")
    yt_trending_feed_bronze_notebook = Variable.get("YOUTUBE_TRENDING_FEED_NOTEBOOK",
                                                    "/Repos/Production/google-data-integration/"
                                                    "src/notebooks/youtube/trending_feed/yt_trending_feed")
    yt_trending_feed_silver_notebook = Variable.get("YOUTUBE_TRENDING_FEED_NOTEBOOK",
                                                    "/Repos/Production/google-data-integration/"
                                                    "src/notebooks/youtube/trending_feed/yt_trending_feed_silver")
    yt_groups_bronze_notebook = Variable.get("YOUTUBE_GROUPS_NOTEBOOK",
                                             "/Repos/Production/google-data-integration/"
                                             "src/notebooks/youtube/group_content/yt_groups")
    yt_groups_silver_notebook = Variable.get("YOUTUBE_GROUPS_NOTEBOOK",
                                             "/Repos/Production/google-data-integration/"
                                             "src/notebooks/youtube/group_content/yt_groups_silver")
    yt_group_content_bronze_notebook = Variable.get("YOUTUBE_GROUP_CONTENT_NOTEBOOK",
                                                    "/Repos/Production/google-data-integration/"
                                                    "src/notebooks/youtube/group_content/yt_group_contents")
    yt_group_content_silver_notebook = Variable.get("YOUTUBE_GROUP_CONTENT_NOTEBOOK",
                                                    "/Repos/Production/google-data-integration/"
                                                    "src/notebooks/youtube/group_content/yt_group_contents_silver")
    yt_missing_data_backfill_notebook = Variable.get("YOUTUBE_MISSING_DATA_BACKFILL_NOTEBOOK",
                                                     "/Repos/Production/google-data-integration/"
                                                     "src/notebooks/youtube/backfill/missing_data_backfill")


class AmazonUKEmeaAffiliatesConfig:
    daily_revenue_ingestion_notebook_name = os.getenv('AMAZON_UK_EMEA_AFFILIATE_SILVER_NOTEBOOK',
                                                      '/Repos/Production/affiliate-data-integration/'
                                                      'src/notebooks/amazon/ingest_uk_emea_daily_revenue')

    content_insights_ingestion_notebook_name = os.getenv('AMAZON_UK_EMEA_AFFILIATE_CONTENT_INSIGHTS_SILVER_NOTEBOOK',
                                                         '/Repos/Production/affiliate-data-integration/src/notebooks/'
                                                         'amazon/ingest_uk_emea_content_insights')

    daily_clicks_ingestion_notebook_name = os.getenv('AMAZON_UK_EMEA_AFFILIATE_CONTENT_INSIGHTS_SILVER_NOTEBOOK',
                                                     '/Repos/Production/affiliate-data-integration/src/notebooks/'
                                                     'amazon/ingest_uk_emea_daily_clicks')

    missing_report_notebook_name = os.getenv('AMAZON_AFFILIATE_MISSING_REPORT_NOTEBOOK',
                                             '/Repos/Production/affiliate-data-integration/'
                                             'src/notebooks/amazon/process_uk_emea_missing_content_insights_reports')


class ImpactAffiliatesConfig:
    ingestion_notebook_name = os.getenv('IMPACT_AFFILIATE_SILVER_NOTEBOOK',
                                        '/Repos/Production/affiliate-data-integration/src/notebooks/impact/ingest_impact')


class StaqConfig:
    aws_s3_access_key_name = 'evergreen_airflow_s3_access_key'
    aws_s3_secret_key_name = 'evergreen_airflow_s3_secret_access_key'
    vendor_conf = {"triple-lift": "triple_lift", "ValidationFiles": "staq_pmp_push", "spotx": "spotx", "rubicon": "rubicon",
                   "IndexExchangeV2": "index_exchange", "app-nexus": "appnexus", "amazon/pmp-data": "amazon_pmp"}
    src_bucket = "cn-data-vendor"
    tgt_bucket = "cn-dse-staq-prod"
    staq_raw_files_copy_notebook = Variable.get('STAQ_RAW_FILES_COPY_NOTEBOOK',
                                                '/Repos/Production/staq-integration/src/notebooks/staq_copy_data')
    staq_raw_data_load_notebook = Variable.get('STAQ_2_DATA_LOAD_NOTEBOOK',
                                               '/Repos/Production/staq-integration/src/notebooks/staq_driver')
    staq_backfill_notebook = Variable.get('STAQ_2_BACKFILL_NOTEBOOK',
                                          '/Repos/Production/staq-integration/src/notebooks/staq_backfill')


class FacebookPagesConfig:
    pages_silver_notebook = os.getenv("PAGES_SILVER_NOTEBOOK",
                                      "/Repos/Production/enterprise-data-integration/src/notebooks/"
                                      "facebook/facebook_pages_silver")

    pages_connectors = ['cn_dse_fb_pages_condenast', 'cn_dse_fb_pages_allure', 'cn_dse_fb_pages_architecturaldigest',
                        'cn_dse_fb_pages_arstechnica', 'cn_dse_fb_pages_bonappetitmag',
                        'cn_dse_fb_pages_condenasttraveler', 'cn_dse_fb_pages_epicurious', 'cn_dse_fb_pages_glamour',
                        'cn_dse_fb_pages_gq', 'cn_dse_fb_pages_gqstyle', 'cn_dse_fb_pages_pitchfork',
                        'cn_dse_fb_pages_pitchforkmusicfestival', 'cn_dse_fb_pages_selfmagazine',
                        'cn_dse_fb_pages_teenvogue', 'cn_dse_fb_pages_newyorker', 'cn_dse_fb_pages_vanityfairmagazine',
                        'cn_dse_fb_pages_vogue', 'cn_dse_fb_pages_voguerunway', 'cn_dse_fb_pages_wired',
                        'cn_dse_fb_pages_heyirisdotcom', 'cn_dse_fb_pages_allurebeautymyths',
                        'cn_dse_fb_pages_adopendoor', 'cn_dse_fb_pages_cleverbyarchdigest',
                        'cn_dse_fb_pages_itsalivewithbrad', 'cn_dse_fb_pages_projectherfilms',
                        'cn_dse_fb_pages_basicskillschallenge', 'cn_dse_fb_pages_yousangmysong',
                        'cn_dse_fb_pages_gqactuallyme', 'cn_dse_fb_pages_gqtattootour',
                        'cn_dse_fb_pages_virtuallydating', 'cn_dse_fb_pages_newyorkerobsessions',
                        'cn_dse_fb_pages_secrettalenttheatre', 'cn_dse_fb_pages_wiredautocomplete',
                        'cn_dse_fb_pages_gqtruthbetold', 'cn_dse_fb_pages_basically',
                        'cn_dse_fb_pages_newyorkercartoons', 'cn_dse_fb_pages_healthyish',
                        'cn_dse_fb_pages_vfhollywood', 'cn_dse_fb_pages_vfvanities', 'cn_dse_fb_pages_vfhive',
                        'cn_dse_fb_pages_teenvogueitgirls', 'cn_dse_fb_pages_wiredscience',
                        'cn_dse_fb_pages_irispornstars', 'cn_dse_fb_pages_batestkitchen', 'cn_dse_fb_pages_manypeople',
                        'cn_dse_fb_pages_epicuriouspricepoints', 'cn_dse_fb_pages_thescreeningroom',
                        'cn_dse_fb_pages_them', 'cn_dse_fb_pages_versespitchfork', 'cn_dse_fb_pages_tnybackstory',
                        'cn_dse_fb_pages_sweatwithself', 'cn_dse_fb_pages_gqiconiccharacters',
                        'cn_dse_fb_pages_9newthings', 'cn_dse_fb_pages_linernotes']


class GA360apiReportConfig:
    silver_notebook = os.getenv("SILVER_NOTEBOOK",
                                "/Repos/Production/enterprise-data-integration/"
                                "src/notebooks/ga360_api_report/api_report_silver")

    ga360_connectors = ['cn_dse_ga360_epicurious', 'cn_dse_ga360_architectural_digest', 'cn_dse_ga360_allure',
                        'cn_dse_ga360_bon_appetit', 'cn_dse_ga360_conde_nast_traveller', 'cn_dse_ga360_teen_vogue',
                        'cn_dse_ga360_them', 'cn_dse_ga360_pitchfork', 'cn_dse_ga360_self', 'cn_dse_ga360_non_oo_video',
                        'cn_dse_ga360_ars_technica', 'cn_dse_ga360_gq', 'cn_dse_ga360_glamour',
                        'cn_dse_ga360_conde_nast_traveller', 'cn_dse_ga360_wired', 'cn_dse_ga360_vogue',
                        'cn_dse_ga360_the_new_yorker', 'cn_dse_ga360_vanity_fair', 'cn_dse_ga360_lenny_letter']

    alert_channel = '#fivetran_success_alert'
    failure_channel = '#fivetran_failure_alert'


class EncoreConfig:
    mongo_notebook = os.getenv("mongo_notebook", "/Repos/Production/dbt-content/"
                                                 "databricks/notebooks/encore/mongo_api_ingest")
    bq_notebook = os.getenv("mongo_notebook", "/Repos/Production/dbt-content/"
                                              "databricks/notebooks/encore/bq_encore_export")
    instance_profile = os.getenv("encore_profile", "arn:aws:iam::930908212222:instance-profile/"
                                                   "encore-prod-deployment-role-instance-profile")


class SkimlinksUKEmeaAffiliatesConfig:
    daily_revenue_ingestion_notebook_name = os.getenv('SKIMLINKS_UK_EMEA_DAILY_REVENUE_SILVER_NOTEBOOK',
                                                      '/Repos/Production/affiliate-data-integration'
                                                      '/src/notebooks/skimlinks/ingest_uk_emea_daily_revenue')
    commission_ingestion_notebook_name = os.getenv('SKIMLINKS_UK_EMEA_COMMISSION_SILVER_NOTEBOOK',
                                                   '/Repos/Production/affiliate-data-integration'
                                                   '/src/notebooks/skimlinks/ingest_uk_emea_daily_commission')
    instance_profile = os.getenv("SKIMLINKS_PROFILE", "arn:aws:iam::930908212222:instance-profile/"
                                                      "affiliate-prod-deployment-role-instance-profile")


class WebgearsUKEmeaAffiliatesConfig:
    ingestion_notebook_name = os.getenv("WEBGEARS_UK_EMEA_COMMISSION_SILVER_NOTEBOOK",
                                        "/Repos/Production/affiliate-data-integration"
                                        "/src/notebooks/webgears/ingest_uk_emea")

from airflow.exceptions import AirflowException
import boto3
# from plugins.utilities import update_tardis
from plugins.utilities import tardis_utils
from plugins.utilities.vault import vault_instance
# from cryptography.fernet import Fernet
from plugins.config import TrackonomicsConfig

# import json

# client_data = Vault.get_vault_data(VaultConfig.url, VaultConfig.token, VaultConfig.secrets_path)
# s3_vendor_resource = boto3.resource(
#     's3',
#     aws_access_key_id=client_data['trx_aws_key'],
#     aws_secret_access_key=client_data['trx_aws_secret'],
# )

vendor_bucket = TrackonomicsConfig.vendor_s3_bucket


def check_s3_key(**args):
    # key = args['key']
    key = 'daily/daily_by_merchant_2021-12-14.csv'
    src_s3 = boto3.resource(
        's3', aws_access_key_id=vault_instance.get_secret('trx_aws_key'),
        aws_secret_access_key=vault_instance.get_secret('trx_aws_secret'))
    src_bucket = src_s3.Bucket(vendor_bucket,)
    if key.split("/")[0] == 'reports':
        src_keys = [i.key for i in src_bucket.objects.filter(Prefix=key.split("/")[0]).all()]
    else:
        src_keys = [i.key for i in src_bucket.objects.filter(Prefix=key.split("/")[0]).all()]
    if key in src_keys:
        pass
    else:
        raise AirflowException("Trackonomics Bucket is not Updated")


def update_tardis_status(post_type, source, status, comments, context=None):
    logdate = context['ds']

    for i in status:
        tardis_utils.update_tardis(
            post_type=post_type,
            tardis_source=source,
            logdate=logdate,
            status=i,
            comments=comments)

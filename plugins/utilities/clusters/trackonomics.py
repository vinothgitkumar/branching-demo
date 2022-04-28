from airflow.models import Variable
from plugins.config import VaultConfig
import os


def get_trackonomics_silver_cluster_config(env):
    instance_profile = get_aws_instance_profile(env)

    return {
        "num_workers": 32,
        "spark_version": "9.1.x-scala2.12",
        "spark_conf": {
            "spark.databricks.io.cache.maxMetaDataCache": "10g",
            "spark.databricks.io.cache.maxDiskUsage": "200g",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.io.cache.enabled": "true",
            "spark.driver.maxResultSize": 0,
            "spark.databricks.repl.allowedLanguages": "python,sql",
            "spark.databricks.adaptive.enabled": "true",
            "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
            "spark.databricks.delta.merge.enableLowShuffle": "true",
            "spark.databricks.adaptive.coalescePartitions.enabled": "true"
        },
        "aws_attributes": {
            "first_on_demand": 32 + 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1a",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 3,
            "ebs_volume_size": 100
        },
        "node_type_id": "i3.2xlarge",
        "driver_node_type_id": "i3.2xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Sparrow Silver {}".format(env)
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
            "VAULT_ADDR": VaultConfig.url,
            "VAULT_TOKEN": VaultConfig.token,
            "VAULT_PATH": VaultConfig.secrets_path
        },
        "enable_elastic_disk": "true",
        "cluster_source": "JOB",
        "init_scripts": []
    }


class TardisConfig:
    CLIENT_JAR_PATH = Variable.get('TARDIS_CLIENT_JAR_PATH', 'https://tardis.conde.io/download/tardis-0.1.3'
                                                             '-py3-none-any.whl')


def get_trackonomics_silver_lib(env="default"):
    GH_TOKEN = os.getenv('GIT_TOKEN')

    if env.lower() == 'production':
        CLIENT_JAR_PATH_DQM = Variable.get('DQM_CLIENT_JAR_PATH',
                                           'git+https://' + GH_TOKEN + '@github.com/CondeNast/data-quality-metrics'
                                                                       '@latest')
    else:
        CLIENT_JAR_PATH_DQM = Variable.get('DQM_CLIENT_JAR_PATH',
                                           'git+https://' + GH_TOKEN + '@github.com/CondeNast/data-quality-metrics'
                                                                       '@non-prodv1.2')
    new_lib = [
        {
            "pypi": {"package": "hvac"}
        },
        {
            "pypi": {"package": "requests"}
        },
        {
            "pypi": {"package": "datadog"}
        },
        {
            "pypi": {"package": "cryptography"}
        },
        {
            "pypi": {"package": TardisConfig.CLIENT_JAR_PATH}
        },
        {
            "pypi": {"package": CLIENT_JAR_PATH_DQM}
        }
    ]
    return new_lib


def get_aws_instance_profile(env="default"):
    # instance_profile = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    instance_profile = "arn:aws:iam::930908212222:instance-profile/fivetran-stg-deployment-role-instance-profile"
    if env.lower() == "staging":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/affiliate-stg-deployment-role-instance-profile"
        # instance_profile = "arn:aws:iam::930908212222:instance-profile/fivetran-stg-deployment-role-instance-profile"
    if env.lower() == "production":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/affiliate-prod-deployment-role-instance-profile"
    return instance_profile

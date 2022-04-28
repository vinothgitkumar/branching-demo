import os
from airflow.models import Variable
from plugins.config import VaultConfig


def get_aws_instance_profile(env="default"):
    instance_profile = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    if env.lower() == "staging":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/megaphone-stg-deployment-role-instance-profile"
    if env.lower() == "production":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/megaphone-prod-deployment-role-instance-profile"
    return instance_profile


def getMegaphoneS3BronzeCluster(env='default'):
    new_cluster = {
        "autoscale": {
            "min_workers": 1,
            "max_workers": 2
        },
        "num_workers": 1,
        "spark_version": "9.1.x-scala2.12",
        "spark_conf": {},
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1a",
            "instance_profile_arn": get_aws_instance_profile(env),
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 3,
            "ebs_volume_size": 100
        },
        "node_type_id": "m4.large",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Vivacity",
            "Workstream": "Enterprise Data",
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
            "VAULT_ADDR": VaultConfig.url,
            "VAULT_TOKEN": VaultConfig.token,
            "VAULT_PATH": VaultConfig.secrets_path
        },
        "driver_node_type_id": "m4.large",
        "enable_elastic_disk": "true",
        "cluster_source": "JOB",
        "init_scripts": []
    }

    return new_cluster


def getMegaphoneS3SilverCluster(env='default'):
    new_cluster = {
        "autoscale": {
            "min_workers": 1,
            "max_workers": 2
        },
        "num_workers": 1,
        "spark_version": "9.1.x-scala2.12",
        "spark_conf": {},
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1a",
            "instance_profile_arn": get_aws_instance_profile(env),
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 3,
            "ebs_volume_size": 100
        },
        "node_type_id": "m4.large",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Vivacity",
            "Workstream": "Enterprise Data",
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
            "VAULT_ADDR": VaultConfig.url,
            "VAULT_TOKEN": VaultConfig.token,
            "VAULT_PATH": VaultConfig.secrets_path
        },
        "driver_node_type_id": "m4.large",
        "enable_elastic_disk": "true",
        "cluster_source": "JOB",
        "init_scripts": [],
    }

    return new_cluster


def getMegaphoneS3ClusterLibs(env='default'):
    GH_TOKEN = os.getenv('GIT_TOKEN')
    CLIENT_JAR_PATH = Variable.get('TARDIS_CLIENT_JAR_PATH', 'https://tardis.conde.io/download/tardis-0.1.3'
                                                             '-py3-none-any.whl')
    if env.lower() == 'production':
        CLIENT_JAR_PATH_DQM = Variable.get('DQM_CLIENT_JAR_PATH',
                                           'git+https://' + GH_TOKEN + '@github.com/CondeNast/data-quality-metrics'
                                                                       '@latest')
    else:
        CLIENT_JAR_PATH_DQM = Variable.get('DQM_CLIENT_JAR_PATH',
                                           'git+https://' + GH_TOKEN + '@github.com/CondeNast/data-quality-metrics'
                                                                       '@non-prodv1.2')

    databricks_megaphone_s3_load_libs = [
        {
            "pypi": {"package": "hvac"},
        },
        {
            "pypi": {"package": "boto3"},
        },
        {
            "pypi": {"package": "requests"}
        },
        {
            "pypi": {"package": "cryptography"}
        },
        {
            "pypi": {"package": CLIENT_JAR_PATH}
        },
        {
            "pypi": {"package": CLIENT_JAR_PATH_DQM}
        }
    ]

    return databricks_megaphone_s3_load_libs

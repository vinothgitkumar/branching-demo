from airflow.models import Variable
from plugins.config.gam.gam_config import TardisConfig


def getGamAPICluster(creds_path, env='staging'):
    instance_profile_arn = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    if env.lower() == "staging":
        instance_profile_arn = "arn:aws:iam::930908212222:instance-profile/google-stg-deployment-role-instance-profile"
    elif env.lower() == "production":
        instance_profile_arn = "arn:aws:iam::930908212222:instance-profile/google-prod-deployment-role-instance-profile"
    return {
        "num_workers": 0,
        "spark_version": "8.0.x-scala2.12",
        "spark_conf": {
            "spark.master": "local[*]",
            "spark.sql.jsonGenerator.ignoreNullFields": "false"
        },
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1b",
            "instance_profile_arn": instance_profile_arn,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 32
        },
        "node_type_id": "i3.4xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "GAM API"
        },
        "spark_env_vars": {
            "GOOGLE_APPLICATION_CREDENTIALS": creds_path,
            "DATADOG_API_KEY_UK": Variable.get("DATADOG_API_KEY_UK")
        },
        "enable_elastic_disk": "false",
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_gam_api_backfill_cluster(creds_path, env='staging'):
    tardis_env = 'production' if env == 'production' else 'nonprod'
    instance_profile_arn = get_aws_instance_profile(env)

    return {
        "num_workers": 4,
        "node_type_id": "i3.4xlarge",
        "spark_version": "9.1.x-scala2.12",
        "spark_conf": {
            "spark.sql.jsonGenerator.ignoreNullFields": "false",
            "spark.databricks.acl.dfAclsEnabled": "true",
            "spark.databricks.repl.allowedLanguages": "python,sql"
        },
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1b",
            "instance_profile_arn": instance_profile_arn,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 100
        },
        "driver_node_type_id": "i3.4xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Project": "GAM API",
            "Group": "Data Engineering"
        },
        "spark_env_vars": {
            "GOOGLE_APPLICATION_CREDENTIALS": creds_path,
            "ENVIRONMENT": tardis_env
        },
        "enable_elastic_disk": "false",
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_gam_api_backfill_clusterLibs():
    return [
        {
            "pypi": {"package": TardisConfig.CLIENT_JAR_PATH},
        },
        {
            "pypi": {"package": "cryptography"},
        },
        {
            "pypi": {"package": "hvac"}
        },
        {
            'pypi': {"package": "googleads"}
        },
        {
            'pypi': {"package": "openpyxl"}
        }
    ]


def getGamApiClusterLibs():
    return [
        {
            "pypi": {"package": TardisConfig.CLIENT_JAR_PATH},
        },
        {
            "pypi": {"package": "hvac"}
        },
        {
            'pypi': {"package": "googleads"}
        },
        {
            'pypi': {"package": "openpyxl"}
        }
    ]


def get_aws_instance_profile(env="default"):
    instance_profile = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    if env.lower() == "staging":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/google-stg-deployment-role-instance-profile"

    if env.lower() == "production":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/google-prod-deployment-role-instance-profile"
    return instance_profile


def get_gam_log_cluster(creds_path, env='staging'):

    tardis_env = 'production' if env == 'production' else 'nonprod'
    instance_profile_arn = get_aws_instance_profile(env)

    return {
        "num_workers": 6,
        "spark_version": "10.4.x-scala2.12",
        "spark_conf": {},
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1b",
            "instance_profile_arn": instance_profile_arn,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 100
        },
        "driver_node_type_id": "r5.4xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Project": "Google Ad Manager",
            "Group": "Data Engineering"
        },
        "spark_env_vars": {
            "GOOGLE_APPLICATION_CREDENTIALS": creds_path,
            "ENVIRONMENT": tardis_env,
        },
        "enable_elastic_disk": "false",
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_gam_logs_cluster_libs():
    return [
        {
            "pypi": {"package": TardisConfig.CLIENT_JAR_PATH}
        },
        {
            "pypi": {"package": "hvac"}
        },
        {
            "pypi": {"package": "cryptography"}
        }
    ]

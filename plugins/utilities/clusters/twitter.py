from plugins.config import VaultConfig


def get_twitter_silver_cluster_config(env):
    instance_profile = get_aws_instance_profile(env)

    return {
        "num_workers": 2,
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
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1a",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 3,
            "ebs_volume_size": 100
        },
        "node_type_id": "i3.xlarge",
        "driver_node_type_id": "i3.xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Twitter Silver {}".format(env)
        },
        "spark_env_vars": {
            "VAULT_TOKEN": VaultConfig.token,
            "VAULT_ADDR": VaultConfig.url,
            "VAULT_PATH": VaultConfig.secrets_path,
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": "true",
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_twitter_silver_lib():
    new_lib = [
        # {
        #     "whl": "dbfs:/k2d/3.0/aleph_sdk_py-3.0-py3-none-any.whl" # K2D SDK isn't available yet
        # },
        {
            "pypi": {"package": "datetime"}
        },
        {
            "pypi": {"package": "hvac"}
        },
        {
            "pypi": {"package": "boto3"}
        },
        {
            "pypi": {"package": "pandas"}
        },
        {
            "pypi": {"package": "cryptography"}
        },
        {
            "pypi": {
                    "package": "https://tardis.conde.io/download/tardis-0.1.2-py3-none-any.whl"
            }
        }
    ]
    return new_lib


def get_aws_instance_profile(env="default"):
    instance_profile = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    if env.lower() == "staging":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/fivetran-stg-deployment-role-instance-profile"
    if env.lower() == "production":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/fivetran-prod-deployment-role-instance-profile"
    return instance_profile

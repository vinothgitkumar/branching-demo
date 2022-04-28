def get_sparrow_kafka_consumer_cluster_config(env="staging"):
    instance_profile = "arn:aws:iam::930908212222:instance-profile/sparrow-stg-deployment-role-instance-profile"
    if env.lower() == "production":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/sparrow-prod-deployment-role-instance-profile"
    return {
        "num_workers": 80,
        "cluster_name": "",
        "spark_version": "8.4.x-scala2.12",
        "spark_conf": {
            "spark.sql.shuffle.partitions": "400",
            "spark.sql.streaming.stateStore.providerClass":
                "com.databricks.sql.streaming.state.RocksDBStateStoreProvider",
            "spark.kafka.consumer.cache.capacity": 128,
            "spark.databricks.delta.merge.enableLowShuffle": True
        },
        "aws_attributes": {
            "first_on_demand": 81,
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
            "Project": "Sparrow"
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": False,
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_sparrow_silver_cluster_config(env):
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
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": "true",
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_sparrow_silver_lib():
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
        }
    ]
    return new_lib


def get_sparrow_silver_agg_cluster_config(env):
    instance_profile = get_aws_instance_profile(env)

    return {
        "num_workers": 12,
        "spark_version": "9.1.x-scala2.12",
        "spark_conf": {
            "spark.databricks.io.cache.maxMetaDataCache": "10g",
            "spark.databricks.delta.optimizeWrite.enabled": True,
            "spark.databricks.io.cache.enabled": True,
            "spark.driver.maxResultSize": 0
        },
        "aws_attributes": {
            "first_on_demand": 12 + 1,
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
            "Project": "Sparrow Silver {}".format(env)
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": False,
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_sparrow_silver_backfill_cluster_config(env):
    instance_profile = get_aws_instance_profile(env)

    return {
        "autoscale": {
            "min_workers": "8",
            "max_workers": "16"
        },
        "spark_version": "10.4.x-photon-scala2.12",
        "spark_conf": {
            "spark.databricks.io.cache.maxMetaDataCache": "10g",
            "spark.databricks.io.cache.maxDiskUsage": "200g",
            "spark.databricks.delta.optimizeWrite.enabled": True,
            "spark.databricks.io.cache.enabled": True,
            "spark.driver.maxResultSize": 0,
            "spark.databricks.acl.dfAclsEnabled": True,
            "spark.databricks.repl.allowedLanguages": "python,sql"
        },
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1a",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": 100
        },
        "node_type_id": "i3.4xlarge",
        "driver_node_type_id": "i3.2xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Sparrow Silver Backfill{}".format(env)
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": "true",
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_aws_instance_profile(env="default"):
    instance_profile = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    if env.lower() == "staging":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/sparrow-stg-deployment-role-instance-profile"
    if env.lower() == "production":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/sparrow-prod-deployment-role-instance-profile"
    return instance_profile

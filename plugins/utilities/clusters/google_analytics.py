def _get_lib_info():
    return [
        {
            "pypi": {
                "package": "hvac"

            }
        },
        {
            "pypi": {
                "package": "https://tardis.conde.io/download/tardis-0.1.2-py3-none-any.whl"
            }
        }
    ]


def get_aws_instance_profile(env="default"):
    instance_profile = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    if env.lower() == "staging":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/google-stg-deployment-role-instance-profile"

    if env.lower() == "production":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/google-prod-deployment-role-instance-profile"
    return instance_profile


def get_bronze_cluster_config(creds_file_path, env="default"):
    instance_profile = get_aws_instance_profile(env)
    databricks_cluster_ga = {
        "autoscale": {
            "min_workers": 3,
            "max_workers": 5
        },
        "spark_version": "8.4.x-scala2.12",
        "spark_conf": {},
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1b",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 100
        },
        "spark_env_vars": {
            "GOOGLE_APPLICATION_CREDENTIALS": creds_file_path
        },
        "node_type_id": "i3.4xlarge",
        "driver_node_type_id": "r4.2xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Google Analytics"
        }
    }

    return databricks_cluster_ga, _get_lib_info()


def get_silver_cluster_config(env="default"):
    instance_profile = get_aws_instance_profile(env)

    databricks_cluster_ga = {
        "autoscale": {
            "min_workers": 5,
            "max_workers": 8
        },
        "spark_version": "8.4.x-scala2.12",
        "spark_conf": {},
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1b",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 100
        },
        "node_type_id": "i3.4xlarge",
        "driver_node_type_id": "r4.2xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Google Analytics"
        }
    }

    return databricks_cluster_ga, _get_lib_info()


def get_management_api_cluster_config(env='default'):
    instance_profile = get_aws_instance_profile(env)

    databricks_cluster = {
        "num_workers": 0,
        "spark_version": "8.0.x-scala2.12",
        "spark_conf": {
            "spark.master": "local[*]",
            "spark.databricks.cluster.profile": "singleNode"
        },

        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1b",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 50
        },
        "node_type_id": "i3.xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Google Analytics",
            "ResourceClass": "SingleNode"
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },

    }

    dependent_libs = [
        {
            "pypi": {
                "package": "google-api-python-client"
            }
        },
        {
            "pypi": {
                "package": "oauth2client"
            }
        }
    ]
    return databricks_cluster, dependent_libs


def get_ga360_api_report_cluster(env='default'):
    instance_profile = get_aws_instance_profile(env)
    new_cluster = {
        "autoscale": {
            "min_workers": "2",
            "max_workers": "4"
        },
        "spark_version": "9.1.x-scala2.12",
        "spark_conf": {
            "spark.databricks.delta.formatCheck.enabled": "false",
            "spark.databricks.delta.optimizeWrite.enabled": "true"
        },
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1a",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 100
        },
        "node_type_id": "i3.xlarge",
        "driver_node_type_id": "i3.xlarge",
        "custom_tags": {
            "Group": "Data Engineering",
            "Workstream": "Enterprise Data",
            "Project": "Google Analytics"
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": "False",
        "init_scripts": []
    }

    libs = [
        {
            "pypi": {
                "package": "https://tardis.conde.io/download/tardis-0.1.2-py3-none-any.whl"
            }
        }
    ]
    return new_cluster, libs

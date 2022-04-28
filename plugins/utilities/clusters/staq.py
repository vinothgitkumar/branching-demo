def get_aws_instance_profile(env="default"):
    instance_profile = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    if env.lower() == "staging":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/staq-stg-deployment-role-instance-profile"
    if env.lower() == "production":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/staq-prod-deployment-role-instance-profile"
    return instance_profile


def get_staq_copy_cluster(env):
    return {
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
            "ebs_volume_count": 1,
            "ebs_volume_size": 32
        },
        "node_type_id": "m4.large",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "STAQ 2.0",
            "Workstream": "Enterprise Data",
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "driver_node_type_id": "m4.large",
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_staq_data_load_cluster(env):
    return {
        "autoscale": {
            "min_workers": 1,
            "max_workers": 4
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
            "ebs_volume_count": 1,
            "ebs_volume_size": 32
        },
        "node_type_id": "m4.large",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "STAQ 2.0",
            "Workstream": "Enterprise Data",
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "driver_node_type_id": "m4.large",
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_staq_backfill_cluster(env):
    return {
        "autoscale": {
            "min_workers": 1,
            "max_workers": 4
        },
        "num_workers": 1,
        "spark_version": "9.1.x-scala2.12",
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1a",
            "instance_profile_arn": get_aws_instance_profile(env),
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 200
        },
        "node_type_id": "c4.2xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "STAQ 2.0",
            "Workstream": "Enterprise Data",
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "driver_node_type_id": "c4.2xlarge",
        "cluster_source": "JOB",
        "init_scripts": []
    }


def get_staq_cluster_lib():
    return [
        {
            "pypi": {
                "package": "hvac"

            }
        },
        {
            "pypi": {
                "package":  "https://tardis.conde.io/download/"
                            "tardis-0.1.3-py3-none-any.whl"
            }
        }
    ]

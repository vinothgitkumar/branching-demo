# Instance Profile
def get_aws_instance_profile(env="default"):
    instance_profile = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    if env.lower() == "staging":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/fivetran-stg-deployment-role-instance-profile"
    if env.lower() == "production":
        instance_profile = "arn:aws:iam::930908212222:instance-profile/fivetran-prod-deployment-role-instance-profile"
    return instance_profile


# Libraries
def get_marketing_campaign_lib():
    new_lib = [
        {
            "pypi": {
                "package": "https://tardis.conde.io/download/tardis-0.1.2-py3-none-any.whl"
            },
        }
    ]
    return new_lib


# Cluster Configuration
def get_marketing_campaign_cluster_config(env='default'):
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
            "Project": "CNE"
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": "False",
        "init_scripts": []
    }
    return new_cluster

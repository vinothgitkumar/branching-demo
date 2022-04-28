def get_bronze_configuration(instance_profile: str):
    new_cluster = {
        "autoscale": {
            "min_workers": "1",
            "max_workers": "2"
        },
        "spark_version": "9.1.x-scala2.12",
        "spark_conf": {
            "spark.driver.maxResultSize": "0"
        },
        "aws_attributes": {
            "first_on_demand": "1",
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1b",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": "100",
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": "3",
            "ebs_volume_size": "100"
        },
        "node_type_id": "r4.4xlarge",
        "driver_node_type_id": "r4.xlarge",
        "custom_tags": {
            "Group": "Data Engineering",
            "Workstream": "Enterprise Data",
            "Project": "Parsely"
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": "False",
        "init_scripts": []
    }
    return new_cluster


def get_silver_configuration(instance_profile: str):
    new_cluster = {
        "num_workers": 7,
        "spark_version": "9.1.x-scala2.12",
        "spark_conf": {
            "spark.hadoop.fs.s3a.canned.acl": "BucketOwnerFullControl",
            "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl"
        },
        "aws_attributes": {
            "first_on_demand": 2,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1b",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 100
        },
        "node_type_id": "r4.4xlarge",
        "driver_node_type_id": "r4.4xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Workstream": "Enterprise Data",
            "Project": "Parsely"
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": "False",
        "init_scripts": []
    }
    return new_cluster


def get_parsely_lib():
    new_lib = [
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

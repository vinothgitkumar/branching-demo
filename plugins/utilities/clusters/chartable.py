def get_chartable_configuration(instance_profile: str):
    new_cluster = {
        "autoscale": {
            "min_workers": "1",
            "max_workers": "2"
        },
        "spark_version": "9.1.x-scala2.12",
        "spark_conf": {
            "spark.hadoop.fs.s3a.canned.acl": "BucketOwnerFullControl",
            "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl"
        },
        "aws_attributes": {
            "first_on_demand": "2",
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1b",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": "100",
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": "1",
            "ebs_volume_size": "100"
        },
        "node_type_id": "i3.2xlarge",
        "driver_node_type_id": "i3.2xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Workstream": "Enterprise Data",
            "Project": "Chartable"
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": "false",
        "init_scripts": []
    }
    return new_cluster

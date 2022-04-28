def get_cluster_config(instance_profile):
    databricks_cluster = {
        "autoscale": {
            "min_workers": "1",
            "max_workers": "2"
        },
        'spark_version': "9.1.x-scala2.12",
        "spark_conf": {
          "spark.driver.maxResultSize": "120g",
          "spark.rdd.compress": "true",
          "spark.sql.inMemoryColumnarStorage.compressed": "true",
          "spark.default.parallelism": "600"
        },
        "node_type_id": "i3.xlarge",
        "driver_node_type_id": "i3.xlarge",
        'aws_attributes': {
            "first_on_demand": "2",
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1b",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": "100",
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": "1",
            "ebs_volume_size": "50"
        },
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Commerce"
        },
        "spark_env_vars": {
          "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": "false"
    }
    return databricks_cluster


def get_cluster_libs():
    databricks_skimlinks_cluster_libs = [
        {"pypi": {"package": "hvac"}},
        {"pypi": {"package": "requests"}}
    ]
    return databricks_skimlinks_cluster_libs

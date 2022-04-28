def get_cne_videos_configuration(instance_profile: str):
    new_cluster = {
        "autoscale": {
            "min_workers": "1",
            "max_workers": "2"
        },
        "spark_version": "9.1.x-scala2.12",
        "aws_attributes": {
            'availability': 'ON_DEMAND',
            "instance_profile_arn": instance_profile,
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


def get_cne_videos_lib():
    new_lib = [
        {
            "pypi": {"package": "boto3"}
        },
        {
            "pypi": {
                "package": "https://tardis.conde.io/download/tardis-0.1.2-py3-none-any.whl"
            }
        }
    ]
    return new_lib

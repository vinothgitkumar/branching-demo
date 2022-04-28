# This method provides a basic cluster configuration that most notebook tasks can use for simple execution. If a task needs a more powerful cluster or some
# other different configuration than this, then users must provide their own definition
def get_default_cluster_config(project_name: str, instance_profile: str):
    custom_tags = {
        "Group": "Data Engineering",
        "Project": project_name,
        "Workstream": "Enterprise Data",
    }
    cluster_config = {
        "spark_version": "8.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "driver_node_type_id": "i3.xlarge",
        'aws_attributes': {
            'availability': 'ON_DEMAND',
            "instance_profile_arn": instance_profile,
        },
        'num_workers': 2,
        "custom_tags": custom_tags
    }
    return cluster_config

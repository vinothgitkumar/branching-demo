
def get_aws_instance_profile(env):
    if env.lower() == "staging":
        instance_profile_arn = "arn:aws:iam::930908212222:instance-profile/ytrevshare-stg-deployment-role-instance-profile"
    elif env.lower() == "production":
        instance_profile_arn = "arn:aws:iam::930908212222:instance-profile/ytrevshare-prod-deployment-role-instance-profile"
    else:
        instance_profile_arn = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    return instance_profile_arn


def get_cluster_config(env):
    instance_profile = get_aws_instance_profile(env)
    databricks_cluster_yt = {
        'spark_version': "9.1.x-scala2.12",
        'node_type_id': 'r5.xlarge',
        'aws_attributes': {"availability": "ON_DEMAND",
                           "instance_profile_arn": instance_profile,
                           "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                           "ebs_volume_count": 1,
                           "ebs_volume_size": 50
                           },
        'num_workers': 1,
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Youtube RevShare"
        },
    }
    return databricks_cluster_yt

from plugins.config.youtube.youtube_config import TardisConfig as Tardis


def _get_lib_info():
    return [
        {
            "pypi": {
                "package": "hvac"
            }
        },
        {
            "pypi": {
                "package":  Tardis.CLIENT_JAR_PATH
            }
        },
        {
            "pypi": {"package": "cryptography"}
        },
        {
            "pypi": {"package": "bs4"}
        },
        {
            "pypi": {"package": "httplib2"}
        },
        {
            "pypi": {"package": "google-api-python-client"}
        },
        {
            "pypi": {"package": "oauth2client"}
        }
    ]


def get_aws_instance_profile(env):
    if env.lower() == "staging":
        instance_profile_arn = "arn:aws:iam::930908212222:instance-profile/google-stg-deployment-role-instance-profile"
    elif env.lower() == "production":
        instance_profile_arn = "arn:aws:iam::930908212222:instance-profile/google-prod-deployment-role-instance-profile"
    else:
        instance_profile_arn = "arn:aws:iam::930908212222:instance-profile/data-eng-team-role-instance-profile"
    return instance_profile_arn


def get_cluster_config(creds_file_path, env):
    instance_profile = get_aws_instance_profile(env)
    databricks_cluster_yt = {
        "autoscale": {
            "min_workers": 1,
            "max_workers": 4
        },
        "spark_version": "9.1.x-scala2.12",
        "spark_conf": {},
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1a",
            "instance_profile_arn": instance_profile,
            "spot_bid_price_percent": 100,
            "ebs_volume_count": 0
        },
        "node_type_id": "i3.xlarge",
        "driver_node_type_id": "i3.xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "Group": "Data Engineering",
            "Project": "Google Youtube"
        },
        "spark_env_vars": {
            "GOOGLE_APPLICATION_CREDENTIALS": creds_file_path
        },
    }
    return databricks_cluster_yt, _get_lib_info()

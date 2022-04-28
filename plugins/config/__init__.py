# This config module loads config classes based on the environment variable `ENV`. If no values are found that match
# an expected value, then it defaults to, well, `default`
# example usage:
# from plugins.config import DatabricksConfig

import os
import sys

# for airflow execution, it doesn't recognize paths the same as in a static project structure and local directory
# must be added to the path
# see link for more: https://stackoverflow.com/questions/47998552/apache-airflow-dag-cannot-import-local-module
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
env = os.getenv("ENV", "default")

# this can be updated to use `match: case: ` syntax if we ever upgrade to python 3.10+
if env.lower() == "staging":
    from plugins.config.config_staging import * # noqa
elif env.lower() == "production":
    from plugins.config.config_production import * # noqa
else:
    from plugins.config.config_default import * # noqa

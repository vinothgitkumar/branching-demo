from plugins.utilities.tardis_utils import update_tardis
from plugins.utilities.vault import vault_instance
import json
from plugins.config import DatabricksConfig
from plugins.utilities.airflow_connections import create_airflow_connection


def update_tardis_status(post_type, source, day, status, comments, context=None):
    update_tardis(post_type=post_type,
                  tardis_source=source,
                  logdate=day,
                  status=status,
                  comments=comments
                  )


def create_databricks_connection(**kwargs):
    TOKEN = vault_instance.get_secret(DatabricksConfig.token_key)
    HOST = DatabricksConfig.host
    create_airflow_connection(conn_id="databricks_default",
                              # Databricks doesn't work find the constant in the source code
                              conn_type="Databricks",
                              host=HOST,
                              login=None,
                              password=None,
                              port=None,
                              extra=json.dumps({"token": TOKEN, "host": HOST}),
                              uri=None,
                              )

import json

from plugins.config import DatabricksConfig
from plugins.utilities.airflow_connections import create_airflow_connection
from plugins.utilities.vault import vault_instance


def create_databricks_connection():
    token = vault_instance.get_secret(DatabricksConfig.token_key),
    host = DatabricksConfig.host
    connection_id = "databricks_default"
    create_airflow_connection(conn_id=connection_id,
                              conn_type="databricks",
                              host=host,
                              login=None,
                              password=None,
                              port=None,
                              extra=json.dumps({"token": token, "host": host.split("//")[1]}),
                              uri=None
                              )

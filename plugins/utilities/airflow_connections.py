import json

from airflow import settings
from airflow.models import Connection

from plugins.config import DatabricksConfig, DbtConfig
# create a connection from passed parameters; Will only create a new connection if it doesn't already exist or if the
# `force_create` flag is set to true
from plugins.utilities.vault import vault_instance


def create_airflow_connection(conn_id, conn_type, host, login, password, port, extra, uri, force_create=False):
    """
    Reference: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
    """
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port,
        extra=extra,
        uri=uri
    )

    session = settings.Session

    # check if connection exists already or force_create flag is true
    if (force_create) or (conn_id not in [str(c) for c in list_connections(session)]):
        session = settings.Session
        session.add(conn)
        session.commit()
        session.close()


def list_connections(session):
    _session = None

    if session is not None:
        _session = session
    else:
        _session = settings.Session

    _c = _session.query(Connection).all()

    return _c


def create_databricks_connection():
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


def create_dbt_connection():
    HOST = DbtConfig.host
    create_airflow_connection(conn_id="dbt_api",
                              conn_type="HTTP",
                              host=HOST,
                              login=None,
                              password=None,
                              port=None,
                              extra=None,
                              uri=None,
                              )


def create_gcp_connection(connection_id, vault_key, force_create=False):
    vault_instance.get_vault_data(SECRET_PATH=vault_key, force_refresh=True)
    response = vault_instance.secret

    print('Downloaded the gcs credentials from vault')
    key_json = json.dumps(response)
    # print(key_json)

    conn = Connection(
        conn_id=connection_id,
        conn_type='google_cloud_platform'
    )

    conn_extra = {
        "extra__google_cloud_platform__keyfile_dict": key_json
    }
    conn_extra_json = json.dumps(conn_extra)
    # print(conn_extra_json)
    conn.set_extra(conn_extra_json)
    create_airflow_connection(conn_id=connection_id,
                              conn_type='google_cloud_platform',
                              extra=json.dumps(conn_extra),
                              uri=None,
                              password=None,
                              host=None,
                              port=None,
                              force_create=force_create,
                              login=None,
                              )


if __name__ == "__main__":
    TOKEN = vault_instance.get_secret(DatabricksConfig.token_key)
    HOST = DatabricksConfig.host
    create_airflow_connection(conn_id="databricks_default",
                              # Databricks doesn't work find the constant in the source code
                              conn_type="Databricks",
                              host=HOST,
                              login=None,
                              password=None,
                              port=None,
                              extra=str({"token": TOKEN, "host": HOST}),
                              uri=None
                              )
    pass

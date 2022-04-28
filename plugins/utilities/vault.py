import hvac

from plugins.config import VaultConfig


class Vault:
    def __init__(self):
        self.secret = None

    def get_vault_data(self, url=VaultConfig.url, token=VaultConfig.token, force_refresh=False,
                       SECRET_PATH=VaultConfig.secrets_path):
        if force_refresh is True:
            client = hvac.Client(url=url, token=token)
            self.secret = client.read(SECRET_PATH)['data']

        return self.secret

    def get_secret(self, key):
        return self.secret.get(key)


vault_instance = Vault()
vault_instance.get_vault_data(force_refresh=True)

"""
This driver module is part of an ETL project (extract, transform, load).
It's meant to be imported by main.py script and used to parse credentials from Hashi Vault API

v.2023-11-16: Add new docstrings
"""

import os
import sys
import hvac

# ------------------------------------------------------------------------------------------
# TODO If HASHI_VAULT_APP_TOKEN is not present on local environment
# Load variable value from local ".env" file
# ------------------------------------------------------------------------------------------
# from dotenv import load_dotenv
# load_dotenv()


def test():
    """
    Function to test the HashiVaultClient module using a sample secret.
    """
    hashi_client = HashiVaultClient()
    secret_path = "segredo-teste"
    status, secret = hashi_client.get_secret(secret_path)

    if status:
        print(f"INFO  - {secret}")


class HashiVaultClient:
    """
    HashiVaultClient class for interacting with HashiCorp Vault API to retrieve secrets.
    """

    def __init__(self):
        """
        Initializes the HashiVaultClient with the necessary configuration and checks for authentication.
        Uses token credential from local environment variable `$HASHI_VAULT_APP_TOKEN`

        Raises:
            Exception: If the Hashi Vault token is invalid.
        """
        self.vault_url = "https://cluster-hashivault-0001-01.holos.company:8200"
        self.mount_point = "holos-company/data"
        self.token = os.environ.get("HASHI_VAULT_APP_TOKEN")

        self.client = hvac.Client(url=self.vault_url, token=self.token)
        self.vault_status = self.client.is_authenticated()

        if not self.vault_status:
            raise Exception("ERROR - Hashi Vault token invalid !!!")

    def get_secret(self, secret_path: str):
        """
        Retrieves a secret from the specified path in HashiCorp Vault.

        Args:
            secret_path (str): The path to the secret.

        Returns:
            tuple: A tuple containing a boolean indicating success and the secret data (dict) if successful.
                   If unsuccessful, returns False and None.
        """
        try:
            read_secret_result = self.client.secrets.kv.v1.read_secret(path=secret_path, mount_point=self.mount_point)
            response = read_secret_result.get("data", {}).get("data")

        except Exception as e:
            print(f"\nERROR  - An exception of type {type(e).__name__} occurred on {sys._getframe().f_code.co_name}",
                  f"ERROR  - {e}", sep="\n")
            raise Exception

        if response and "errors" not in response:
            print(f"INFO  - Successful Hashi-Vault API request for secret '{secret_path}'")
            return True, response
        else:
            print(f"\nERROR  - Failed to retrieve secret '{secret_path}'")
            return False, None


if __name__ == "__main__":
    test()

# ------------------------------------------------------------------------------------------
# END of module
# ------------------------------------------------------------------------------------------

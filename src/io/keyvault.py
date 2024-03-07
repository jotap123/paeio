import logging
import os

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

log = logging.getLogger(__name__)

# Set the logging level for all azure-* libraries
logger = logging.getLogger('azure')
logger.setLevel(logging.ERROR)


def load_from_keyvault():
    """Load secrets from Azure KeyVault into the environment"""

    kv_url = os.getenv("AZURE_KEYVAULT_URL")
    if not kv_url:
        log.warning("KeyVault URL not set, make sure to fix this!")
        return

    log.info("Connecting to Azure KeyVault: " + kv_url)
    creds = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    client = SecretClient(kv_url, creds)

    log.info("Loading secrets into memory")
    for secret in client.list_properties_of_secrets():
        name = secret.name.replace("-", "_")
        if name in os.environ:
            log.info(f"Secret '{name}' already in environment, not overwriting")
        else:
            log.info("Loading secret: " + name)
            os.environ[name] = client.get_secret(secret.name).value

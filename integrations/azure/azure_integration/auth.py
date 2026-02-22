from azure.core.credentials_async import AsyncTokenCredential
from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential
from loguru import logger


class AzureAuthenticatorFactory:
    @staticmethod
    def create(
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> AsyncTokenCredential:
        if tenant_id and client_id and client_secret:
            logger.info("Using ClientSecretCredential for Azure authentication")
            return ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )

        logger.info(
            "Client secret not provided, using DefaultAzureCredential "
            "(supports managed identity, workload identity, Azure CLI, etc.)"
        )
        return DefaultAzureCredential()

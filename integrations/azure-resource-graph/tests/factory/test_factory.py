import pytest
from typing import Any

from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential

from azure_integration.factory import (
    AzureAuthenticatorFactory,
    create_azure_client,
    AzureClientType,
)
from azure_integration.clients.rest.rest_client import AzureRestClient


def test_authenticator_with_all_credentials_returns_client_secret_credential() -> None:
    """When all three credentials are provided, use ClientSecretCredential."""
    cred = AzureAuthenticatorFactory.create(
        tenant_id="tid", client_id="cid", client_secret="sec"
    )
    assert isinstance(cred, ClientSecretCredential)


def test_authenticator_without_secret_returns_default_credential() -> None:
    """When client_secret is missing, fall back to DefaultAzureCredential."""
    cred = AzureAuthenticatorFactory.create(
        tenant_id="tid", client_id="cid", client_secret=None
    )
    assert isinstance(cred, DefaultAzureCredential)


def test_authenticator_with_no_credentials_returns_default_credential() -> None:
    """When no credentials provided at all, use DefaultAzureCredential."""
    cred = AzureAuthenticatorFactory.create()
    assert isinstance(cred, DefaultAzureCredential)


def test_authenticator_with_empty_strings_returns_default_credential() -> None:
    """Empty strings are treated as missing."""
    cred = AzureAuthenticatorFactory.create(
        tenant_id="", client_id="", client_secret=""
    )
    assert isinstance(cred, DefaultAzureCredential)


def test_authenticator_with_partial_credentials_returns_default_credential() -> None:
    """When only tenant_id is provided (partial), fall back to DefaultAzureCredential."""
    cred = AzureAuthenticatorFactory.create(
        tenant_id="tid", client_id=None, client_secret=None
    )
    assert isinstance(cred, DefaultAzureCredential)


def test_create_azure_client_returns_rest_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Avoid requiring azure.identity by faking the credential
    class _DummyCred:
        async def get_token(self, *args: Any, **kwargs: Any) -> Any:
            class _Tok:
                token = "t"

            return _Tok()

    monkeypatch.setattr(
        AzureAuthenticatorFactory, "create", staticmethod(lambda **_: _DummyCred())
    )

    client = create_azure_client(AzureClientType.RESOURCE_MANAGER)
    assert isinstance(client, AzureRestClient)

"""Tests for Azure authentication factory"""

from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential

from azure_integration.auth import AzureAuthenticatorFactory


def test_all_credentials_returns_client_secret_credential() -> None:
    """When all three credentials are provided, use ClientSecretCredential."""
    cred = AzureAuthenticatorFactory.create(
        tenant_id="tid", client_id="cid", client_secret="sec"
    )
    assert isinstance(cred, ClientSecretCredential)


def test_missing_secret_returns_default_credential() -> None:
    """When client_secret is missing, fall back to DefaultAzureCredential."""
    cred = AzureAuthenticatorFactory.create(
        tenant_id="tid", client_id="cid", client_secret=None
    )
    assert isinstance(cred, DefaultAzureCredential)


def test_missing_client_id_returns_default_credential() -> None:
    """When client_id is missing, fall back to DefaultAzureCredential."""
    cred = AzureAuthenticatorFactory.create(
        tenant_id="tid", client_id=None, client_secret="sec"
    )
    assert isinstance(cred, DefaultAzureCredential)


def test_missing_tenant_id_returns_default_credential() -> None:
    """When tenant_id is missing, fall back to DefaultAzureCredential."""
    cred = AzureAuthenticatorFactory.create(
        tenant_id=None, client_id="cid", client_secret="sec"
    )
    assert isinstance(cred, DefaultAzureCredential)


def test_no_credentials_returns_default_credential() -> None:
    """When no credentials provided at all, use DefaultAzureCredential."""
    cred = AzureAuthenticatorFactory.create()
    assert isinstance(cred, DefaultAzureCredential)


def test_empty_strings_returns_default_credential() -> None:
    """Empty strings are falsy and treated as missing."""
    cred = AzureAuthenticatorFactory.create(
        tenant_id="", client_id="", client_secret=""
    )
    assert isinstance(cred, DefaultAzureCredential)

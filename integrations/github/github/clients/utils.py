from typing import Any, Dict, Optional, cast, TYPE_CHECKING
from github.core.options import ListOrganizationOptions
from port_ocean.context.ocean import ocean

from github.clients.auth.abstract_authenticator import AbstractGitHubAuthenticator
from github.clients.http.base_client import AbstractGithubClient
from github.helpers.utils import GithubClientType
from port_ocean.context.event import event

if TYPE_CHECKING:
    from integration import GithubPortAppConfig


def integration_config(authenticator: AbstractGitHubAuthenticator) -> Dict[str, Any]:
    return {
        "authenticator": authenticator,
        "github_host": ocean.integration_config["github_host"],
    }


def get_github_organizations() -> ListOrganizationOptions:
    """Get the organizations from the integration config."""
    organization = ocean.integration_config.get("github_organization")
    port_app_config = cast("GithubPortAppConfig", event.port_app_config)

    return ListOrganizationOptions(
        organization=organization,
        allowed_multi_organizations=port_app_config.organizations,
        include_authenticated_user=port_app_config.include_authenticated_user,
    )


def get_mono_repo_organization(organization: str | None) -> str | None:
    """Get the organization for a monorepo."""
    return organization or ocean.integration_config.get("github_organization")


def is_github_app_multi_org_mode() -> bool:
    """Return True when running as a GitHub App without a fixed single organisation.

    In this mode the App is installed on multiple organisations and each org
    requires its own installation-scoped access token.
    """
    return (
        bool(ocean.integration_config.get("github_app_id"))
        and bool(ocean.integration_config.get("github_app_private_key"))
        and not ocean.integration_config.get("github_organization")
    )


def get_client_for_org(
    org: Dict[str, Any],
    client_type: GithubClientType = GithubClientType.REST,
) -> AbstractGithubClient:
    """Return an appropriately-authenticated client for *org*.

    In multi-org GitHub App mode each org needs its own installation token, so a
    fresh per-org client is created.  In all other modes the shared singleton
    client is returned unchanged.
    """
    if is_github_app_multi_org_mode():
        from github.clients.client_factory import create_github_client_for_org

        return create_github_client_for_org(
            org_login=org["login"],
            installation_id=org.get("__installation_id"),
            client_type=client_type,
        )

    from github.clients.client_factory import create_github_client

    return create_github_client(client_type)

from typing import Dict, Type, overload, Literal, Optional

from port_ocean.context.ocean import ocean
from github.clients.http.rest_client import GithubRestClient
from github.clients.http.graphql_client import GithubGraphQLClient
from github.clients.http.base_client import AbstractGithubClient
from loguru import logger
from github.helpers.utils import GithubClientType
from github.clients.utils import integration_config

from github.clients.auth.abstract_authenticator import AbstractGitHubAuthenticator
from github.clients.auth.personal_access_token_authenticator import (
    PersonalTokenAuthenticator,
)
from github.clients.auth.github_app_authenticator import (
    GitHubAppAuthenticator,
    GitHubAppJWTAuthenticator,
)
from github.helpers.exceptions import MissingCredentials


class GitHubAuthenticatorFactory:
    @staticmethod
    def create(
        github_host: str,
        organization: Optional[str] = None,
        token: Optional[str] = None,
        app_id: Optional[str] = None,
        installation_id: Optional[str] = None,
        private_key: Optional[str] = None,
    ) -> AbstractGitHubAuthenticator:
        if token:
            logger.debug(
                f"Creating Personal Token Authenticator for select organizations for PAT on {github_host}"
            )
            return PersonalTokenAuthenticator(token)

        if organization and app_id and private_key:
            logger.debug(
                f"Creating GitHub App Authenticator for {organization} on {github_host}"
            )
            return GitHubAppAuthenticator(
                app_id=app_id,
                installation_id=installation_id,
                private_key=private_key,
                organization=organization,
                github_host=github_host,
            )

        if app_id and private_key:
            logger.debug(
                f"Creating GitHub App JWT Authenticator (no org) on {github_host}"
            )
            return GitHubAppJWTAuthenticator(app_id=app_id, private_key=private_key)

        raise MissingCredentials("No valid GitHub credentials provided.")


class GithubClientFactory:
    _instance = None
    _clients: Dict[GithubClientType, Type[AbstractGithubClient]] = {
        GithubClientType.REST: GithubRestClient,
        GithubClientType.GRAPHQL: GithubGraphQLClient,
    }
    _instances: Dict[GithubClientType, AbstractGithubClient] = {}

    def __new__(cls) -> "GithubClientFactory":
        if cls._instance is None:
            cls._instance = super(GithubClientFactory, cls).__new__(cls)
        return cls._instance

    def get_client(self, client_type: GithubClientType) -> AbstractGithubClient:
        """Get or create a client instance from Ocean configuration.

        Args:
            client_type: Type of client to create ("rest" or other supported types)

        Returns:
            An instance of AbstractGithubClient

        Raises:
            ValueError: If client_type is invalid
        """

        if client_type not in self._instances:
            if client_type not in self._clients:
                logger.error(f"Invalid client type: {client_type}")
                raise ValueError(f"Invalid client type: {client_type}")

            authenticator = GitHubAuthenticatorFactory.create(
                github_host=ocean.integration_config["github_host"],
                organization=ocean.integration_config.get("github_organization"),
                token=ocean.integration_config.get("github_token"),
                app_id=ocean.integration_config.get("github_app_id"),
                installation_id=ocean.integration_config.get(
                    "github_app_installation_id"
                ),
                private_key=ocean.integration_config.get("github_app_private_key"),
            )

            logger.info(f"instantiated new {client_type} client.")

            self._instances[client_type] = self._clients[client_type](
                **integration_config(authenticator),
            )

        return self._instances[client_type]


@overload
def create_github_client(
    client_type: Literal[GithubClientType.REST],
) -> GithubRestClient: ...


@overload
def create_github_client(client_type: None = None) -> GithubRestClient: ...


@overload
def create_github_client(
    client_type: Literal[GithubClientType.GRAPHQL],
) -> GithubGraphQLClient: ...


def create_github_client(
    client_type: GithubClientType | None = GithubClientType.REST,
) -> AbstractGithubClient:
    factory = GithubClientFactory()
    return factory.get_client(client_type or GithubClientType.REST)


def create_github_client_for_org(
    org_login: str,
    installation_id: Optional[str] = None,
    client_type: GithubClientType = GithubClientType.REST,
) -> AbstractGithubClient:
    """Create a fresh org-scoped GitHub client with an installation token.

    Used in multi-org GitHub App mode where each org needs its own authenticator
    and installation token to access private resources.  Not cached — each call
    returns a new instance.
    """
    github_host = ocean.integration_config["github_host"]
    authenticator = GitHubAuthenticatorFactory.create(
        github_host=github_host,
        organization=org_login,
        app_id=ocean.integration_config.get("github_app_id"),
        installation_id=installation_id,
        private_key=ocean.integration_config.get("github_app_private_key"),
    )
    client_cls: Type[AbstractGithubClient] = {
        GithubClientType.REST: GithubRestClient,
        GithubClientType.GRAPHQL: GithubGraphQLClient,
    }[client_type]
    return client_cls(**integration_config(authenticator))

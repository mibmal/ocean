import asyncio
import base64
from typing import Any, Optional
from loguru import logger
from datetime import datetime, timedelta, timezone
import jwt
from github.clients.auth.abstract_authenticator import (
    AbstractGitHubAuthenticator,
    GitHubToken,
    GitHubHeaders,
)
from github.helpers.exceptions import AuthenticationException

_JWT_EXPIRY_MINUTES = 10


def generate_jwt(app_id: str, private_key: str) -> str:
    """Generate a GitHub App JWT token for App-level API calls."""
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=_JWT_EXPIRY_MINUTES)
    payload = {"iss": app_id, "iat": now, "exp": expires_at}
    decoded_key = (
        private_key
        if private_key.startswith("-----BEGIN")
        else base64.b64decode(private_key).decode()
    )
    return jwt.encode(payload, decoded_key, algorithm="RS256")


class GitHubAppJWTAuthenticator(AbstractGitHubAuthenticator):
    """Authenticator that always returns a fresh JWT.

    Used for App-level GitHub API calls (e.g. GET /app/installations,
    GET /users/{login}) where no installation token is needed.
    """

    def __init__(self, app_id: str, private_key: str) -> None:
        self.app_id = app_id
        self.private_key = private_key

    async def get_token(self, **kwargs: Any) -> GitHubToken:
        return GitHubToken(token=generate_jwt(self.app_id, self.private_key))

    async def get_headers(self, **kwargs: Any) -> GitHubHeaders:
        token = await self.get_token()
        return GitHubHeaders(
            Authorization=f"Bearer {token.token}",
            Accept="application/vnd.github+json",
            X_GitHub_Api_Version="2022-11-28",
        )


class GitHubAppAuthenticator(AbstractGitHubAuthenticator):
    JWT_EXPIRY_MINUTES = _JWT_EXPIRY_MINUTES

    def __init__(
        self,
        app_id: str,
        private_key: str,
        organization: str,
        github_host: str,
        installation_id: Optional[str] = None,
    ):
        self.app_id = app_id
        self.installation_id = installation_id
        self.private_key = private_key
        self.organization = organization
        self.github_host = github_host.rstrip("/")
        self.cached_installation_token: Optional[GitHubToken] = None
        self.installation_token_lock = asyncio.Lock()

    async def get_token(self, **kwargs: Any) -> GitHubToken:
        jwt_token = self._generate_jwt()
        if kwargs.get("return_jwt", False):
            return jwt_token

        async with self.installation_token_lock:
            if (
                self.cached_installation_token
                and not self.cached_installation_token.is_expired
            ):
                return self.cached_installation_token

            if not self.installation_id:
                self.installation_id = await self._fetch_installation_id(
                    jwt_token.token
                )

            self.cached_installation_token = await self._fetch_installation_token(
                jwt_token.token
            )
            logger.info("New GitHub App token acquired.")
            return self.cached_installation_token

    async def get_headers(self, **kwargs: Any) -> GitHubHeaders:
        token_response = await self.get_token(**kwargs)
        return GitHubHeaders(
            Authorization=f"Bearer {token_response.token}",
            Accept="application/vnd.github+json",
            X_GitHub_Api_Version="2022-11-28",
        )

    async def _fetch_installation_id(self, jwt_token: str) -> str:
        try:
            url = f"{self.github_host}/orgs/{self.organization}/installation"
            if await self.is_personal_org(self.github_host, self.organization):
                url = f"{self.github_host}/users/{self.organization}/installation"
            headers = {"Authorization": f"Bearer {jwt_token}"}
            response = await self.client.get(url, headers=headers)
            response.raise_for_status()
            return str(response.json()["id"])
        except Exception as e:
            raise AuthenticationException(
                f"Failed to fetch installation ID: {e}"
            ) from e

    async def _fetch_installation_token(self, jwt_token: str) -> GitHubToken:
        try:
            url = f"{self.github_host}/app/installations/{self.installation_id}/access_tokens"
            headers = {"Authorization": f"Bearer {jwt_token}"}
            response = await self.client.post(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            return GitHubToken(token=data["token"], expires_at=data["expires_at"])
        except Exception as e:
            raise AuthenticationException(
                f"Failed to fetch installation token: {e}"
            ) from e

    def _generate_jwt(self) -> GitHubToken:
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=self.JWT_EXPIRY_MINUTES)
        token = generate_jwt(self.app_id, self.private_key)
        return GitHubToken(token=token, expires_at=str(int(expires_at.timestamp())))

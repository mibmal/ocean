import re
from typing import List, Optional

from loguru import logger
from port_ocean.context.ocean import ocean
from port_ocean.core.ocean_types import ASYNC_GENERATOR_RESYNC_TYPE, RAW_ITEM
from port_ocean.utils.cache import cache_iterator_result, cache_coroutine_result

from github.clients.auth.github_app_authenticator import generate_jwt
from github.core.exporters.abstract_exporter import AbstractGithubExporter
from github.core.options import ListOrganizationOptions
from github.clients.http.rest_client import GithubRestClient
from github.helpers.exceptions import OrganizationRequiredException

_LINK_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


class RestOrganizationExporter(AbstractGithubExporter[GithubRestClient]):
    """Exporter for GitHub organizations using REST API."""

    async def is_classic_pat_token(self) -> bool:
        response = await self.client.make_request(f"{self.client.base_url}/user", {})
        return "x-oauth-scopes" in response.headers

    @cache_coroutine_result()
    async def get_personal_org(self) -> RAW_ITEM:
        """
        Fetch the personal account of the authenticated user.
        This method is cached to avoid repeated API calls.
        """
        logger.info("Fetching personal account")
        response = await self.client.send_api_request(f"{self.client.base_url}/user")
        if response:
            logger.info(
                f"Fetched personal account of login {response['login']} successfully"
            )
        return response

    @cache_iterator_result()
    async def get_paginated_resources(
        self, options: ListOrganizationOptions
    ) -> ASYNC_GENERATOR_RESYNC_TYPE:
        """Fetch organisations based on auth mode.

        Resolution order:
        1. Single org  — ``github_organization`` config is set.
        2. GitHub App  — App credentials set, no ``github_organization``.
        3. Classic PAT — no org config, classic PAT token.
        4. Error       — none of the above.
        """
        logger.info("Fetching organizations")

        allowed_multi_organizations: List[str] = options.get(
            "allowed_multi_organizations", []
        )
        include_authenticated_user: bool = options.get(
            "include_authenticated_user", False
        )

        if organization := options.get("organization"):
            logger.info(f"Fetching single organization {organization}")
            yield [
                await self.client.send_api_request(
                    f"{self.client.base_url}/users/{organization}"
                )
            ]
            return

        if ocean.integration_config.get("github_app_id") and ocean.integration_config.get(
            "github_app_private_key"
        ):
            logger.info("Fetching organizations via GitHub App installations")
            async for batch in self._stream_github_app_installations(
                allowed_multi_organizations
            ):
                yield batch
            return

        if not await self.is_classic_pat_token():
            raise OrganizationRequiredException(
                "Organization is required for non-classic PAT tokens"
            )

        async for batch in self._stream_selected_organizations(
            allowed_multi_organizations, include_authenticated_user
        ):
            yield batch

    async def get_resource[
        ExporterOptionsT: None
    ](self, options: None) -> Optional[RAW_ITEM]:
        raise NotImplementedError

    async def _stream_github_app_installations(
        self, allowed_multi_organizations: list[str]
    ) -> ASYNC_GENERATOR_RESYNC_TYPE:
        """Discover all organisations the GitHub App is installed on.

        Calls ``GET /app/installations`` with a short-lived JWT, then fetches
        each org's full data via ``GET /users/{login}`` so the yielded dicts
        match the shape produced by the single-org and PAT multi-org paths.

        An ``__installation_id`` key is embedded in each org dict so callers can
        create org-scoped installation-token clients without an extra round-trip.
        """
        import httpx

        app_id = ocean.integration_config["github_app_id"]
        private_key = ocean.integration_config["github_app_private_key"]
        github_host = ocean.integration_config["github_host"].rstrip("/")

        jwt_token = generate_jwt(app_id, private_key)
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        installations: list[tuple[str, str]] = []
        url: Optional[str] = f"{github_host}/app/installations"
        params: dict = {"per_page": 100}

        async with httpx.AsyncClient() as http_client:
            while url:
                response = await http_client.get(url, headers=headers, params=params)
                response.raise_for_status()
                for inst in response.json():
                    login = inst.get("account", {}).get("login")
                    installation_id = str(inst["id"])
                    if login and (
                        not allowed_multi_organizations
                        or login in allowed_multi_organizations
                    ):
                        installations.append((login, installation_id))
                match = _LINK_NEXT_RE.search(response.headers.get("Link", ""))
                url = match.group(1) if match else None
                params = {}

        logger.info(
            f"Discovered {len(installations)} GitHub App installation(s)"
        )

        batch = []
        for login, installation_id in installations:
            org_data = await self.client.send_api_request(
                f"{self.client.base_url}/users/{login}"
            )
            if org_data:
                batch.append({**org_data, "__installation_id": installation_id})

        if batch:
            yield batch

    async def _stream_selected_organizations(
        self, allowed_multi_organizations: list[str], include_authenticated_user: bool
    ) -> ASYNC_GENERATOR_RESYNC_TYPE:
        if include_authenticated_user:
            yield [await self.get_personal_org()]

        async for batch in self.client.send_paginated_request(
            f"{self.client.base_url}/user/orgs"
        ):
            yield [
                {**org, "type": "Organization"}
                for org in batch
                if not allowed_multi_organizations
                or org.get("login") in allowed_multi_organizations
            ]

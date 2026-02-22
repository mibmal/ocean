"""
Tests for port_ocean.health.health health probe endpoints.
"""

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from port_ocean.health.health import create_health_router


def _make_ocean_mock(started: bool) -> MagicMock:
    ocean_app = MagicMock()
    ocean_app.started = started
    le = MagicMock()
    le.identity = "pod-abc"
    le.is_leader = started
    le.leadership_transitions = 3
    le.last_renewal_latency_ms = 12.5
    le.consecutive_errors = 0
    ocean_app.leader_election = le
    return ocean_app


@pytest.fixture
def app_started() -> FastAPI:
    """FastAPI app with health routes, simulating a fully started Ocean app."""
    fast_api = FastAPI()
    fast_api.include_router(create_health_router(_make_ocean_mock(started=True)))
    return fast_api


@pytest.fixture
def app_not_started() -> FastAPI:
    """FastAPI app with health routes, simulating an Ocean app still starting up."""
    fast_api = FastAPI()
    fast_api.include_router(create_health_router(_make_ocean_mock(started=False)))
    return fast_api


@pytest.mark.asyncio
async def test_livez_always_returns_200(app_started: FastAPI) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app_started), base_url="http://test"
    ) as client:
        response = await client.get("/livez")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_livez_returns_200_even_before_startup(
    app_not_started: FastAPI,
) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app_not_started), base_url="http://test"
    ) as client:
        response = await client.get("/livez")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_readyz_returns_200_when_started(app_started: FastAPI) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app_started), base_url="http://test"
    ) as client:
        response = await client.get("/readyz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_readyz_returns_503_when_not_started(
    app_not_started: FastAPI,
) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app_not_started), base_url="http://test"
    ) as client:
        response = await client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["status"] == "not ready"


@pytest.mark.asyncio
async def test_startup_returns_200_when_started(app_started: FastAPI) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app_started), base_url="http://test"
    ) as client:
        response = await client.get("/startup")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_startup_returns_503_when_not_started(
    app_not_started: FastAPI,
) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app_not_started), base_url="http://test"
    ) as client:
        response = await client.get("/startup")
    assert response.status_code == 503
    assert response.json()["status"] == "starting"


@pytest.mark.asyncio
async def test_leaderz_returns_election_status(app_started: FastAPI) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app_started), base_url="http://test"
    ) as client:
        response = await client.get("/leaderz")
    assert response.status_code == 200
    data = response.json()
    assert data["identity"] == "pod-abc"
    assert data["is_leader"] is True
    assert data["leadership_transitions"] == 3
    assert data["last_renewal_latency_ms"] == 12.5
    assert data["consecutive_errors"] == 0

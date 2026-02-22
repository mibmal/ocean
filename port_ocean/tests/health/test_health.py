"""
Tests for port_ocean.health.health health probe endpoints.
"""

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from port_ocean.health.health import create_health_router


@pytest.fixture
def app_started() -> FastAPI:
    """FastAPI app with health routes, simulating a fully started Ocean app."""
    ocean_app = MagicMock()
    ocean_app.started = True
    fast_api = FastAPI()
    fast_api.include_router(create_health_router(ocean_app))
    return fast_api


@pytest.fixture
def app_not_started() -> FastAPI:
    """FastAPI app with health routes, simulating an Ocean app still starting up."""
    ocean_app = MagicMock()
    ocean_app.started = False
    fast_api = FastAPI()
    fast_api.include_router(create_health_router(ocean_app))
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

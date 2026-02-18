import pytest
from typing import Any
from unittest.mock import AsyncMock, patch
from httpx import BasicAuth, Response, ReadTimeout
from azure.core.credentials import AccessToken
from azure.core.credentials_async import AsyncTokenCredential
from azure_devops.client.base_client import (
    AZURE_DEVOPS_SCOPE,
    HTTPBaseClient,
    CONTINUATION_TOKEN_HEADER,
    PAGE_SIZE,
)


@pytest.fixture
def mock_client() -> HTTPBaseClient:
    return HTTPBaseClient(personal_access_token="test_token")


@pytest.mark.asyncio
async def test_get_paginated_by_top_and_continuation_token_single_page(
    mock_client: HTTPBaseClient,
) -> None:
    """Test pagination with a single page of results (no continuation token)."""
    mock_response = AsyncMock(spec=Response)
    mock_response.status_code = 200
    mock_response.headers = {}
    mock_response.json.return_value = {"value": [{"id": 1}, {"id": 2}]}

    with patch.object(
        mock_client, "send_request", return_value=mock_response
    ) as mock_send:
        generator = mock_client._get_paginated_by_top_and_continuation_token("test_url")
        results = [item async for page in generator for item in page]

        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2
        mock_send.assert_called_once_with("GET", "test_url", params={"$top": 50})


@pytest.mark.asyncio
async def test_get_paginated_by_top_and_continuation_token_multiple_pages(
    mock_client: HTTPBaseClient,
) -> None:
    """Test pagination with multiple pages using continuation token."""
    mock_response1 = AsyncMock(spec=Response)
    mock_response1.status_code = 200
    mock_response1.headers = {CONTINUATION_TOKEN_HEADER: "token123"}
    mock_response1.json.return_value = {"value": [{"id": 1}, {"id": 2}]}

    mock_response2 = AsyncMock(spec=Response)
    mock_response2.status_code = 200
    mock_response2.headers = {}
    mock_response2.json.return_value = {"value": [{"id": 3}, {"id": 4}]}

    with patch.object(
        mock_client, "send_request", side_effect=[mock_response1, mock_response2]
    ) as mock_send:
        generator = mock_client._get_paginated_by_top_and_continuation_token("test_url")
        results = [item async for page in generator for item in page]

        assert len(results) == 4
        assert results[0]["id"] == 1
        assert results[3]["id"] == 4

        assert mock_send.call_count == 2
        mock_send.assert_any_call("GET", "test_url", params={"$top": 50})
        mock_send.assert_any_call(
            "GET",
            "test_url",
            params={"$top": 50, "continuationToken": "token123"},
        )


@pytest.mark.asyncio
async def test_get_paginated_by_top_and_continuation_token_with_custom_data_key(
    mock_client: HTTPBaseClient,
) -> None:
    """Test pagination with a custom data key (not 'value')."""
    mock_response = AsyncMock(spec=Response)
    mock_response.status_code = 200
    mock_response.headers = {}
    mock_response.json.return_value = {"items": [{"id": 1}]}

    with patch.object(mock_client, "send_request", return_value=mock_response):
        generator = mock_client._get_paginated_by_top_and_continuation_token(
            "test_url", data_key="items"
        )
        results = [item async for page in generator for item in page]

        assert len(results) == 1
        assert results[0]["id"] == 1


@pytest.mark.asyncio
async def test_get_paginated_by_top_and_continuation_token_retry_on_timeout(
    mock_client: HTTPBaseClient,
) -> None:
    """Test pagination retries successfully after a timeout."""
    mock_response = AsyncMock(spec=Response)
    mock_response.status_code = 200
    mock_response.headers = {}
    mock_response.json.return_value = {"value": [{"id": 1}]}

    with patch.object(
        mock_client,
        "send_request",
        side_effect=[ReadTimeout("Request timed out"), mock_response],
    ) as mock_send:
        generator = mock_client._get_paginated_by_top_and_continuation_token("test_url")
        results = [item async for page in generator for item in page]

        assert len(results) == 1
        assert mock_send.call_count == 2


@pytest.mark.asyncio
async def test_get_paginated_by_top_and_continuation_token_exhausts_retries(
    mock_client: HTTPBaseClient,
) -> None:
    """Test pagination raises ReadTimeout after exhausting retries."""
    side_effects = [
        ReadTimeout("Request timed out 1"),
        ReadTimeout("Request timed out 2"),
        ReadTimeout("Request timed out 3"),
    ]
    with patch.object(
        mock_client, "send_request", side_effect=side_effects
    ) as mock_send:
        with pytest.raises(ReadTimeout):
            generator = mock_client._get_paginated_by_top_and_continuation_token(
                "test_url"
            )
            _ = [item async for page in generator for item in page]

        assert mock_send.call_count == 3


@pytest.mark.asyncio
async def test_get_paginated_by_top_and_skip_retry_on_timeout(
    mock_client: HTTPBaseClient,
) -> None:
    """Test _get_paginated_by_top_and_skip retries successfully after a timeout."""
    mock_response = AsyncMock(spec=Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"value": [{"id": 1}]}

    mock_response_empty = AsyncMock(spec=Response)
    mock_response_empty.status_code = 200
    mock_response_empty.json.return_value = {"value": []}

    with patch.object(
        mock_client,
        "send_request",
        side_effect=[
            ReadTimeout("Request timed out"),
            mock_response,
            mock_response_empty,
        ],
    ) as mock_send:
        generator = mock_client._get_paginated_by_top_and_skip("test_url")
        results = [item async for page in generator for item in page]

        assert len(results) == 1
        assert mock_send.call_count == 3


@pytest.mark.asyncio
async def test_get_paginated_by_top_with_max_results(
    mock_client: HTTPBaseClient,
) -> None:
    """Test _get_paginated_by_top_and_skip with max_results."""
    # page 1
    mock_response1 = AsyncMock(spec=Response)
    mock_response1.status_code = 200
    mock_response1.json.return_value = {"value": [{"id": idx} for idx in range(50)]}

    # page 2
    mock_response2 = AsyncMock(spec=Response)
    mock_response2.status_code = 200
    mock_response2.json.return_value = {
        "value": [{"id": idx} for idx in range(50, 100)]
    }

    max_results = 50
    # case 1: max_results matches exactly PAGE_SIZE
    captured_calls = []

    async def capture_and_return(*args: Any, **kwargs: Any) -> Response:
        # Capture a snapshot of params at call time
        captured_calls.append(
            {
                "args": args,
                "params": (
                    kwargs.get("params", {}).copy() if kwargs.get("params") else {}
                ),
            }
        )
        return mock_response1

    with patch.object(
        mock_client,
        "send_request",
        side_effect=capture_and_return,
    ):
        generator = mock_client._get_paginated_by_top_and_skip(
            "test_url", max_results=max_results
        )
        results = [item async for page in generator for item in page]

        assert len(results) == max_results
        assert len(captured_calls) == 1
        assert captured_calls[0]["args"] == ("GET", "test_url")
        assert captured_calls[0]["params"] == {"$top": max_results, "$skip": 0}

    # case 2: max_results is less than PAGE_SIZE
    mock_response3 = AsyncMock(spec=Response)
    mock_response3.status_code = 200
    mock_response3.json.return_value = {"value": [{"id": idx} for idx in range(25)]}
    max_results = 25
    captured_calls = []

    async def capture_and_return2(*args: Any, **kwargs: Any) -> Response:
        captured_calls.append(
            {
                "args": args,
                "params": (
                    kwargs.get("params", {}).copy() if kwargs.get("params") else {}
                ),
            }
        )
        return mock_response3

    with patch.object(
        mock_client,
        "send_request",
        side_effect=capture_and_return2,
    ):
        generator = mock_client._get_paginated_by_top_and_skip(
            "test_url", max_results=max_results
        )
        results = [item async for page in generator for item in page]

        assert len(results) == max_results
        assert len(captured_calls) == 1
        assert results[-1]["id"] == 24
        assert captured_calls[0]["args"] == ("GET", "test_url")
        assert captured_calls[0]["params"] == {"$top": max_results, "$skip": 0}

    # case 3: max_results is more than PAGE_SIZE
    mock_response_partial = AsyncMock(spec=Response)
    mock_response_partial.status_code = 200
    mock_response_partial.json.return_value = {
        "value": [{"id": idx} for idx in range(50, 75)]
    }
    max_results = 75
    captured_calls = []
    responses = [mock_response1, mock_response_partial]

    async def capture_and_return3(*args: Any, **kwargs: Any) -> Response:
        captured_calls.append(
            {
                "args": args,
                "params": (
                    kwargs.get("params", {}).copy() if kwargs.get("params") else {}
                ),
            }
        )
        return responses[len(captured_calls) - 1]

    with patch.object(
        mock_client,
        "send_request",
        side_effect=capture_and_return3,
    ):
        generator = mock_client._get_paginated_by_top_and_skip(
            "test_url", max_results=max_results
        )
        results = [item async for page in generator for item in page]

        assert len(results) == max_results
        assert len(captured_calls) == 2
        # First call
        assert captured_calls[0]["params"] == {"$top": PAGE_SIZE, "$skip": 0}
        # Second call
        assert captured_calls[1]["params"] == {"$top": 25, "$skip": PAGE_SIZE}


@pytest.mark.asyncio
async def test_get_paginated_by_top_and_skip_exhausts_retries(
    mock_client: HTTPBaseClient,
) -> None:
    """Test _get_paginated_by_top_and_skip raises ReadTimeout after exhausting retries."""
    side_effects = [
        ReadTimeout("Request timed out 1"),
        ReadTimeout("Request timed out 2"),
        ReadTimeout("Request timed out 3"),
    ]
    with patch.object(
        mock_client, "send_request", side_effect=side_effects
    ) as mock_send:
        with pytest.raises(ReadTimeout):
            generator = mock_client._get_paginated_by_top_and_skip("test_url")
            _ = [item async for page in generator for item in page]

        assert mock_send.call_count == 3


# --- Auth mode tests ---


@pytest.fixture
def mock_credential() -> AsyncTokenCredential:
    """Mock Azure credential that returns a dummy bearer token."""
    cred = AsyncMock(spec=AsyncTokenCredential)
    cred.get_token = AsyncMock(
        return_value=AccessToken("bearer-test-token", 9999999999)
    )
    return cred


@pytest.fixture
def mock_client_with_credential(
    mock_credential: AsyncTokenCredential,
) -> HTTPBaseClient:
    return HTTPBaseClient(credential=mock_credential)


def test_init_with_pat_sets_pat() -> None:
    """PAT-based client stores the PAT."""
    client = HTTPBaseClient(personal_access_token="my-pat")
    assert client._personal_access_token == "my-pat"
    assert client._credential is None


def test_init_with_credential_sets_credential(
    mock_credential: AsyncTokenCredential,
) -> None:
    """Credential-based client stores the credential."""
    client = HTTPBaseClient(credential=mock_credential)
    assert client._credential is mock_credential
    assert client._personal_access_token is None


def test_init_with_neither_raises_value_error() -> None:
    """Must provide either PAT or credential."""
    with pytest.raises(ValueError, match="Either personal_access_token or credential"):
        HTTPBaseClient()


@pytest.mark.asyncio
async def test_send_request_with_pat_uses_basic_auth(
    mock_client: HTTPBaseClient,
) -> None:
    """When PAT is configured, send_request uses BasicAuth."""
    mock_response = AsyncMock(spec=Response)
    mock_response.status_code = 200
    mock_response.headers = {}

    with patch.object(
        mock_client._client, "request", return_value=mock_response
    ):
        with patch.object(mock_client._rate_limiter, "__aenter__", return_value=None):
            with patch.object(mock_client._rate_limiter, "__aexit__", return_value=None):
                with patch.object(
                    mock_client._rate_limiter, "update_from_headers"
                ):
                    await mock_client.send_request(
                        "GET", "https://dev.azure.com/test"
                    )

    assert isinstance(mock_client._client.auth, BasicAuth)


@pytest.mark.asyncio
async def test_send_request_with_credential_uses_bearer_token(
    mock_client_with_credential: HTTPBaseClient,
    mock_credential: AsyncTokenCredential,
) -> None:
    """When credential is configured, send_request acquires a bearer token."""
    mock_response = AsyncMock(spec=Response)
    mock_response.status_code = 200
    mock_response.headers = {}

    with patch.object(
        mock_client_with_credential._client, "request", return_value=mock_response
    ) as mock_request:
        with patch.object(
            mock_client_with_credential._rate_limiter,
            "__aenter__",
            return_value=None,
        ):
            with patch.object(
                mock_client_with_credential._rate_limiter,
                "__aexit__",
                return_value=None,
            ):
                with patch.object(
                    mock_client_with_credential._rate_limiter,
                    "update_from_headers",
                ):
                    await mock_client_with_credential.send_request(
                        "GET", "https://dev.azure.com/test"
                    )

    mock_credential.get_token.assert_called_once_with(AZURE_DEVOPS_SCOPE)
    call_kwargs = mock_request.call_args
    headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers", {})
    assert headers["Authorization"] == "Bearer bearer-test-token"

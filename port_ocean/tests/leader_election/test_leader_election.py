"""
Tests for port_ocean.leader_election.leader_election.LeaderElection.

Strategy
--------
- All Kubernetes API calls are mocked via sys.modules injection so the test
  suite can run without installing kubernetes_asyncio.
- The internal helpers (_try_acquire_or_renew, _create_lease,
  _on_leadership_acquired, _on_leadership_lost) are tested directly to keep
  tests fast and deterministic.
- The public contract (is_leader, identity, callbacks, start/stop) is covered
  by integration-style tests that exercise the full state machine.
"""

import asyncio
import datetime
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from port_ocean.leader_election.leader_election import LeaderElection


# ---------------------------------------------------------------------------
# Helpers for building fake kubernetes objects
# ---------------------------------------------------------------------------


def _make_lease_spec(
    holder: str,
    renew_time: datetime.datetime,
    lease_duration: int = 15,
    transitions: int = 1,
) -> MagicMock:
    spec = MagicMock()
    spec.holder_identity = holder
    spec.renew_time = renew_time
    spec.lease_duration_seconds = lease_duration
    spec.lease_transitions = transitions
    return spec


def _make_lease(
    holder: str,
    renew_time: datetime.datetime,
    lease_duration: int = 15,
) -> MagicMock:
    lease = MagicMock()
    lease.spec = _make_lease_spec(holder, renew_time, lease_duration)
    return lease


def _make_api_exception(status: int) -> type:
    """Return a unique ApiException *class* (not instance) with the given status."""

    class ApiException(Exception):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.status = status
            super().__init__(*args, **kwargs)

    return ApiException


def _inject_k8s_modules(api_exception_cls: type) -> dict[str, Any]:
    """
    Inject fake kubernetes_asyncio modules into sys.modules.
    Returns a dict of {module_name: original_value} for teardown.
    """
    original: dict[str, Any] = {}
    for mod in ("kubernetes_asyncio", "kubernetes_asyncio.client"):
        original[mod] = sys.modules.get(mod)

    fake_client = MagicMock()
    fake_client.ApiException = api_exception_cls
    fake_client.V1Lease = MagicMock(return_value=MagicMock())
    fake_client.V1ObjectMeta = MagicMock(return_value=MagicMock())
    fake_client.V1LeaseSpec = MagicMock(return_value=MagicMock())

    fake_k8s = MagicMock()
    fake_k8s.client = fake_client

    sys.modules["kubernetes_asyncio"] = fake_k8s
    sys.modules["kubernetes_asyncio.client"] = fake_client

    return original


def _restore_k8s_modules(original: dict[str, Any]) -> None:
    for mod, value in original.items():
        if value is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = value


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def disabled_le() -> LeaderElection:
    """Leader election disabled — always the sole leader."""
    return LeaderElection(enabled=False, integration_identifier="test-integration")


@pytest.fixture
def enabled_le() -> LeaderElection:
    """Leader election enabled with a fixed identity."""
    with patch("socket.gethostname", return_value="pod-abc"):
        return LeaderElection(
            enabled=True,
            integration_identifier="test-integration",
            namespace="default",
            lease_duration=15,
            renew_deadline=10,
            retry_period=2,
        )


# ---------------------------------------------------------------------------
# Disabled-mode (backwards compatibility)
# ---------------------------------------------------------------------------


def test_disabled_is_always_leader(disabled_le: LeaderElection) -> None:
    assert disabled_le.is_leader is True


@pytest.mark.asyncio
async def test_disabled_start_creates_no_background_task(
    disabled_le: LeaderElection,
) -> None:
    await disabled_le.start()
    assert disabled_le._lease_task is None


@pytest.mark.asyncio
async def test_disabled_stop_is_safe(disabled_le: LeaderElection) -> None:
    await disabled_le.start()
    await disabled_le.stop()  # must not raise


# ---------------------------------------------------------------------------
# Enabled-mode: initial state
# ---------------------------------------------------------------------------


def test_enabled_starts_as_follower(enabled_le: LeaderElection) -> None:
    assert enabled_le.is_leader is False


def test_identity_property(enabled_le: LeaderElection) -> None:
    assert enabled_le.identity == "pod-abc"


# ---------------------------------------------------------------------------
# Namespace detection
# ---------------------------------------------------------------------------


def test_detect_namespace_reads_from_serviceaccount_file(
    tmp_path: pytest.TempPathFactory,
) -> None:
    ns_file = tmp_path / "namespace"  # type: ignore[operator]
    ns_file.write_text("production\n")  # type: ignore[union-attr]

    with patch(
        "port_ocean.leader_election.leader_election.open",
        create=True,
        return_value=open(str(ns_file)),  # noqa: SIM115
    ):
        result = LeaderElection._detect_namespace()

    assert result == "production"


def test_detect_namespace_defaults_to_default_when_file_missing() -> None:
    with patch("builtins.open", side_effect=FileNotFoundError):
        result = LeaderElection._detect_namespace()

    assert result == "default"


# ---------------------------------------------------------------------------
# Leadership transition callbacks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_leadership_acquired_fires_callbacks_and_sets_flag(
    enabled_le: LeaderElection,
) -> None:
    callback_a = AsyncMock()
    callback_b = AsyncMock()
    enabled_le.on_started_leading(callback_a)
    enabled_le.on_started_leading(callback_b)

    assert not enabled_le.is_leader

    await enabled_le._on_leadership_acquired("port-ocean-test")

    assert enabled_le.is_leader
    callback_a.assert_awaited_once()
    callback_b.assert_awaited_once()


@pytest.mark.asyncio
async def test_on_leadership_lost_fires_callbacks_and_clears_flag(
    enabled_le: LeaderElection,
) -> None:
    callback = AsyncMock()
    enabled_le.on_stopped_leading(callback)
    enabled_le._is_leader = True  # simulate previously holding the lease

    await enabled_le._on_leadership_lost("port-ocean-test")

    assert not enabled_le.is_leader
    callback.assert_awaited_once()


# ---------------------------------------------------------------------------
# _create_lease
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_lease_succeeds(enabled_le: LeaderElection) -> None:
    ApiException = _make_api_exception(0)
    original = _inject_k8s_modules(ApiException)
    try:
        coordination_v1 = AsyncMock()
        coordination_v1.create_namespaced_lease = AsyncMock(return_value=MagicMock())

        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        result = await enabled_le._create_lease(coordination_v1, "port-ocean-test", now)

        assert result is True
        coordination_v1.create_namespaced_lease.assert_awaited_once()
    finally:
        _restore_k8s_modules(original)


@pytest.mark.asyncio
async def test_create_lease_returns_false_on_conflict(
    enabled_le: LeaderElection,
) -> None:
    """409 Conflict means another instance won the simultaneous creation race."""
    ApiException409 = _make_api_exception(409)
    original = _inject_k8s_modules(ApiException409)
    try:
        coordination_v1 = AsyncMock()
        coordination_v1.create_namespaced_lease = AsyncMock(
            side_effect=ApiException409()
        )

        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        result = await enabled_le._create_lease(coordination_v1, "port-ocean-test", now)

        assert result is False
    finally:
        _restore_k8s_modules(original)


@pytest.mark.asyncio
async def test_create_lease_propagates_unexpected_errors(
    enabled_le: LeaderElection,
) -> None:
    ApiException500 = _make_api_exception(500)
    original = _inject_k8s_modules(ApiException500)
    try:
        coordination_v1 = AsyncMock()
        coordination_v1.create_namespaced_lease = AsyncMock(
            side_effect=ApiException500()
        )

        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        with pytest.raises(ApiException500):
            await enabled_le._create_lease(coordination_v1, "port-ocean-test", now)
    finally:
        _restore_k8s_modules(original)


# ---------------------------------------------------------------------------
# _try_acquire_or_renew
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_try_acquire_returns_false_when_another_holds_valid_lease(
    enabled_le: LeaderElection,
) -> None:
    ApiException = _make_api_exception(0)
    original = _inject_k8s_modules(ApiException)
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        # Lease held by a different instance, renewed 2 seconds ago (duration 15s)
        lease = _make_lease(
            "other-pod", now - datetime.timedelta(seconds=2), lease_duration=15
        )

        coordination_v1 = AsyncMock()
        coordination_v1.read_namespaced_lease = AsyncMock(return_value=lease)

        result = await enabled_le._try_acquire_or_renew(
            coordination_v1, "port-ocean-test"
        )

        assert result is False
        coordination_v1.replace_namespaced_lease.assert_not_awaited()
    finally:
        _restore_k8s_modules(original)


@pytest.mark.asyncio
async def test_try_acquire_takes_over_expired_lease(
    enabled_le: LeaderElection,
) -> None:
    ApiException = _make_api_exception(0)
    original = _inject_k8s_modules(ApiException)
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        # Lease expired 20 seconds ago (duration 15s)
        expired_renew = now - datetime.timedelta(seconds=20)
        lease = _make_lease("old-pod", expired_renew, lease_duration=15)

        coordination_v1 = AsyncMock()
        coordination_v1.read_namespaced_lease = AsyncMock(return_value=lease)
        coordination_v1.replace_namespaced_lease = AsyncMock(return_value=MagicMock())

        result = await enabled_le._try_acquire_or_renew(
            coordination_v1, "port-ocean-test"
        )

        assert result is True
        coordination_v1.replace_namespaced_lease.assert_awaited_once()
        assert lease.spec.holder_identity == enabled_le.identity
        assert lease.spec.lease_transitions == 2  # incremented
    finally:
        _restore_k8s_modules(original)


@pytest.mark.asyncio
async def test_try_acquire_renews_own_lease(enabled_le: LeaderElection) -> None:
    ApiException = _make_api_exception(0)
    original = _inject_k8s_modules(ApiException)
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        # We already hold the lease
        lease = _make_lease(enabled_le.identity, now - datetime.timedelta(seconds=5))

        coordination_v1 = AsyncMock()
        coordination_v1.read_namespaced_lease = AsyncMock(return_value=lease)
        coordination_v1.replace_namespaced_lease = AsyncMock(return_value=MagicMock())

        result = await enabled_le._try_acquire_or_renew(
            coordination_v1, "port-ocean-test"
        )

        assert result is True
        coordination_v1.replace_namespaced_lease.assert_awaited_once()
        # No transition when we already hold it
        assert lease.spec.lease_transitions == 1  # unchanged
    finally:
        _restore_k8s_modules(original)


@pytest.mark.asyncio
async def test_try_acquire_creates_lease_when_missing(
    enabled_le: LeaderElection,
) -> None:
    ApiException404 = _make_api_exception(404)
    original = _inject_k8s_modules(ApiException404)
    try:
        coordination_v1 = AsyncMock()
        coordination_v1.read_namespaced_lease = AsyncMock(side_effect=ApiException404())
        coordination_v1.create_namespaced_lease = AsyncMock(return_value=MagicMock())

        result = await enabled_le._try_acquire_or_renew(
            coordination_v1, "port-ocean-test"
        )

        assert result is True
        coordination_v1.create_namespaced_lease.assert_awaited_once()
    finally:
        _restore_k8s_modules(original)


@pytest.mark.asyncio
async def test_try_acquire_propagates_unexpected_api_errors(
    enabled_le: LeaderElection,
) -> None:
    ApiException500 = _make_api_exception(500)
    original = _inject_k8s_modules(ApiException500)
    try:
        coordination_v1 = AsyncMock()
        coordination_v1.read_namespaced_lease = AsyncMock(side_effect=ApiException500())

        with pytest.raises(ApiException500):
            await enabled_le._try_acquire_or_renew(coordination_v1, "port-ocean-test")
    finally:
        _restore_k8s_modules(original)


# ---------------------------------------------------------------------------
# Timezone-aware renew_time handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_try_acquire_handles_naive_renew_time(enabled_le: LeaderElection) -> None:
    """renew_time without tzinfo must not raise; treat as UTC."""
    ApiException = _make_api_exception(0)
    original = _inject_k8s_modules(ApiException)
    try:
        naive_renew = datetime.datetime.now() - datetime.timedelta(seconds=2)
        # naive_renew has no tzinfo — should still work
        assert naive_renew.tzinfo is None

        lease = _make_lease("other-pod", naive_renew, lease_duration=15)

        coordination_v1 = AsyncMock()
        coordination_v1.read_namespaced_lease = AsyncMock(return_value=lease)

        result = await enabled_le._try_acquire_or_renew(
            coordination_v1, "port-ocean-test"
        )

        # Lease is still valid (2s elapsed < 15s duration) → we should not take it
        assert result is False
    finally:
        _restore_k8s_modules(original)


# ---------------------------------------------------------------------------
# stop() cancels the background lease task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_cancels_lease_task(enabled_le: LeaderElection) -> None:
    async def _sleep_forever() -> None:
        await asyncio.sleep(9999)

    enabled_le._lease_task = asyncio.create_task(_sleep_forever())
    await enabled_le.stop()

    assert enabled_le._lease_task.cancelled()


@pytest.mark.asyncio
async def test_stop_is_idempotent_when_no_task(enabled_le: LeaderElection) -> None:
    assert enabled_le._lease_task is None
    await enabled_le.stop()  # must not raise

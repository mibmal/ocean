import asyncio
import datetime
import random
import socket
from typing import TYPE_CHECKING, Callable, Awaitable

if TYPE_CHECKING:
    from kubernetes_asyncio.client import CoordinationV1Api  # type: ignore[import]

from loguru import logger


class LeaderElection:
    """
    Kubernetes Lease-based leader election for port-ocean integrations.

    When leader_election is enabled (via config), multiple replicas of the same
    integration can run simultaneously. Only the leader performs scheduled resyncs
    and reconciliation. All instances (leader and followers) handle live webhook events.

    When leader_election is disabled (default), the instance always considers itself
    the leader — preserving full backwards compatibility with single-replica deployments.

    Leadership is determined by holding a Kubernetes Lease object in the same namespace
    as the pod. The lease is renewed every `renew_deadline` seconds. If the leader fails
    to renew, another instance acquires the lease after `lease_duration` seconds.
    """

    def __init__(
        self,
        enabled: bool,
        integration_identifier: str,
        namespace: str | None = None,
        lease_duration: int = 15,
        renew_deadline: int = 10,
        retry_period: int = 2,
    ) -> None:
        self._enabled = enabled
        self._integration_identifier = integration_identifier
        self._namespace = namespace or self._detect_namespace()
        self._lease_duration = lease_duration
        self._renew_deadline = renew_deadline
        self._retry_period = retry_period
        self._is_leader = not enabled  # single-instance always starts as leader
        self._identity = socket.gethostname()
        self._on_started_leading_callbacks: list[Callable[[], Awaitable[None]]] = []
        self._on_stopped_leading_callbacks: list[Callable[[], Awaitable[None]]] = []
        self._lease_task: asyncio.Task[None] | None = None

    @staticmethod
    def _detect_namespace() -> str:
        """Read the pod namespace from the service account mount (standard in K8s)."""
        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
                return f.read().strip()
        except FileNotFoundError:
            return "default"

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @property
    def identity(self) -> str:
        return self._identity

    def on_started_leading(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Register a callback to run when this instance becomes leader."""
        self._on_started_leading_callbacks.append(callback)

    def on_stopped_leading(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Register a callback to run when this instance loses leadership."""
        self._on_stopped_leading_callbacks.append(callback)

    async def start(self) -> None:
        """
        Start leader election. If disabled, this is a no-op (instance is always leader).
        If enabled, begins competing for the K8s Lease in a background task.
        """
        if not self._enabled:
            logger.info(
                "Leader election disabled — this instance is the sole leader",
                identity=self._identity,
            )
            return

        logger.info(
            "Leader election enabled — competing for leadership",
            identity=self._identity,
            namespace=self._namespace,
            lease_duration=self._lease_duration,
        )
        self._lease_task = asyncio.create_task(self._run_election_loop())

    async def stop(self) -> None:
        if self._lease_task and not self._lease_task.done():
            self._lease_task.cancel()
            try:
                await self._lease_task
            except asyncio.CancelledError:
                pass

    async def _on_leadership_acquired(self, lease_name: str) -> None:
        logger.info(
            "Acquired leadership",
            identity=self._identity,
            lease=lease_name,
        )
        self._is_leader = True
        for cb in self._on_started_leading_callbacks:
            try:
                await asyncio.wait_for(cb(), timeout=self._renew_deadline)
            except asyncio.TimeoutError:
                logger.warning(
                    "on_started_leading callback timed out",
                    identity=self._identity,
                    timeout=self._renew_deadline,
                )
            except Exception as e:
                logger.warning(
                    "on_started_leading callback raised an error",
                    identity=self._identity,
                    error=str(e),
                )

    async def _on_leadership_lost(self, lease_name: str) -> None:
        logger.warning(
            "Lost leadership",
            identity=self._identity,
            lease=lease_name,
        )
        self._is_leader = False
        for cb in self._on_stopped_leading_callbacks:
            try:
                await asyncio.wait_for(cb(), timeout=self._renew_deadline)
            except asyncio.TimeoutError:
                logger.warning(
                    "on_stopped_leading callback timed out",
                    identity=self._identity,
                    timeout=self._renew_deadline,
                )
            except Exception as e:
                logger.warning(
                    "on_stopped_leading callback raised an error",
                    identity=self._identity,
                    error=str(e),
                )

    async def _run_election_loop(self) -> None:
        """Continuously attempt to acquire/renew the K8s Lease."""
        # Import here so kubernetes is only required when leader election is enabled
        try:
            from kubernetes_asyncio import client, config  # type: ignore[import]
            from kubernetes_asyncio.client import ApiException  # type: ignore[import]
        except ImportError:
            raise ImportError(
                "Leader election requires the 'kubernetes' extra: "
                "install port-ocean[k8s]"
            )

        try:
            config.load_incluster_config()
        except Exception:
            logger.warning(
                "Could not load in-cluster K8s config, trying kubeconfig",
                identity=self._identity,
            )
            await config.load_kube_config()

        coordination_v1 = client.CoordinationV1Api()
        lease_name = f"port-ocean-{self._integration_identifier}"

        while True:
            try:
                acquired = await self._try_acquire_or_renew(coordination_v1, lease_name)
                if acquired and not self._is_leader:
                    await self._on_leadership_acquired(lease_name)
                elif not acquired and self._is_leader:
                    await self._on_leadership_lost(lease_name)

            except ApiException as e:
                logger.warning(
                    "K8s API error during leader election, will retry",
                    error=str(e),
                    identity=self._identity,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "Unexpected error during leader election, will retry",
                    error=str(e),
                    identity=self._identity,
                )

            if self._is_leader:
                sleep_seconds = self._renew_deadline
            else:
                # Add up to 10% jitter so followers don't all retry simultaneously
                jitter = random.uniform(0, self._retry_period * 0.1)
                sleep_seconds = self._retry_period + jitter
            await asyncio.sleep(sleep_seconds)

    async def _try_acquire_or_renew(
        self,
        coordination_v1: "CoordinationV1Api",
        lease_name: str,
    ) -> bool:
        """
        Try to acquire the lease if not held, or renew it if we already hold it.
        Returns True if this instance holds the lease after this call.
        """
        from kubernetes_asyncio.client import ApiException  # type: ignore[import]

        now = datetime.datetime.now(datetime.timezone.utc)
        now_micro = now.replace(microsecond=0)

        try:
            lease = await coordination_v1.read_namespaced_lease(  # type: ignore[union-attr]
                name=lease_name, namespace=self._namespace
            )
            spec = lease.spec
            current_holder = spec.holder_identity
            renew_time = spec.renew_time
            lease_duration = spec.lease_duration_seconds or self._lease_duration

            # Check if the current holder's lease has expired
            if current_holder != self._identity and renew_time is not None:
                # renew_time may or may not be timezone-aware depending on the k8s client version
                aware_renew_time = (
                    renew_time
                    if renew_time.tzinfo is not None
                    else renew_time.replace(tzinfo=datetime.timezone.utc)
                )
                elapsed = (now - aware_renew_time).total_seconds()
                if elapsed < lease_duration:
                    # Another instance holds a valid lease
                    return False

            # Either we hold it or it has expired — take/renew it
            spec.holder_identity = self._identity
            spec.renew_time = now_micro
            spec.lease_duration_seconds = self._lease_duration
            if current_holder != self._identity:
                spec.acquire_time = now_micro
                spec.lease_transitions = (spec.lease_transitions or 0) + 1

            try:
                await coordination_v1.replace_namespaced_lease(  # type: ignore[union-attr]
                    name=lease_name,
                    namespace=self._namespace,
                    body=lease,
                )
            except ApiException as e:
                if e.status in (409, 422):
                    # Another instance won the replace race — we lost this round
                    return False
                raise
            return True

        except ApiException as e:
            if e.status == 404:
                # Lease doesn't exist yet — create it
                return await self._create_lease(coordination_v1, lease_name, now_micro)
            raise

    async def _create_lease(
        self,
        coordination_v1: "CoordinationV1Api",
        lease_name: str,
        now: datetime.datetime,
    ) -> bool:
        from kubernetes_asyncio import client  # type: ignore[import]
        from kubernetes_asyncio.client import ApiException  # type: ignore[import]

        lease_body = client.V1Lease(
            metadata=client.V1ObjectMeta(
                name=lease_name,
                namespace=self._namespace,
            ),
            spec=client.V1LeaseSpec(
                holder_identity=self._identity,
                acquire_time=now,
                renew_time=now,
                lease_duration_seconds=self._lease_duration,
                lease_transitions=1,
            ),
        )
        try:
            await coordination_v1.create_namespaced_lease(  # type: ignore[union-attr]
                namespace=self._namespace,
                body=lease_body,
            )
            return True
        except ApiException as e:
            if e.status == 409:
                # Another instance created it simultaneously — we lost the race
                return False
            raise

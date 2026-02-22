from typing import TYPE_CHECKING

from fastapi import APIRouter
from fastapi.responses import JSONResponse

if TYPE_CHECKING:
    from port_ocean.ocean import Ocean


def create_health_router(app: "Ocean") -> APIRouter:
    """
    Health probe endpoints for Kubernetes.

    /livez   — Liveness: is the event loop responsive? Returns 200 always.
               K8s restarts the pod if this fails (deadlock detection).

    /readyz  — Readiness: is the integration ready to serve traffic?
               Returns 200 once the app lifecycle has started. Returns 503
               during startup or shutdown. K8s removes the pod from Service
               endpoints when this fails.

    /startup — Startup: has initial boot completed? Returns 200 once the
               integration has fully started. K8s uses this to delay liveness
               checks for slow-starting integrations.
    """
    router = APIRouter()

    @router.get("/livez")
    async def livez() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @router.get("/readyz")
    async def readyz() -> JSONResponse:
        if not app.started:
            return JSONResponse(
                {"status": "not ready", "reason": "integration not started"},
                status_code=503,
            )
        return JSONResponse({"status": "ok"})

    @router.get("/startup")
    async def startup() -> JSONResponse:
        if not app.started:
            return JSONResponse(
                {"status": "starting", "reason": "integration not started"},
                status_code=503,
            )
        return JSONResponse({"status": "ok"})

    return router

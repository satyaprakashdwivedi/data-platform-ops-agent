"""HTTP entry point: REST endpoints for the n8n workflow steps, and the MCP server at /mcp for the agent.

    uvicorn ops_tools.api:app --host 0.0.0.0 --port 8000
"""
import hmac
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware

from .db import Database
from .mcp_server import build_mcp_server, jsonable
from .pipelines import PipelineNotFound
from .service import OpsService, ProposalError
from .settings import load_settings
from .spark_analysis import EventLogNotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
settings = load_settings()
db = Database(settings.database_url, settings.schema_file)
service = OpsService(settings, db)
mcp_server = build_mcp_server(service)
# The MCP endpoint sits behind the bearer token below; the hosts check is for browsers, not service-to-service calls.
mcp_app = mcp_server.streamable_http_app(
    stateless_http=True, json_response=True,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    db.create_schema()
    async with mcp_server.session_manager.run():
        yield
    db.close()


app = FastAPI(title="data-platform-ops-tools", version="1.0.0", lifespan=lifespan)


class BearerTokenMiddleware(BaseHTTPMiddleware):
    """Every endpoint except /health needs `Authorization: Bearer <OPS_TOOLS_TOKEN>` (when a token is set)."""

    async def dispatch(self, request, call_next):
        if settings.api_token and request.url.path != "/health":
            supplied = request.headers.get("authorization", "").removeprefix("Bearer ").strip()
            if not hmac.compare_digest(supplied, settings.api_token):
                return JSONResponse({"detail": "missing or invalid bearer token"}, status_code=401)
        return await call_next(request)


app.add_middleware(BearerTokenMiddleware)


def audited(tool: str):
    """Record each REST call in ops.tool_call, tied to the run id the workflow passes in X-Run-Id."""
    def dependency(x_run_id: int | None = Header(default=None)):
        return lambda arguments, fn: _run(tool, x_run_id, arguments, fn)
    return dependency


def _run(tool: str, run_id: int | None, arguments: dict, fn):
    try:
        with service.audited("rest", tool, arguments, run_id):
            return jsonable(fn())
    except (ProposalError, PipelineNotFound, EventLogNotFound) as error:
        raise HTTPException(status_code=409 if isinstance(error, ProposalError) else 404, detail=str(error))
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error))


class StartRun(BaseModel):
    workflow: str
    subject: str | None = None
    trigger: dict = Field(default_factory=dict)
    n8n_execution_id: str | None = None


class AnalyzeJob(BaseModel):
    app_id: str
    pipeline: str | None = None


class NewProposal(BaseModel):
    kind: str
    pipeline: str
    changes: dict
    diagnosis: str = ""
    rationale: str = ""
    confidence: float | None = None


class Decision(BaseModel):
    decision: str = Field(pattern="^(approve|decline|expire)$")
    decided_by: str = "unknown"
    comment: str = ""


class Notification(BaseModel):
    title: str
    body: str
    links: dict[str, str] = Field(default_factory=dict)


@app.get("/health")
def health():
    db.fetch_one("SELECT 1 AS ok")
    return {"status": "ok", "pull_requests": "github" if settings.opens_real_pull_requests else "dry-run patches",
            "notifications": "slack" if settings.slack_webhook_url else "log"}


@app.post("/runs")
def start_run(body: StartRun, run=Depends(audited("start_run"))):
    return run(body.model_dump(), lambda: service.start_run(body.workflow, body.subject, body.trigger,
                                                              body.n8n_execution_id))


@app.post("/runs/{run_id}/finish")
def finish_run(run_id: int, status: str = "completed", run=Depends(audited("finish_run"))):
    return run({"run_id": run_id, "status": status}, lambda: service.finish_run(run_id, status))


@app.post("/executions/{n8n_execution_id}/failed")
def execution_failed(n8n_execution_id: str, error: str = "", run=Depends(audited("execution_failed"))):
    return run({"n8n_execution_id": n8n_execution_id, "error": error},
               lambda: service.fail_runs_of_execution(n8n_execution_id, error))


@app.post("/spark/analyze")
def analyze(body: AnalyzeJob, run=Depends(audited("analyze_spark_job")), x_run_id: int | None = Header(default=None)):
    return run(body.model_dump(), lambda: service.analyze_spark_job(body.app_id, body.pipeline, x_run_id))


@app.post("/spark/review")
def review(limit: int = 20, run=Depends(audited("review_recent_jobs")), x_run_id: int | None = Header(default=None)):
    return run({"limit": limit}, lambda: service.review_recent_jobs(limit, x_run_id))


@app.get("/quality/incidents")
def incidents(window_minutes: int | None = None, run=Depends(audited("quality_incidents"))):
    return run({"window_minutes": window_minutes}, lambda: service.quality_incidents(window_minutes))


@app.get("/pipelines/{pipeline}")
def pipeline_config(pipeline: str, run=Depends(audited("get_pipeline_config"))):
    return run({"pipeline": pipeline}, lambda: service.pipeline_config(pipeline))


@app.post("/proposals")
def create_proposal(body: NewProposal, run=Depends(audited("create_proposal")),
                    x_run_id: int | None = Header(default=None)):
    return run(body.model_dump(), lambda: service.create_proposal(
        body.kind, body.pipeline, body.changes, body.diagnosis, body.rationale, body.confidence, x_run_id))


@app.get("/proposals/{proposal_id}")
def get_proposal(proposal_id: int, run=Depends(audited("get_proposal"))):
    return run({"proposal_id": proposal_id}, lambda: service.get_proposal(proposal_id))


@app.post("/proposals/{proposal_id}/decision")
def decide(proposal_id: int, body: Decision, run=Depends(audited("decide_proposal"))):
    return run({"proposal_id": proposal_id, **body.model_dump()},
               lambda: service.decide(proposal_id, body.decision, body.decided_by, body.comment))


@app.post("/proposals/{proposal_id}/apply")
def apply(proposal_id: int, run=Depends(audited("apply_proposal"))):
    return run({"proposal_id": proposal_id}, lambda: service.apply(proposal_id))


@app.post("/notify")
def send_notification(body: Notification, run=Depends(audited("notify")), x_run_id: int | None = Header(default=None)):
    return run(body.model_dump(), lambda: service.notify(body.title, body.body, body.links, x_run_id))


@app.get("/notifications")
def recent_notifications(limit: int = 10):
    return jsonable(db.fetch_all("SELECT * FROM ops.notification ORDER BY notification_id DESC LIMIT %s",
                                 (min(limit, 100),)))


app.mount("/", mcp_app)

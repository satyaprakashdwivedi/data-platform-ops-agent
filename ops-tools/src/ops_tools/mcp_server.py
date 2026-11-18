"""MCP tools for the agent (n8n's MCP Client Tool node, Claude Code or Claude Desktop).

The tool surface is deliberately read-and-check only: the agent can investigate and dry-run a
proposal against the guardrails, but it cannot create, approve or apply changes. Those are
deterministic workflow steps behind a human approval.
"""
import json
from decimal import Decimal

from mcp.server.mcpserver import MCPServer
from mcp_types import ToolAnnotations

from .service import OpsService

INSTRUCTIONS = """Tools for diagnosing Spark jobs and data quality incidents on the data platform.
Ground every claim in tool output. Never invent Spark config values: use what analyze_spark_job recommends,
and run check_proposal before you finalise a change."""

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)


def jsonable(value):
    """Datetimes, Decimals and other database types as plain JSON values."""
    return json.loads(json.dumps(value, default=lambda v: float(v) if isinstance(v, Decimal) else str(v)))


def build_mcp_server(service: OpsService) -> MCPServer:
    server = MCPServer("data-platform-ops-tools", instructions=INSTRUCTIONS)

    def call(tool: str, run_id: int | None, arguments: dict, fn):
        with service.audited("mcp", tool, arguments, run_id):
            return jsonable(fn())

    @server.tool(annotations=READ_ONLY)
    def analyze_spark_job(app_id: str, pipeline: str = "", run_id: int | None = None) -> dict:
        """Run the deterministic Spark analyzer on an application's event log and executor logs.
        Returns the primary diagnosis, findings with evidence, and the recommended config changes."""
        return call("analyze_spark_job", run_id, {"app_id": app_id, "pipeline": pipeline},
                    lambda: service.analyze_spark_job(app_id, pipeline or None, run_id))

    @server.tool(annotations=READ_ONLY)
    def get_pipeline_config(pipeline: str, run_id: int | None = None) -> dict:
        """The pipeline's checked-in Spark config, its data quality rules, downstream consumers and paused state."""
        return call("get_pipeline_config", run_id, {"pipeline": pipeline}, lambda: service.pipeline_config(pipeline))

    @server.tool(annotations=READ_ONLY)
    def sample_failed_rows(pipeline: str, rule: str, limit: int = 20, run_id: int | None = None) -> dict:
        """Recent rows quarantined by a quality rule, with a per-field profile (presence, nulls, top values)
        and the fields seen in other quarantined rows, to tell a schema change from bad data."""
        return call("sample_failed_rows", run_id, {"pipeline": pipeline, "rule": rule, "limit": limit},
                    lambda: service.sample_failed_rows(pipeline, rule, limit))

    @server.tool(annotations=READ_ONLY)
    def get_failure_history(pipeline: str, rule: str, hours: int = 24, run_id: int | None = None) -> dict:
        """Hourly checked rows, failed rows and failed % for one rule: sudden step change or slow drift?"""
        return call("get_failure_history", run_id, {"pipeline": pipeline, "rule": rule, "hours": hours},
                    lambda: {"history": service.failure_history(pipeline, rule, hours)})

    @server.tool(annotations=READ_ONLY)
    def check_proposal(kind: str, pipeline: str, changes: dict, run_id: int | None = None) -> dict:
        """Dry-run the guardrails without storing anything.
        kind=spark_conf: changes={"spark_conf": {"spark.key": "value"}}.
        kind=dq_action: changes={"action": "monitor|open_incident|pause_downstream|adjust_rule", "rule": "...",
        "rule_change": {"max_failed_pct": 1.0}} (rule_change only for adjust_rule).
        Returns passed, violations and warnings."""
        return call("check_proposal", run_id, {"kind": kind, "pipeline": pipeline, "changes": changes},
                    lambda: service.check_proposal(kind, pipeline, changes, run_id))

    return server

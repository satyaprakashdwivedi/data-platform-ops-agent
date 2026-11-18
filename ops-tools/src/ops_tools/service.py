"""The operations the agent and the n8n workflows can perform. REST (api.py) and MCP (mcp_server.py)
are thin wrappers around this class, so both are audited and guarded the same way."""
import hashlib
import json
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from . import github_pr, notify, quality, spark_analysis
from .db import Database, as_json
from .guardrails import check_dq_action, check_spark_conf, load_policy
from .pipelines import PipelineRepository, unified_diff, with_rule_threshold, with_spark_conf
from .settings import Settings


class ProposalError(Exception):
    """A proposal cannot move to the requested state (wrong status, unknown id...)."""


class OpsService:
    def __init__(self, settings: Settings, db: Database):
        self.settings = settings
        self.db = db
        self.pipelines = PipelineRepository(settings.pipelines_dir)

    @property
    def policy(self) -> dict:
        return load_policy(self.settings.policy_file)  # re-read so policy edits apply without a restart

    # ------------------------------------------------------------------ audit

    def start_run(self, workflow: str, subject: str | None = None, trigger: dict | None = None,
                  n8n_execution_id: str | None = None) -> dict:
        return self.db.fetch_one(
            """INSERT INTO ops.agent_run (workflow, subject, trigger_payload, n8n_execution_id)
               VALUES (%s, %s, %s, %s) RETURNING run_id, started_at""",
            (workflow, subject, as_json(trigger or {}), n8n_execution_id))

    def finish_run(self, run_id: int, status: str = "completed") -> dict:
        row = self.db.fetch_one(
            """UPDATE ops.agent_run SET status = %s, finished_at = now() WHERE run_id = %s
               RETURNING run_id, status, finished_at""", (status, run_id))
        if not row:
            raise ProposalError(f"run {run_id} not found")
        return row

    def fail_runs_of_execution(self, n8n_execution_id: str, error: str = "") -> dict:
        """Called by the error workflow: close any run a crashed n8n execution left open."""
        rows = self.db.fetch_all(
            """UPDATE ops.agent_run SET status = 'failed', finished_at = now(),
                      trigger_payload = trigger_payload || jsonb_build_object('error', %s::text)
               WHERE n8n_execution_id = %s AND status = 'running' RETURNING run_id""", (error[:500], n8n_execution_id))
        return {"failed_runs": [r["run_id"] for r in rows]}

    @contextmanager
    def audited(self, channel: str, tool: str, arguments: dict, run_id: int | None):
        started, error = time.monotonic(), None
        try:
            yield
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            if run_id is not None and not self.db.fetch_one("SELECT 1 AS ok FROM ops.agent_run WHERE run_id = %s",
                                                            (run_id,)):
                run_id = None
            self.db.fetch_one(
                """INSERT INTO ops.tool_call (run_id, channel, tool, arguments, succeeded, error, duration_ms)
                   VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING call_id""",
                (run_id, channel, tool, as_json(arguments), error is None, error,
                 int((time.monotonic() - started) * 1000)))

    # ------------------------------------------------------------------ spark

    def analyze_spark_job(self, app_id: str, pipeline: str | None = None, run_id: int | None = None) -> dict:
        files, source = spark_analysis.find_event_logs(app_id, self.settings.spark_event_log_dir,
                                                       self.settings.spark_history_server_url)
        result = spark_analysis.run_analyzer(files)
        compact = spark_analysis.for_agent(result)
        pipeline = pipeline or result["summary"].get("app_name")
        if pipeline in self.pipelines.names():
            # the pipeline's checked-in config is what a PR would change, so guardrails compare against it
            compact["current_config"] = {**compact["current_config"],
                                         **{k: str(v) for k, v in self.pipelines.config(pipeline)["spark_conf"].items()}}
        row = self.db.fetch_one(
            """INSERT INTO ops.spark_analysis (run_id, pipeline, app_id, app_name, diagnosis, findings,
                                              recommended, summary, idle_core_hours)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING analysis_id""",
            (run_id, pipeline, app_id, result["summary"].get("app_name"), compact["diagnosis"],
             as_json(compact["findings"]), as_json(compact["recommended"]), as_json(compact["summary"]),
             compact["idle_core_hours"]))
        return {"analysis_id": row["analysis_id"], "pipeline": pipeline, "source": source, **compact}

    def review_recent_jobs(self, limit: int = 20, run_id: int | None = None) -> dict:
        """Analyse every application in the event log directory and rank by idle core-hours."""
        jobs = []
        log_dir = self.settings.spark_event_log_dir
        for app_dir in sorted(p for p in log_dir.iterdir() if p.is_dir())[:limit] if log_dir.exists() else []:
            analysis = self.analyze_spark_job(app_dir.name, run_id=run_id)
            jobs.append({"app_id": app_dir.name, "pipeline": analysis["pipeline"],
                         "diagnosis": analysis["diagnosis"], "idle_core_hours": analysis["idle_core_hours"],
                         "duration_min": round((analysis["summary"].get("duration_ms") or 0) / 60000, 1),
                         "top_findings": [f["title"] for f in analysis["findings"] if f["severity"] == "high"][:3],
                         "recommended_changes": len(analysis["recommended"])})
        jobs.sort(key=lambda j: j["idle_core_hours"], reverse=True)
        return {"jobs_reviewed": len(jobs), "total_idle_core_hours": round(sum(j["idle_core_hours"] for j in jobs), 1),
                "jobs": jobs}

    # ------------------------------------------------------------------ quality

    def quality_incidents(self, window_minutes: int | None = None) -> dict:
        incidents = quality.find_incidents(self.db, self.pipelines, self.policy, window_minutes)
        return {"incident_count": len(incidents), "incidents": incidents}

    def sample_failed_rows(self, pipeline: str, rule: str, limit: int = 20) -> dict:
        return quality.sample_failed_rows(self.db, pipeline, rule, limit)

    def failure_history(self, pipeline: str, rule: str, hours: int = 24) -> list[dict]:
        return quality.failure_history(self.db, pipeline, rule, hours)

    def pipeline_config(self, pipeline: str) -> dict:
        control = self.db.fetch_one("SELECT paused, reason FROM ops.pipeline_control WHERE pipeline = %s",
                                    (pipeline,))
        return {"pipeline": pipeline, "config": self.pipelines.config(pipeline),
                "quality_rules": self.pipelines.rules(pipeline),
                "paused": bool(control and control["paused"]), "pause_reason": control and control["reason"]}

    # ------------------------------------------------------------------ proposals

    def check_proposal(self, kind: str, pipeline: str, changes: dict, run_id: int | None = None) -> dict:
        """Dry-run the guardrails: the agent uses this to fix a proposal before handing it to a human."""
        return self._guardrail_report(kind, pipeline, changes, run_id).as_dict()

    def _guardrail_report(self, kind: str, pipeline: str, changes: dict, run_id: int | None):
        if kind == "spark_conf":
            analysis = self.db.fetch_one(
                """SELECT recommended FROM ops.spark_analysis WHERE pipeline = %s
                   ORDER BY (run_id = %s) DESC NULLS LAST, analysed_at DESC LIMIT 1""", (pipeline, run_id))
            current = {k: str(v) for k, v in self.pipelines.config(pipeline).get("spark_conf", {}).items()}
            report = check_spark_conf(changes.get("spark_conf", {}), current,
                                      analysis["recommended"] if analysis else [], self.policy)
            if not analysis:
                report.violations.append("no analyzer run exists for this pipeline; analyse the job first")
            return report
        if kind == "dq_action":
            return check_dq_action(changes, self.pipelines.rules(pipeline), self.policy)
        raise ProposalError(f"unknown proposal kind {kind!r}")

    def create_proposal(self, kind: str, pipeline: str, changes: dict, diagnosis: str = "", rationale: str = "",
                        confidence: float | None = None, run_id: int | None = None) -> dict:
        """Validate an agent proposal against policy and store it. Never applies anything."""
        report = self._guardrail_report(kind, pipeline, changes, run_id)
        status = "pending_approval" if report.passed else "rejected_by_guardrails"
        key = hashlib.sha256(json.dumps([kind, pipeline, changes], sort_keys=True).encode()).hexdigest()[:32]
        if report.passed:
            existing = self.db.fetch_one(
                """SELECT * FROM ops.proposal WHERE idempotency_key = %s
                   AND status IN ('pending_approval', 'approved')""", (key,))
            if existing:
                return {**self._public(existing), "duplicate": True}
        row = self.db.fetch_one(
            """INSERT INTO ops.proposal (run_id, kind, pipeline, changes, diagnosis, rationale, confidence, status,
                                         guardrail_report, idempotency_key)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
            (run_id, kind, pipeline, as_json(changes), diagnosis, rationale, confidence, status,
             as_json(report.as_dict()), key))
        return {**self._public(row), "duplicate": False}

    def get_proposal(self, proposal_id: int) -> dict:
        row = self.db.fetch_one("SELECT * FROM ops.proposal WHERE proposal_id = %s", (proposal_id,))
        if not row:
            raise ProposalError(f"proposal {proposal_id} not found")
        return self._public(row)

    def decide(self, proposal_id: int, decision: str, decided_by: str = "unknown", comment: str = "") -> dict:
        """Record a human decision. Only a pending proposal can be decided; old ones expire instead."""
        proposal = self.get_proposal(proposal_id)
        if proposal["status"] != "pending_approval":
            raise ProposalError(f"proposal {proposal_id} is {proposal['status']}, not pending_approval")
        expiry = proposal["created_at"] + timedelta(hours=self.policy["approval"]["expire_after_hours"])
        if decision not in ("approve", "decline", "expire"):
            raise ProposalError("decision must be approve, decline or expire")
        status = {"approve": "approved", "decline": "declined", "expire": "expired"}[decision]
        if datetime.now(timezone.utc) > expiry:
            status, comment = "expired", f"approval window closed at {expiry.isoformat()}"
        row = self.db.fetch_one(
            """UPDATE ops.proposal SET status = %s, decided_by = %s, decision_comment = %s, decided_at = now()
               WHERE proposal_id = %s AND status = 'pending_approval' RETURNING *""",
            (status, decided_by, comment, proposal_id))
        if not row:
            raise ProposalError(f"proposal {proposal_id} was decided concurrently")
        return self._public(row)

    def apply(self, proposal_id: int) -> dict:
        """Carry out an approved proposal. Safe to retry: an applied proposal returns its existing result."""
        proposal = self.get_proposal(proposal_id)
        if proposal["status"] == "applied":
            return {**proposal, "already_applied": True}
        if proposal["status"] != "approved":
            raise ProposalError(f"proposal {proposal_id} is {proposal['status']}; only approved proposals are applied")
        try:
            applied_ref = self._carry_out(proposal)
            status = "applied"
        except Exception as exc:  # recorded on the proposal and re-raised for the workflow's error branch
            applied_ref, status = f"error: {exc}", "apply_failed"
        row = self.db.fetch_one(
            "UPDATE ops.proposal SET status = %s, applied_ref = %s WHERE proposal_id = %s RETURNING *",
            (status, applied_ref, proposal_id))
        if status == "apply_failed":
            raise ProposalError(applied_ref)
        return {**self._public(row), "already_applied": False}

    def _carry_out(self, proposal: dict) -> str:
        pipeline, changes, pid = proposal["pipeline"], proposal["changes"], proposal["proposal_id"]
        body = (f"**Diagnosis:** {proposal['diagnosis']}\n\n**Why:** {proposal['rationale']}\n\n"
                f"**Guardrails:** passed. {'; '.join(proposal['guardrail_report'].get('warnings', [])) or ''}\n\n"
                f"**Approved by:** {proposal['decided_by']} ({proposal['decided_at']:%Y-%m-%d %H:%M} UTC)\n\n"
                f"Opened by data-platform-ops-agent, proposal #{pid}, run #{proposal['run_id']}.")
        if proposal["kind"] == "spark_conf":
            before = self.pipelines.config_text(pipeline)
            after = with_spark_conf(before, changes["spark_conf"])
            path = self.pipelines.relative_path(pipeline)
            return github_pr.publish_change(self.settings, f"ops-agent/proposal-{pid}", path, after,
                                            unified_diff(before, after, path),
                                            f"[ops-agent] {pipeline}: tune Spark config ({proposal['diagnosis']})", body)
        action = changes["action"]
        if action == "adjust_rule":
            before = self.pipelines.rules_text(pipeline) or ""
            after = with_rule_threshold(before, changes["rule"], float(changes["rule_change"]["max_failed_pct"]))
            path = self.pipelines.relative_path(pipeline, rules=True)
            return github_pr.publish_change(self.settings, f"ops-agent/proposal-{pid}", path, after,
                                            unified_diff(before, after, path),
                                            f"[ops-agent] {pipeline}: adjust rule {changes['rule']}", body)
        if action == "pause_downstream":
            self.db.fetch_one(
                """INSERT INTO ops.pipeline_control (pipeline, paused, reason, updated_by) VALUES (%s, true, %s, %s)
                   ON CONFLICT (pipeline) DO UPDATE SET paused = true, reason = EXCLUDED.reason,
                   updated_by = EXCLUDED.updated_by, updated_at = now() RETURNING pipeline""",
                (pipeline, f"proposal #{pid}: {proposal['diagnosis']}", proposal["decided_by"]))
            return f"pipeline_control:{pipeline}:paused"
        if action == "open_incident":
            sent = notify.send(self.db, self.settings, f"Incident: {pipeline} data quality",
                               f"{proposal['diagnosis']}\n\n{proposal['rationale']}", run_id=proposal["run_id"])
            return f"incident:notification:{sent['notification_id']}"
        return "monitor:no-op"

    def notify(self, title: str, body: str, links: dict | None = None, run_id: int | None = None) -> dict:
        return notify.send(self.db, self.settings, title, body, links, run_id)

    @staticmethod
    def _public(row: dict) -> dict:
        return {k: v for k, v in row.items() if k != "idempotency_key"}

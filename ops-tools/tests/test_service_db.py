"""Service tests against Postgres: proposal lifecycle, idempotency, audit and quality incidents."""
from datetime import timedelta
from pathlib import Path

import pytest

from ops_tools.service import ProposalError

DAILY = "application_1700000000000_0042"


def analysed(service):
    run = service.start_run("test", DAILY)
    analysis = service.analyze_spark_job(DAILY, run_id=run["run_id"])
    return run["run_id"], analysis


def test_proposal_lifecycle_writes_a_patch(service):
    run_id, analysis = analysed(service)
    assert analysis["diagnosis"] == "container_memory_kill"
    proposal = service.create_proposal("spark_conf", "daily_sales_join",
                                       {"spark_conf": {"spark.executor.memoryOverhead": "2048m"}},
                                       "container kill", "exit 137", 0.9, run_id)
    assert proposal["status"] == "pending_approval"
    with pytest.raises(ProposalError):
        service.apply(proposal["proposal_id"])  # not approved yet
    service.decide(proposal["proposal_id"], "approve", "alice")
    applied = service.apply(proposal["proposal_id"])
    patch = Path(applied["applied_ref"].removeprefix("patch:"))
    assert applied["status"] == "applied" and "spark.executor.memoryOverhead: 2048m" in patch.read_text()
    again = service.apply(proposal["proposal_id"])
    assert again["already_applied"] and again["applied_ref"] == applied["applied_ref"]


def test_same_open_proposal_is_reused(service):
    run_id, _ = analysed(service)
    changes = {"spark_conf": {"spark.executor.memoryOverhead": "2048m"}}
    first = service.create_proposal("spark_conf", "daily_sales_join", changes, run_id=run_id)
    second = service.create_proposal("spark_conf", "daily_sales_join", changes, run_id=run_id)
    assert second["duplicate"] and second["proposal_id"] == first["proposal_id"]


def test_guardrail_rejection_is_stored_and_never_applied(service):
    run_id, _ = analysed(service)
    rejected = service.create_proposal("spark_conf", "daily_sales_join",
                                       {"spark_conf": {"spark.executor.memory": "64g"}}, run_id=run_id)
    assert rejected["status"] == "rejected_by_guardrails" and rejected["guardrail_report"]["violations"]
    with pytest.raises(ProposalError):
        service.decide(rejected["proposal_id"], "approve", "mallory")


def test_old_proposals_expire_instead_of_being_approved(service):
    run_id, _ = analysed(service)
    proposal = service.create_proposal("spark_conf", "daily_sales_join",
                                       {"spark_conf": {"spark.sql.adaptive.enabled": "true"}}, run_id=run_id)
    with service.db.connection() as conn:
        conn.execute("UPDATE ops.proposal SET created_at = now() - %s WHERE proposal_id = %s",
                     (timedelta(hours=25), proposal["proposal_id"]))
    assert service.decide(proposal["proposal_id"], "approve", "late-bob")["status"] == "expired"


def test_tool_calls_are_audited(service):
    run_id, _ = analysed(service)
    with service.audited("mcp", "check_proposal", {"x": 1}, run_id):
        pass
    with pytest.raises(RuntimeError):
        with service.audited("mcp", "boom", {}, run_id):
            raise RuntimeError("tool failed")
    rows = service.db.fetch_all("SELECT tool, succeeded FROM ops.tool_call WHERE run_id = %s ORDER BY call_id",
                                (run_id,))
    assert [(r["tool"], r["succeeded"]) for r in rows] == [("check_proposal", True), ("boom", False)]


def test_quality_incident_and_pause(service, repo):
    with service.db.connection() as conn:
        conn.execute((repo / "demo" / "seed_quality.sql").read_text())
    incidents = service.quality_incidents()["incidents"]
    assert [(i["pipeline"], i["rule"]) for i in incidents] == [("orders_enrichment", "currency_known")]
    samples = service.sample_failed_rows("orders_enrichment", "currency_known", 10)
    assert "currency_code" in samples["field_profile"]
    assert "currency_code" not in samples["fields_seen_in_other_rules"]
    proposal = service.create_proposal("dq_action", "orders_enrichment",
                                       {"action": "pause_downstream", "rule": "currency_known"}, "schema change")
    service.decide(proposal["proposal_id"], "approve", "alice")
    assert service.apply(proposal["proposal_id"])["applied_ref"] == "pipeline_control:orders_enrichment:paused"
    assert service.pipeline_config("orders_enrichment")["paused"]


def test_failed_execution_closes_its_runs(service):
    run = service.start_run("spark-failure-triage", DAILY, n8n_execution_id="42")
    assert service.fail_runs_of_execution("42", "boom") == {"failed_runs": [run["run_id"]]}

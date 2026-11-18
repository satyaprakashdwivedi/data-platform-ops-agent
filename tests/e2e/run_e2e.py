#!/usr/bin/env python3
"""End-to-end test of the whole stack (n8n queue mode + ops-tools + Postgres + Redis + scripted model).

    ./scripts/init_env.sh tests/e2e/.env.test
    docker compose --env-file tests/e2e/.env.test --profile test up -d --build
    python3 tests/e2e/run_e2e.py tests/e2e/.env.test

Drives every workflow through its real webhooks, approves or declines like a person would (basic auth on the
signed resume link), and checks the outcome in the ops database. Standard library only.
"""
import base64
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OPS, N8N = "http://localhost:8000", "http://localhost:5678"


def load_env(path: str) -> dict:
    env = {}
    for line in Path(path).read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            env[key] = value
    return env


ENV = load_env(sys.argv[1] if len(sys.argv) > 1 else "tests/e2e/.env.test")


def request(method: str, url: str, body=None, headers=None, expect_json=True):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            payload = response.read().decode()
            return response.status, (json.loads(payload) if expect_json and payload else payload)
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()
    except (urllib.error.URLError, ConnectionError):  # service still starting
        return 0, None


def ops(method: str, path: str, body=None):
    return request(method, OPS + path, body, {"Authorization": f"Bearer {ENV['OPS_TOOLS_TOKEN']}"})


def trigger(path: str, body: dict):
    status, reply = request("POST", f"{N8N}/webhook/{path}", body, {"X-Webhook-Token": ENV["N8N_WEBHOOK_TOKEN"]})
    assert status == 200, (path, status, reply)


def sql(query: str) -> str:
    result = subprocess.run(["docker", "compose", "--env-file", sys.argv[1] if len(sys.argv) > 1 else
                             "tests/e2e/.env.test", "exec", "-T", "postgres", "psql", "-U", "ops_tools", "-d", "ops",
                             "-tAc", query], cwd=ROOT, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def wait_for(description: str, check, timeout: int = 90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = check()
        if value:
            return value
        time.sleep(2)
    raise AssertionError(f"timed out waiting for {description}")


def latest_notification(title_contains: str, after_id: int):
    _, notes = ops("GET", "/notifications?limit=20")
    return next((n for n in notes if n["notification_id"] > after_id and title_contains in n["title"]), None)


def last_notification_id() -> int:
    _, notes = ops("GET", "/notifications?limit=1")
    return notes[0]["notification_id"] if notes else 0


def decide(link: str, by: str):
    auth = base64.b64encode(f"{ENV.get('APPROVER_USER', 'approver')}:{ENV['APPROVER_PASSWORD']}".encode()).decode()
    status, _ = request("GET", f"{link}&by={by}", headers={"Authorization": f"Basic {auth}"}, expect_json=False)
    assert status == 200, f"approval link returned {status}"


def proposal_id_from(note: dict) -> int:
    return int(note["body"].rsplit("Proposal #", 1)[1].split()[0])


def wait_for_status(proposal_id: int, wanted: str) -> dict:
    last = {}

    def reached():
        last.update(ops("GET", f"/proposals/{proposal_id}")[1])
        return last if last["status"] == wanted else None
    try:
        return wait_for(f"proposal {proposal_id} -> {wanted}", reached)
    except AssertionError:
        raise AssertionError(f"proposal {proposal_id} stayed {last.get('status')}: {last.get('applied_ref')}")


# ------------------------------------------------------------------------------------------------ scenarios

def test_security():
    status, _ = request("GET", f"{OPS}/notifications")
    assert status == 401, "ops-tools must reject calls without the bearer token"
    status, _ = request("POST", f"{N8N}/webhook/spark-job-failed", {"app_id": "x"})
    assert status in (401, 403), f"n8n webhook must require the token, got {status}"
    print("ok  security: ops-tools and webhooks reject unauthenticated calls")


def test_guardrails_block_unsafe_change():
    status, run = ops("POST", "/runs", {"workflow": "e2e", "subject": "guardrails"})
    status, proposal = ops("POST", "/proposals", {
        "kind": "spark_conf", "pipeline": "daily_sales_join",
        "changes": {"spark_conf": {"spark.executor.memory": "64g", "spark.hadoop.fs.s3a.secret.key": "x"}},
        "diagnosis": "hallucinated fix"})
    assert status == 200 and proposal["status"] == "rejected_by_guardrails", proposal
    assert any("not on the allowed list" in v for v in proposal["guardrail_report"]["violations"])
    status, _ = ops("POST", f"/proposals/{proposal['proposal_id']}/apply")
    assert status == 409, "a rejected proposal must never be applied"
    print("ok  guardrails: oversized heap and non-allow-listed key rejected, apply refused")


def test_spark_triage_approved():
    seen = last_notification_id()
    trigger("spark-job-failed", {"app_id": "application_1700000000000_0042", "pipeline": "daily_sales_join"})
    note = wait_for("approval request", lambda: latest_notification("Spark fix for daily_sales_join", seen))
    assert "spark.executor.memoryOverhead" in note["body"], note["body"]
    assert "spark.executor.memory =" not in note["body"], "container kill must not be fixed with more heap"
    pid = proposal_id_from(note)
    decide(note["links"]["Approve"], "e2e-approver")
    proposal = wait_for_status(pid, "applied")
    patch = Path(proposal["applied_ref"].removeprefix("patch:").replace("/app/outbox", str(ROOT / "outbox")))
    assert patch.exists() and "+  spark.executor.memoryOverhead: 2048m" in patch.read_text(), patch
    run_status = wait_for("run finished", lambda: sql(f"SELECT status FROM ops.agent_run WHERE run_id = "
                                                      f"{proposal['run_id']} AND status <> 'running'"))
    assert run_status == "applied", run_status
    mcp_calls = sql(f"SELECT count(*) FROM ops.tool_call WHERE run_id = {proposal['run_id']} AND channel = 'mcp'")
    assert int(mcp_calls) >= 1, "the agent's MCP tool calls must be audited against the run"
    print(f"ok  spark triage: container kill -> memoryOverhead, approved, patch {patch.name}, "
          f"{mcp_calls} audited MCP call(s)")


def test_spark_triage_declined():
    seen = last_notification_id()
    trigger("spark-job-failed", {"app_id": "application_1700000000000_0057", "pipeline": "orders_enrichment"})
    note = wait_for("approval request", lambda: latest_notification("Spark fix for orders_enrichment", seen))
    assert "heap oom" in note["body"] and "spark.executor.memory = 6g" in note["body"], note["body"]
    pid = proposal_id_from(note)
    decide(note["links"]["Decline"], "e2e-approver")
    proposal = wait_for_status(pid, "declined")
    assert proposal["applied_ref"] is None
    print("ok  spark triage: heap OOM -> executor.memory 6g, declined, nothing applied")


def test_quality_incident():
    subprocess.run(["docker", "compose", "--env-file", sys.argv[1] if len(sys.argv) > 1 else "tests/e2e/.env.test",
                    "exec", "-T", "postgres", "psql", "-q", "-U", "ops_tools", "-d", "ops"],
                   stdin=open(ROOT / "demo" / "seed_quality.sql"), cwd=ROOT, check=True)
    seen = last_notification_id()
    trigger("check-quality-now", {})
    note = wait_for("approval request", lambda: latest_notification("Data quality incident on orders_enrichment", seen))
    assert "upstream_schema_change" in note["body"] and "pause_downstream" in note["body"], note["body"]
    pid = proposal_id_from(note)
    decide(note["links"]["Approve"], "e2e-approver")
    wait_for_status(pid, "applied")
    paused = sql("SELECT paused FROM ops.pipeline_control WHERE pipeline = 'orders_enrichment'")
    assert paused == "t", paused
    print("ok  data quality: currency_code schema change -> pause downstream, approved, pipeline paused")


def test_weekly_review():
    seen = last_notification_id()
    trigger("run-weekly-review", {})
    note = wait_for("weekly review", lambda: latest_notification("Weekly Spark tuning review", seen))
    assert "clickstream_sessionize" in note["body"], note["body"]
    print("ok  weekly review: ranked jobs by idle core-hours and posted the review")


if __name__ == "__main__":
    wait_for("ops-tools", lambda: request("GET", f"{OPS}/health")[0] == 200, timeout=180)
    wait_for("n8n", lambda: request("GET", f"{N8N}/healthz")[0] == 200, timeout=180)
    for test in (test_security, test_guardrails_block_unsafe_change, test_spark_triage_approved,
                 test_spark_triage_declined, test_quality_incident, test_weekly_review):
        test()
    print("all end-to-end scenarios passed")

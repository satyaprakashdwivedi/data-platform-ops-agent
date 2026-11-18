"""Lint the generated n8n workflows: up to date, wired correctly, guarded and free of secrets."""
import json
import re
import subprocess
import sys

import pytest

FORBIDDEN = re.compile(r"sk-ant-|ghp_|github_pat_|xox[bp]-|BEGIN [A-Z ]*PRIVATE KEY", re.I)


@pytest.fixture(scope="module")
def workflows(repo):
    return {p.name: json.loads(p.read_text()) for p in sorted((repo / "n8n" / "workflows").glob("*.json"))}


def test_generated_json_is_up_to_date(repo):
    result = subprocess.run([sys.executable, str(repo / "n8n" / "build_workflows.py"), "--check"],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_connections_reference_existing_nodes(workflows):
    for name, wf in workflows.items():
        nodes = {n["name"] for n in wf["nodes"]}
        for source, outputs in wf["connections"].items():
            assert source in nodes, (name, source)
            for branches in outputs.values():
                for branch in branches:
                    for link in branch:
                        assert link["node"] in nodes, (name, link)


def test_every_workflow_reports_errors_and_has_no_secrets(workflows):
    for name, wf in workflows.items():
        if wf["id"] != "opsErrorHandler1":
            assert wf["settings"]["errorWorkflow"] == "opsErrorHandler1", name
        assert not FORBIDDEN.search(json.dumps(wf)), f"{name} contains something that looks like a secret"


def test_webhooks_and_approvals_are_authenticated(workflows):
    for name, wf in workflows.items():
        for node in wf["nodes"]:
            if node["type"] == "n8n-nodes-base.webhook":
                assert node["parameters"]["authentication"] == "headerAuth", (name, node["name"])
            if node["type"] == "n8n-nodes-base.wait":
                assert node["parameters"]["incomingAuthentication"] == "basicAuth", (name, node["name"])
                assert node["parameters"]["limitWaitTime"] is True, "approvals must expire"


def test_agents_cannot_apply_changes(workflows):
    """The agent's MCP tool list is read-and-check only; writes happen in deterministic workflow steps."""
    allowed = {"analyze_spark_job", "get_pipeline_config", "sample_failed_rows", "get_failure_history",
               "check_proposal"}
    for name, wf in workflows.items():
        for node in wf["nodes"]:
            if node["type"].endswith("mcpClientTool"):
                assert node["parameters"]["include"] == "selected"
                assert set(node["parameters"]["includeTools"]) <= allowed, (name, node["parameters"]["includeTools"])


def test_changes_only_after_approval(workflows):
    """In every workflow, the apply step is reachable only through the 'Approved?' true branch."""
    for name, wf in workflows.items():
        appliers = [n["name"] for n in wf["nodes"] if "/apply" in n["parameters"].get("url", "")]
        for applier in appliers:
            parents = [src for src, outs in wf["connections"].items()
                       for i, branch in enumerate(outs.get("main", [])) for link in branch if link["node"] == applier]
            assert parents == ["Approved?"], (name, applier, parents)
            approved_true = wf["connections"]["Approved?"]["main"][0]
            assert any(link["node"] == applier for link in approved_true)

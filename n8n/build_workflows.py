#!/usr/bin/env python3
"""Generate the n8n workflow JSON files in n8n/workflows/ from code and the prompts in n8n/prompts/.

    python3 n8n/build_workflows.py          # write the files
    python3 n8n/build_workflows.py --check  # CI: fail if the committed JSON is stale

Keeping workflows as generated code gives reviewable diffs, shared conventions (auth, retries, audit run ids,
error workflow) and lets CI lint them, instead of hand-editing exported JSON.
"""
from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROMPTS = ROOT / "prompts"
OUT = ROOT / "workflows"

OPS = "={{ $env.OPS_TOOLS_URL }}"
MODEL_ID = "={{ $env.ANTHROPIC_MODEL || 'claude-sonnet-5-5' }}"

# Credentials created by n8n/init/import.sh from environment variables (never stored in git).
CREDENTIALS = {
    "anthropic": {"anthropicApi": {"id": "opsAnthropic0001", "name": "Anthropic"}},
    "ops_tools": {"httpBearerAuth": {"id": "opsToolsBearer01", "name": "Ops tools token"}},
    "webhook": {"httpHeaderAuth": {"id": "opsWebhookToken1", "name": "Webhook token"}},
    "approvers": {"httpBasicAuth": {"id": "opsApprovers0001", "name": "Approvers"}},
}
ERROR_WORKFLOW_ID = "opsErrorHandler1"


def stable_id(*parts: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "data-platform-ops-agent/" + "/".join(parts)))


def prompt(name: str) -> str:
    return (PROMPTS / f"{name}.md").read_text().strip()


class Workflow:
    def __init__(self, workflow_id: str, name: str, description: str):
        self.id, self.name, self.description = workflow_id, name, description
        self.nodes, self.connections = [], {}
        self.column = 0

    def add(self, name: str, node_type: str, version: float, parameters: dict, position: tuple | None = None,
            credentials: str | None = None, **extra) -> str:
        if position is None:
            position = (self.column * 240, 0)
            self.column += 1
        node = {"id": stable_id(self.id, name), "name": name, "type": node_type, "typeVersion": version,
                "position": list(position), "parameters": parameters, **extra}
        if credentials:
            node["credentials"] = CREDENTIALS[credentials]
        self.nodes.append(node)
        return name

    def link(self, source: str, target: str, output: int = 0, kind: str = "main"):
        outputs = self.connections.setdefault(source, {}).setdefault(kind, [])
        while len(outputs) <= output:
            outputs.append([])
        outputs[output].append({"node": target, "type": kind, "index": 0})

    def chain(self, *names: str):
        for source, target in zip(names, names[1:]):
            self.link(source, target)

    def to_json(self) -> dict:
        return {"id": self.id, "name": self.name, "active": True, "nodes": self.nodes,
                "connections": self.connections, "pinData": {},
                "settings": {"executionOrder": "v1", "saveManualExecutions": True,
                             "saveDataErrorExecution": "all", "saveDataSuccessExecution": "all",
                             "callerPolicy": "workflowsFromSameOwner",
                             **({"errorWorkflow": ERROR_WORKFLOW_ID} if self.id != ERROR_WORKFLOW_ID else {})},
                "meta": {"templateCredsSetupCompleted": True, "description": self.description},
                "tags": []}


# ------------------------------------------------------------------------------------------- node helpers

def ops_call(wf: Workflow, name: str, method: str, path: str, body: str | None = None, run_id: str | None = None,
             position=None, retry: bool = True) -> str:
    """HTTP call to ops-tools with the bearer token, the audit run id header and retries on transient errors."""
    parameters = {"method": method, "url": f"{OPS}{path}" if not path.startswith("=") else
                  "={{ $env.OPS_TOOLS_URL }}" + path[1:],
                  "authentication": "genericCredentialType", "genericAuthType": "httpBearerAuth",
                  "sendHeaders": bool(run_id),
                  "options": {"timeout": 120000}}
    if run_id:
        parameters["headerParameters"] = {"parameters": [{"name": "X-Run-Id", "value": run_id}]}
    if body is not None:
        parameters.update({"sendBody": True, "contentType": "json", "specifyBody": "json", "jsonBody": body})
    extra = {"retryOnFail": True, "maxTries": 3, "waitBetweenTries": 3000} if retry else {}
    return wf.add(name, "n8n-nodes-base.httpRequest", 4.5, parameters, position, credentials="ops_tools", **extra)


def webhook(wf: Workflow, name: str, path: str, position=None) -> str:
    return wf.add(name, "n8n-nodes-base.webhook", 2.2,
                  {"httpMethod": "POST", "path": path, "authentication": "headerAuth", "responseMode": "onReceived",
                   "options": {}},
                  position, credentials="webhook", webhookId=stable_id(wf.id, name, "webhook"))


def condition(wf: Workflow, name: str, left: str, operation: str, right=None, kind: str = "string",
              position=None) -> str:
    operator = {"type": kind, "operation": operation}
    if right is None:
        operator["singleValue"] = True
    return wf.add(name, "n8n-nodes-base.if", 2.2, {
        "conditions": {"options": {"caseSensitive": True, "leftValue": "", "typeValidation": "loose", "version": 2},
                       "conditions": [{"id": stable_id(wf.id, name, "cond"), "leftValue": left,
                                       "rightValue": "" if right is None else right, "operator": operator}],
                       "combinator": "and"},
        "looseTypeValidation": True, "options": {}}, position)


def agent(wf: Workflow, name: str, system_prompt: str, user_prompt: str, schema: dict | None, position,
          tools: list[str]) -> str:
    """AI Agent node with Claude, the ops-tools MCP server and (optionally) a structured output parser.
    One agent per workflow, so the sub-nodes have short fixed names (n8n prefixes MCP tool names with them)."""
    x, y = position
    wf.add(name, "@n8n/n8n-nodes-langchain.agent", 3.1, {
        "promptType": "define", "text": user_prompt, "hasOutputParser": schema is not None,
        "options": {"systemMessage": system_prompt, "maxIterations": 10}}, position)
    model = wf.add("Claude", "@n8n/n8n-nodes-langchain.lmChatAnthropic", 1.6, {
        "model": {"__rl": True, "mode": "id", "value": MODEL_ID},
        "options": {"maxTokensToSample": 4096, "temperature": 0}}, (x - 120, y + 220), credentials="anthropic")
    wf.link(model, name, kind="ai_languageModel")
    mcp = wf.add("Ops tools", "@n8n/n8n-nodes-langchain.mcpClientTool", 1.4, {
        "endpointUrl": "={{ $env.OPS_TOOLS_URL }}/mcp", "serverTransport": "httpStreamable",
        "authentication": "bearerAuth", "include": "selected", "includeTools": tools, "options": {"timeout": 120000}},
        (x + 40, y + 220), credentials="ops_tools")
    wf.link(mcp, name, kind="ai_tool")
    if schema is not None:
        parser = wf.add("Output schema", "@n8n/n8n-nodes-langchain.outputParserStructured", 1.3, {
            "schemaType": "manual", "inputSchema": json.dumps(schema, indent=2)}, (x + 200, y + 220))
        wf.link(parser, name, kind="ai_outputParser")
    return name


def fill(template: str, **values: str) -> str:
    for key, value in values.items():
        template = template.replace("{{" + key + "}}", value)
    return "=" + template


def approval_steps(wf: Workflow, proposal_node: str, run_id: str, title: str, summary: str, x: int, y: int) -> tuple:
    """Notify with approve/decline links, wait (max 24h) for a human, then record the decision."""
    resume = "$execution.resumeUrl + ($execution.resumeUrl.includes('?') ? '&' : '?')"
    links = ('{ "Approve": {{ JSON.stringify(' + resume + " + 'decision=approve') }}, "
             '"Decline": {{ JSON.stringify(' + resume + " + 'decision=decline') }} }")
    ask = ops_call(wf, "Ask for approval", "POST", "/notify", run_id=run_id, position=(x, y), body=(
        '={ "title": ' + title + ', "body": ' + summary + ', "links": ' + links + " }"))
    wait = wf.add("Wait for approval (24h)", "n8n-nodes-base.wait", 1.1, {
        "resume": "webhook", "httpMethod": "GET", "incomingAuthentication": "basicAuth", "responseMode": "onReceived",
        "limitWaitTime": True, "limitType": "afterTimeInterval", "resumeAmount": 24, "resumeUnit": "hours",
        "options": {"responseData": "Thanks, your decision was recorded."}},
        (x + 240, y), credentials="approvers", webhookId=stable_id(wf.id, "wait"))
    decide = ops_call(wf, "Record decision", "POST", "=/proposals/{{ $('" + proposal_node + "').item.json.proposal_id }}/decision",
                      run_id=run_id, position=(x + 480, y), body=(
                          '={ "decision": {{ JSON.stringify($json.query?.decision ?? "expire") }}, '
                          '"decided_by": {{ JSON.stringify($json.query?.by ?? "approver") }} }'))
    approved = condition(wf, "Approved?", "={{ $json.status }}", "equals", "approved", position=(x + 720, y))
    wf.chain(ask, wait, decide, approved)
    return approved


def finish(wf: Workflow, name: str, status: str, position) -> str:
    return ops_call(wf, name, "POST", "=/runs/{{ $('Start run').item.json.run_id }}/finish?status=" + status,
                    position=position)


# ------------------------------------------------------------------------------------------- workflows

def spark_failure_triage() -> Workflow:
    wf = Workflow("opsSparkTriage01", "Spark failure triage",
                  "A Spark job failed or ran badly: analyse its logs, let the agent propose a guarded config "
                  "change, wait for human approval, then open a pull request.")
    run_id = "={{ $('Start run').item.json.run_id }}"
    run_expr = "$('Start run').item.json.run_id"
    trigger = webhook(wf, "Spark job failed", "spark-job-failed", (0, 0))
    start = ops_call(wf, "Start run", "POST", "/runs", position=(240, 0), body=(
        '={ "workflow": "spark-failure-triage", "subject": {{ JSON.stringify($json.body.app_id) }}, '
        '"trigger": {{ JSON.stringify($json.body) }}, "n8n_execution_id": {{ JSON.stringify($execution.id) }} }'))
    analyse = ops_call(wf, "Analyse logs (deterministic)", "POST", "/spark/analyze", run_id=run_id, position=(480, 0),
                       body='={ "app_id": {{ JSON.stringify($("Spark job failed").item.json.body.app_id) }}, '
                            '"pipeline": {{ JSON.stringify($("Spark job failed").item.json.body.pipeline ?? null) }} }')
    schema = {"type": "object", "required": ["diagnosis", "root_cause", "spark_conf", "rationale", "confidence"],
              "properties": {
                  "diagnosis": {"type": "string", "description": "one sentence for the approver"},
                  "root_cause": {"type": "string"},
                  "spark_conf": {"type": "object", "additionalProperties": {"type": "string"},
                                 "description": "keys and exact values from the analyzer's recommendations"},
                  "rationale": {"type": "string", "description": "evidence: stage, metric or log line"},
                  "risks": {"type": "string"},
                  "confidence": {"type": "number", "minimum": 0, "maximum": 1}}}
    user = fill(prompt("spark_triage_user"), PIPELINE="{{ $json.pipeline }}", APP_ID="{{ $json.app_id }}",
                RUN_ID="{{ " + run_expr + " }}",
                ANALYSIS_JSON="{{ JSON.stringify({ ...$json, run_id: " + run_expr + " }) }}")
    diagnose = agent(wf, "Diagnose and propose (agent)", prompt("spark_triage_system"), user, schema, (720, 0),
                     ["analyze_spark_job", "get_pipeline_config", "check_proposal"])
    store = ops_call(wf, "Store proposal (guardrails)", "POST", "/proposals", run_id=run_id, position=(1040, 0), body=(
        '={ "kind": "spark_conf", "pipeline": {{ JSON.stringify($("Analyse logs (deterministic)").item.json.pipeline) }}, '
        '"changes": { "spark_conf": {{ JSON.stringify($json.output.spark_conf) }} }, '
        '"diagnosis": {{ JSON.stringify($json.output.diagnosis) }}, '
        '"rationale": {{ JSON.stringify($json.output.rationale + ($json.output.risks ? "\\nRisks: " + $json.output.risks : "")) }}, '
        '"confidence": {{ $json.output.confidence }} }'))
    passed = condition(wf, "Passed guardrails?", "={{ $json.status }}", "equals", "pending_approval",
                       position=(1280, 0))
    wf.chain(trigger, start, analyse, diagnose, store, passed)

    summary = ('{{ JSON.stringify("*" + $json.diagnosis + "*\\n" + Object.entries($json.changes.spark_conf)'
               '.map(([k, v]) => "• `" + k + " = " + v + "`").join("\\n") + "\\n\\n" + $json.rationale + '
               '"\\nGuardrails passed. Confidence " + $json.confidence + ". Proposal #" + $json.proposal_id) }}')
    approved = approval_steps(wf, "Store proposal (guardrails)", run_id,
                              '{{ JSON.stringify("Spark fix for " + $json.pipeline + ": approve?") }}', summary, 1520, 0)
    wf.link(passed, "Ask for approval", 0)

    apply = ops_call(wf, "Open pull request", "POST",
                     "=/proposals/{{ $('Store proposal (guardrails)').item.json.proposal_id }}/apply",
                     run_id=run_id, position=(2480, -120))
    done = ops_call(wf, "Tell the team", "POST", "/notify", run_id=run_id, position=(2720, -120), body=(
        '={ "title": {{ JSON.stringify("Spark fix approved for " + $json.pipeline) }}, '
        '"body": {{ JSON.stringify("Approved by " + $json.decided_by + ". Change: " + $json.applied_ref) }} }'))
    finish_ok = finish(wf, "Finish run (applied)", "applied", (2960, -120))
    finish_no = finish(wf, "Finish run (not approved)", "not_approved", (2480, 120))
    wf.link(approved, apply, 0)
    wf.chain(apply, done, finish_ok)
    wf.link(approved, finish_no, 1)

    rejected = ops_call(wf, "Report guardrail rejection", "POST", "/notify", run_id=run_id, position=(1520, 240), body=(
        '={ "title": {{ JSON.stringify("Agent proposal blocked by guardrails: " + $json.pipeline) }}, '
        '"body": {{ JSON.stringify($json.guardrail_report.violations.join("\\n")) }} }'))
    finish_rejected = finish(wf, "Finish run (blocked)", "blocked_by_guardrails", (1760, 240))
    wf.link(passed, rejected, 1)
    wf.chain(rejected, finish_rejected)
    return wf


def quality_incident_detector() -> Workflow:
    wf = Workflow("opsDqDetector001", "Data quality incident detector",
                  "Every 15 minutes: find quality rules failing above threshold and hand each one to the "
                  "incident workflow.")
    schedule = wf.add("Every 15 minutes", "n8n-nodes-base.scheduleTrigger", 1.2,
                      {"rule": {"interval": [{"field": "minutes", "minutesInterval": 15}]}}, (0, 0))
    manual = webhook(wf, "Check now", "check-quality-now", (0, 200))
    find = ops_call(wf, "Find incidents", "GET", "/quality/incidents", position=(240, 100))
    any_found = condition(wf, "Any incidents?", "={{ $json.incident_count }}", "gt", 0, kind="number",
                          position=(480, 100))
    split = wf.add("One item per incident", "n8n-nodes-base.splitOut", 1, {"fieldToSplitOut": "incidents",
                                                                           "include": "noOtherFields"}, (720, 0))
    hand_off = wf.add("Start incident workflow", "n8n-nodes-base.httpRequest", 4.5, {
        "method": "POST", "url": "={{ $env.N8N_INTERNAL_WEBHOOK_URL }}/webhook/dq-incident",
        "authentication": "genericCredentialType", "genericAuthType": "httpHeaderAuth",
        "sendBody": True, "contentType": "json", "specifyBody": "json", "jsonBody": "={{ JSON.stringify($json) }}",
        "options": {"timeout": 30000}}, (960, 0), credentials="webhook",
        retryOnFail=True, maxTries=3, waitBetweenTries=3000)
    wf.link(schedule, find)
    wf.link(manual, find)
    wf.chain(find, any_found)
    wf.link(any_found, split, 0)
    wf.chain(split, hand_off)
    return wf


def quality_incident() -> Workflow:
    wf = Workflow("opsDqIncident001", "Data quality incident",
                  "One failing quality rule: the agent inspects quarantined rows and history, classifies the "
                  "incident and proposes an action; a human approves before anything changes.")
    run_id = "={{ $('Start run').item.json.run_id }}"
    run_expr = "$('Start run').item.json.run_id"
    trigger = webhook(wf, "Quality incident", "dq-incident", (0, 0))
    start = ops_call(wf, "Start run", "POST", "/runs", position=(240, 0), body=(
        '={ "workflow": "dq-incident", "subject": {{ JSON.stringify($json.body.pipeline + "/" + $json.body.rule) }}, '
        '"trigger": {{ JSON.stringify($json.body) }}, "n8n_execution_id": {{ JSON.stringify($execution.id) }} }'))
    incident = "$('Quality incident').item.json.body"
    schema = {"type": "object", "required": ["classification", "action", "summary", "rationale", "confidence"],
              "properties": {
                  "classification": {"type": "string", "enum": ["upstream_schema_change", "data_defect",
                                                                "rule_too_strict", "transient"]},
                  "action": {"type": "string", "enum": ["monitor", "open_incident", "pause_downstream", "adjust_rule"]},
                  "rule_change": {"type": "object", "properties": {"max_failed_pct": {"type": "number"}},
                                  "description": "only for adjust_rule"},
                  "summary": {"type": "string"}, "rationale": {"type": "string"},
                  "confidence": {"type": "number", "minimum": 0, "maximum": 1}}}
    user = fill(prompt("dq_incident_user"), PIPELINE="{{ " + incident + ".pipeline }}",
                RULE="{{ " + incident + ".rule }}", RUN_ID="{{ " + run_expr + " }}",
                INCIDENT_JSON="{{ JSON.stringify({ ..." + incident + ", run_id: " + run_expr + " }) }}")
    investigate = agent(wf, "Investigate and classify (agent)", prompt("dq_incident_system"), user, schema, (480, 0),
                        ["sample_failed_rows", "get_failure_history", "get_pipeline_config", "check_proposal"])
    store = ops_call(wf, "Store proposal (guardrails)", "POST", "/proposals", run_id=run_id, position=(800, 0), body=(
        '={ "kind": "dq_action", "pipeline": {{ JSON.stringify(' + incident + '.pipeline) }}, '
        '"changes": { "action": {{ JSON.stringify($json.output.action) }}, "rule": {{ JSON.stringify(' + incident +
        '.rule) }}{{ $json.output.action === "adjust_rule" ? ", \\"rule_change\\": " + JSON.stringify($json.output.rule_change) : "" }} }, '
        '"diagnosis": {{ JSON.stringify($json.output.classification + ": " + $json.output.summary) }}, '
        '"rationale": {{ JSON.stringify($json.output.rationale) }}, "confidence": {{ $json.output.confidence }} }'))
    passed = condition(wf, "Passed guardrails?", "={{ $json.status }}", "equals", "pending_approval",
                       position=(1040, 0))
    wf.chain(trigger, start, investigate, store, passed)

    needs_action = condition(wf, "Action needed?", "={{ $json.changes.action }}", "notEquals", "monitor",
                             position=(1280, -100))
    wf.link(passed, needs_action, 0)
    summary = ('{{ JSON.stringify("*" + $json.diagnosis + "*\\nProposed action: `" + $json.changes.action + "`\\n\\n" + '
               '$json.rationale + "\\nConfidence " + $json.confidence + ". Proposal #" + $json.proposal_id) }}')
    approved = approval_steps(wf, "Store proposal (guardrails)", run_id,
                              '{{ JSON.stringify("Data quality incident on " + $json.pipeline + ": approve action?") }}',
                              summary, 1520, -100)
    wf.link(needs_action, "Ask for approval", 0)
    finish_monitor = finish(wf, "Finish run (monitoring)", "monitoring", (1520, 100))
    wf.link(needs_action, finish_monitor, 1)

    apply = ops_call(wf, "Carry out action", "POST",
                     "=/proposals/{{ $('Store proposal (guardrails)').item.json.proposal_id }}/apply",
                     run_id=run_id, position=(2480, -220))
    done = ops_call(wf, "Tell the team", "POST", "/notify", run_id=run_id, position=(2720, -220), body=(
        '={ "title": {{ JSON.stringify("Quality action done on " + $json.pipeline) }}, '
        '"body": {{ JSON.stringify("Approved by " + $json.decided_by + ": " + $json.applied_ref) }} }'))
    finish_ok = finish(wf, "Finish run (applied)", "applied", (2960, -220))
    finish_no = finish(wf, "Finish run (not approved)", "not_approved", (2480, 20))
    wf.link(approved, apply, 0)
    wf.chain(apply, done, finish_ok)
    wf.link(approved, finish_no, 1)

    rejected = ops_call(wf, "Report guardrail rejection", "POST", "/notify", run_id=run_id, position=(1280, 240), body=(
        '={ "title": {{ JSON.stringify("Agent action blocked by guardrails: " + $json.pipeline) }}, '
        '"body": {{ JSON.stringify($json.guardrail_report.violations.join("\\n")) }} }'))
    finish_rejected = finish(wf, "Finish run (blocked)", "blocked_by_guardrails", (1520, 240))
    wf.link(passed, rejected, 1)
    wf.chain(rejected, finish_rejected)
    return wf


def weekly_tuning_review() -> Workflow:
    wf = Workflow("opsWeeklyReview1", "Weekly Spark tuning review",
                  "Mondays 08:00: rank recent Spark jobs by idle core-hours and post a short review written by "
                  "the agent from the analyzer's numbers.")
    run_id = "={{ $('Start run').item.json.run_id }}"
    run_expr = "$('Start run').item.json.run_id"
    schedule = wf.add("Mondays 08:00", "n8n-nodes-base.scheduleTrigger", 1.2,
                      {"rule": {"interval": [{"field": "cronExpression", "expression": "0 8 * * 1"}]}}, (0, 0))
    manual = webhook(wf, "Run now", "run-weekly-review", (0, 200))
    start = ops_call(wf, "Start run", "POST", "/runs", position=(240, 100), body=(
        '={ "workflow": "weekly-tuning-review", "subject": "all jobs", '
        '"n8n_execution_id": {{ JSON.stringify($execution.id) }} }'))
    review = ops_call(wf, "Rank jobs by waste (deterministic)", "POST", "/spark/review", run_id=run_id,
                      position=(480, 100))
    user = fill(prompt("weekly_review_user"), RUN_ID="{{ " + run_expr + " }}",
                REVIEW_JSON="{{ JSON.stringify($json) }}")
    write = agent(wf, "Write the review (agent)", prompt("weekly_review_system"), user, None, (720, 100),
                  ["analyze_spark_job", "get_pipeline_config"])
    post = ops_call(wf, "Post review", "POST", "/notify", run_id=run_id, position=(1040, 100), body=(
        '={ "title": "Weekly Spark tuning review", "body": {{ JSON.stringify($json.output) }} }'))
    done = finish(wf, "Finish run", "completed", (1280, 100))
    wf.link(schedule, start)
    wf.link(manual, start)
    wf.chain(start, review, write, post, done)
    return wf


def error_handler() -> Workflow:
    wf = Workflow(ERROR_WORKFLOW_ID, "Ops agent error handler",
                  "Any ops-agent workflow that fails lands here: tell the team which node failed and why.")
    trigger = wf.add("On workflow error", "n8n-nodes-base.errorTrigger", 1, {}, (0, 0))
    alert = ops_call(wf, "Alert the team", "POST", "/notify", position=(240, 0), body=(
        '={ "title": {{ JSON.stringify("Ops agent workflow failed: " + $json.workflow.name) }}, '
        '"body": {{ JSON.stringify("Node: " + ($json.execution.lastNodeExecuted ?? "?") + "\\nError: " + '
        '($json.execution.error?.message ?? "unknown")) }}, '
        '"links": {{ JSON.stringify($json.execution.url ? { "Open execution": $json.execution.url } : {}) }} }'))
    close = ops_call(wf, "Mark run failed", "POST",
                     "=/executions/{{ $('On workflow error').item.json.execution.id }}/failed?error="
                     "{{ encodeURIComponent($('On workflow error').item.json.execution.error?.message ?? '') }}",
                     position=(480, 0))
    wf.chain(trigger, alert, close)
    return wf


WORKFLOWS = {
    "00-error-handler.json": error_handler,
    "01-spark-failure-triage.json": spark_failure_triage,
    "02-dq-incident-detector.json": quality_incident_detector,
    "03-dq-incident.json": quality_incident,
    "04-weekly-tuning-review.json": weekly_tuning_review,
}


def main(check: bool) -> int:
    OUT.mkdir(exist_ok=True)
    stale = []
    for filename, build in WORKFLOWS.items():
        text = json.dumps(build().to_json(), indent=2) + "\n"
        path = OUT / filename
        if check:
            if not path.exists() or path.read_text() != text:
                stale.append(filename)
        else:
            path.write_text(text)
    if stale:
        print("stale workflow JSON (run python3 n8n/build_workflows.py):", ", ".join(stale))
        return 1
    print(("checked " if check else "wrote ") + f"{len(WORKFLOWS)} workflows in {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main("--check" in sys.argv))

"""A scripted stand-in for the Anthropic Messages API, for hermetic end-to-end tests.

CI has no API key and must be deterministic, so the e2e test points n8n's Anthropic credential at this
server. It plays the agent's part with fixed rules: call the right MCP tool, read the result, then answer
through `format_final_json_response`. The real workflow, MCP tools, guardrails, approval wait and PR step all
run for real; only the model's judgement is scripted.

    uvicorn ops_tools.testing.stub_llm:app --port 8100
"""
import json
import os
import re
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="stub-anthropic")
FINAL_TOOL = "format_final_json_response"

# keys the scripted agent picks from the analyzer's recommendations, per diagnosis
KEYS_BY_DIAGNOSIS = {
    "container_memory_kill": ["spark.executor.memoryOverhead", "spark.executor.pyspark.memory",
                              "spark.sql.adaptive.enabled", "spark.sql.adaptive.skewJoin.enabled"],
    "heap_oom": ["spark.executor.memory", "spark.sql.shuffle.partitions"],
    "data_skew": ["spark.sql.adaptive.enabled", "spark.sql.adaptive.skewJoin.enabled"],
    "over_provisioned": ["spark.dynamicAllocation.enabled", "spark.dynamicAllocation.shuffleTracking.enabled",
                         "spark.dynamicAllocation.maxExecutors", "spark.dynamicAllocation.minExecutors"],
}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/v1/messages")
async def messages(request: Request):
    body = await request.json()
    if os.environ.get("STUB_LLM_DEBUG"):  # log the conversation shape n8n sends
        shape = [(m.get("role"), m["content"] if isinstance(m.get("content"), str) and len(m["content"]) < 200 else
                  [(b.get("type"), str(b)[:300]) for b in m["content"]] if isinstance(m.get("content"), list) else "long text")
                 for m in body.get("messages", [])]
        print("STUB", json.dumps({"tools": [t.get("name") for t in body.get("tools", [])], "messages": shape,
                                  "stream": body.get("stream")})[:4000], flush=True)
    content, stop_reason = next_turn(body)
    message = {"id": f"msg_stub_{uuid.uuid4().hex[:12]}", "type": "message", "role": "assistant",
               "model": body.get("model", "stub"), "content": content, "stop_reason": stop_reason,
               "stop_sequence": None, "usage": {"input_tokens": 100, "output_tokens": 50}}
    if body.get("stream"):
        return StreamingResponse(as_sse(message), media_type="text/event-stream")
    return JSONResponse(message)


# --------------------------------------------------------------------------------------- the script

def next_turn(body: dict) -> tuple[list[dict], str]:
    prompt = conversation_text(body)
    tools = {t["name"] for t in body.get("tools", [])}
    final_schema = next((t.get("input_schema", {}) for t in body.get("tools", []) if t["name"] == FINAL_TOOL), {})
    if list(final_schema.get("properties", {})) == ["output"]:
        tools.add(FINAL_TOOL + ":wrapped")  # n8n's parser wraps the answer as {"output": {...}}
    results = tool_results(body)
    if "Task: spark_triage" in prompt:
        return spark_triage(prompt, results, tools)
    if "Task: dq_incident" in prompt:
        return dq_incident(prompt, results, tools)
    if "Task: weekly_review" in prompt:
        return text(weekly_report(tagged_json(prompt, "review") or {}))
    return text("I can only run the scripted demo tasks.")


def spark_triage(prompt: str, results: list, tools: set):
    analysis = tagged_json(prompt, "analysis") or {}
    recommended = {r["key"]: str(r["recommended"]) for r in analysis.get("recommended", [])}
    diagnosis = analysis.get("diagnosis", "healthy")
    wanted = [k for k in KEYS_BY_DIAGNOSIS.get(diagnosis, list(recommended)[:3]) if k in recommended]
    changes = {k: recommended[k] for k in wanted}

    checks = [r for r in results if isinstance(r, dict) and "passed" in r]
    if checks and not checks[-1]["passed"]:
        rejected = " ".join(checks[-1]["violations"])
        changes = {k: v for k, v in changes.items() if k not in rejected}
    if not checks or (not checks[-1]["passed"] and len(checks) < 2):
        return tool_use("check_proposal", {"kind": "spark_conf", "pipeline": analysis.get("pipeline", ""),
                                           "changes": {"spark_conf": changes}, "run_id": analysis.get("run_id")}, tools)
    answer = {"diagnosis": f"{diagnosis.replace('_', ' ')} on {analysis.get('pipeline')}",
              "root_cause": diagnosis, "spark_conf": changes,
              "rationale": "; ".join(f["title"] for f in analysis.get("findings", []) if f["severity"] == "high"),
              "risks": "Container size changes; watch the next run's executor memory and runtime.",
              "confidence": 0.8 if changes else 0.3}
    return final(answer, tools)


def dq_incident(prompt: str, results: list, tools: set):
    incident = tagged_json(prompt, "incident") or {}
    samples = next((r for r in results if isinstance(r, dict) and "field_profile" in r), None)
    if samples is None:
        return tool_use("sample_failed_rows", {"pipeline": incident.get("pipeline"), "rule": incident.get("rule"),
                                               "limit": 20, "run_id": incident.get("run_id")}, tools)
    new_fields = sorted(set(samples["field_profile"]) - set(samples["fields_seen_in_other_rules"]))
    if new_fields:
        answer = {"classification": "upstream_schema_change", "action": "pause_downstream",
                  "summary": f"Producer started sending new field(s) {', '.join(new_fields)}; the rule's column is null.",
                  "rationale": f"{incident.get('failed_pct')}% failed vs {incident.get('baseline_failed_pct')}% baseline, "
                               f"all failing rows carry {', '.join(new_fields)}. Pause {incident.get('downstream')} "
                               f"until the mapping is fixed.", "confidence": 0.85}
    else:
        answer = {"classification": "data_defect", "action": "open_incident",
                  "summary": "Rows fail the rule with the usual schema.", "rationale": "No schema change found.",
                  "confidence": 0.6}
    return final(answer, tools)


def weekly_report(review: dict) -> str:
    lines = [f"*{review.get('jobs_reviewed', 0)} jobs reviewed, "
             f"{review.get('total_idle_core_hours', 0)} idle core-hours last run.*", ""]
    for job in review.get("jobs", [])[:5]:
        lines.append(f"- *{job['pipeline']}*: {job['diagnosis'].replace('_', ' ')}, "
                     f"{job['idle_core_hours']} idle core-hours, {job['recommended_changes']} suggested changes")
    return "\n".join(lines)


# --------------------------------------------------------------------------------------- helpers

def conversation_text(body: dict) -> str:
    parts = [body["system"] if isinstance(body.get("system"), str) else
             " ".join(b.get("text", "") for b in body.get("system") or [])]
    for message in body.get("messages", []):
        content = message.get("content")
        if isinstance(content, str):
            parts.append(content)
        else:
            parts.extend(b.get("text", "") for b in content if b.get("type") == "text")
    return "\n".join(parts)


def tool_results(body: dict) -> list:
    found = []
    for message in body.get("messages", []):
        if message.get("role") != "user" or isinstance(message.get("content"), str):
            continue
        for block in message["content"]:
            if block.get("type") == "tool_result":
                content = block.get("content")
                raw = content if isinstance(content, str) else " ".join(
                    c.get("text", "") for c in content or [] if isinstance(c, dict))
                found.append(parse_json(raw))
    return found


def parse_json(raw: str):
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        match = re.search(r"\{.*\}", raw or "", re.S)
        try:
            value = json.loads(match.group(0)) if match else raw
        except json.JSONDecodeError:
            return raw
    # n8n wraps MCP results: [{"response": [{"type": "text", "text": "{...}"}]}]
    if isinstance(value, list) and value and isinstance(value[0], dict):
        if "response" in value[0]:
            return parse_json(json.dumps(value[0]["response"]))
        if "text" in value[0]:
            return parse_json(value[0]["text"])
    return value


def tagged_json(prompt: str, tag: str):
    match = re.search(rf"<{tag}>\s*(.*?)\s*</{tag}>", prompt, re.S)
    return parse_json(match.group(1)) if match else None


def tool_use(name: str, arguments: dict, tools: set):
    # n8n prefixes MCP tool names with the tool node's name, e.g. "Ops_tools_check_proposal"
    resolved = next((t for t in sorted(tools) if t == name or t.endswith("_" + name)), name)
    return [{"type": "tool_use", "id": f"toolu_stub_{uuid.uuid4().hex[:12]}", "name": resolved,
             "input": arguments}], "tool_use"


def final(answer: dict, tools: set):
    if FINAL_TOOL in tools:
        return tool_use(FINAL_TOOL, {"output": answer} if FINAL_TOOL + ":wrapped" in tools else answer, tools)
    return text(json.dumps(answer))


def text(message: str):
    return [{"type": "text", "text": message}], "end_turn"


def as_sse(message: dict):
    def event(name: str, data: dict) -> str:
        return f"event: {name}\ndata: {json.dumps(data)}\n\n"

    yield event("message_start", {"type": "message_start", "message": {**message, "content": [], "stop_reason": None}})
    for index, block in enumerate(message["content"]):
        if block["type"] == "text":
            yield event("content_block_start", {"type": "content_block_start", "index": index,
                                                "content_block": {"type": "text", "text": ""}})
            yield event("content_block_delta", {"type": "content_block_delta", "index": index,
                                                "delta": {"type": "text_delta", "text": block["text"]}})
        else:
            yield event("content_block_start", {"type": "content_block_start", "index": index,
                                                "content_block": {**block, "input": {}}})
            yield event("content_block_delta", {"type": "content_block_delta", "index": index,
                                                "delta": {"type": "input_json_delta",
                                                          "partial_json": json.dumps(block["input"])}})
        yield event("content_block_stop", {"type": "content_block_stop", "index": index})
    yield event("message_delta", {"type": "message_delta", "delta": {"stop_reason": message["stop_reason"],
                                                                     "stop_sequence": None},
                                  "usage": {"output_tokens": 50}})
    yield event("message_stop", {"type": "message_stop"})

"""The scripted model used by the e2e test must behave like a careful agent."""
import json

from ops_tools.testing.stub_llm import next_turn

TOOLS = [{"name": "Ops_tools_check_proposal"}, {"name": "Ops_tools_sample_failed_rows"},
         {"name": "format_final_json_response", "input_schema": {"properties": {"output": {}}}}]
ANALYSIS = {"pipeline": "daily_sales_join", "run_id": 1, "diagnosis": "container_memory_kill", "findings": [],
            "recommended": [{"key": "spark.executor.memoryOverhead", "recommended": "2048m"},
                            {"key": "spark.executor.memory", "recommended": "5g"}]}


def ask(results=()):
    messages = [{"role": "user", "content": f"Task: spark_triage\n<analysis>{json.dumps(ANALYSIS)}</analysis>"}]
    for result in results:
        messages += [{"role": "assistant", "content": [{"type": "tool_use", "id": "t", "name": "x", "input": {}}]},
                     {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t",
                                                   "content": json.dumps([{"response": [
                                                       {"type": "text", "text": json.dumps(result)}]}])}]}]
    return next_turn({"messages": messages, "tools": TOOLS})


def test_checks_guardrails_before_answering():
    content, stop = ask()
    assert stop == "tool_use" and content[0]["name"] == "Ops_tools_check_proposal"
    assert content[0]["input"]["changes"] == {"spark_conf": {"spark.executor.memoryOverhead": "2048m"}}


def test_answers_through_the_output_parser_tool():
    content, _ = ask([{"passed": True, "violations": [], "warnings": []}])
    assert content[0]["name"] == "format_final_json_response"
    assert content[0]["input"]["output"]["spark_conf"] == {"spark.executor.memoryOverhead": "2048m"}

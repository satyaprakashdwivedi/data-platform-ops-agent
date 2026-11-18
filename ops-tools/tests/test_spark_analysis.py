import pytest

from ops_tools import spark_analysis

EXPECTED = {
    "application_1700000000000_0042": ("daily_sales_join", "container_memory_kill", "spark.executor.memoryOverhead"),
    "application_1700000000000_0057": ("orders_enrichment", "heap_oom", "spark.executor.memory"),
    "application_1700000000000_0063": ("clickstream_sessionize", "over_provisioned", "spark.dynamicAllocation.enabled"),
}


@pytest.mark.parametrize("app_id", sorted(EXPECTED))
def test_demo_jobs_are_diagnosed(demo_logs, app_id):
    app_name, diagnosis, key = EXPECTED[app_id]
    files, source = spark_analysis.find_event_logs(app_id, demo_logs)
    result = spark_analysis.for_agent(spark_analysis.run_analyzer(files))
    assert result["summary"]["app_name"] == app_name
    assert result["diagnosis"] == diagnosis
    assert key in {r["key"] for r in result["recommended"]}
    assert source.startswith("local:")


def test_container_kill_is_not_fixed_with_more_heap(demo_logs):
    files, _ = spark_analysis.find_event_logs("application_1700000000000_0042", demo_logs)
    result = spark_analysis.run_analyzer(files)
    recommended = {r["key"]: r["recommended"] for r in result["recommended"]}
    assert recommended["spark.executor.memoryOverhead"] == "2048m"
    heap_after = spark_analysis.analyzer.parse_size(recommended.get("spark.executor.memory", "8g"))
    assert heap_after <= 8 * 1024 ** 3, "off-heap kills must not be answered with a bigger heap"


def test_idle_core_hours():
    assert spark_analysis.idle_core_hours({"executors_total": 10, "executor_cores": 4, "duration_ms": 3_600_000,
                                           "slot_utilisation_pct": 25}) == 30.0
    assert spark_analysis.idle_core_hours({"executors_total": 1, "executor_cores": 1, "duration_ms": 1000,
                                           "slot_utilisation_pct": 140}) == 0.0


def test_app_id_is_validated(demo_logs):
    with pytest.raises(ValueError):
        spark_analysis.find_event_logs("../secrets", demo_logs)
    with pytest.raises(spark_analysis.EventLogNotFound):
        spark_analysis.find_event_logs("application_0_0", demo_logs)

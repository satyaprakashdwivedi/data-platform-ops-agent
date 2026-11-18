import yaml

from ops_tools.pipelines import PipelineRepository, unified_diff, with_rule_threshold, with_spark_conf


def test_spark_conf_edit_keeps_comments_and_order(repo):
    before = PipelineRepository(repo / "pipelines").config_text("daily_sales_join")
    after = with_spark_conf(before, {"spark.sql.adaptive.enabled": "true", "spark.executor.memoryOverhead": "2048m"})
    assert after.startswith("# Spark job config")
    conf = yaml.safe_load(after)["spark_conf"]
    assert conf["spark.sql.adaptive.enabled"] == "true" and conf["spark.executor.memoryOverhead"] == "2048m"
    assert conf["spark.executor.memory"] == "8g"
    diff = unified_diff(before, after, "daily_sales_join.yaml")
    assert '-  spark.sql.adaptive.enabled: "false"' in diff and "+  spark.executor.memoryOverhead: 2048m" in diff


def test_rule_threshold_edit(repo):
    text = PipelineRepository(repo / "pipelines").rules_text("orders_enrichment")
    after = yaml.safe_load(with_rule_threshold(text, "currency_known", 3.0))
    rule = next(r for r in after["rules"] if r["name"] == "currency_known")
    assert rule["max_failed_pct"] == 3.0 and rule["expression"].startswith("currency IN")


def test_pipeline_names_are_validated(repo):
    pipelines = PipelineRepository(repo / "pipelines")
    assert "daily_sales_join" in pipelines.names()
    try:
        pipelines.config_text("../../etc/passwd")
    except ValueError:
        return
    raise AssertionError("path traversal must be rejected")

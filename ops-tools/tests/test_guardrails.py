from ops_tools.guardrails import check_dq_action, check_spark_conf

CURRENT = {"spark.executor.memory": "8g", "spark.executor.cores": "8", "spark.executor.instances": "10",
           "spark.sql.shuffle.partitions": "200"}
RECOMMENDED = [{"key": "spark.executor.memoryOverhead", "recommended": "2048m"},
               {"key": "spark.sql.shuffle.partitions", "recommended": "480"},
               {"key": "spark.executor.memory", "recommended": "12g"}]


def check(changes, policy, current=CURRENT, recommended=RECOMMENDED):
    return check_spark_conf(changes, current, recommended, policy)


def test_grounded_change_passes(policy):
    report = check({"spark.executor.memoryOverhead": "2048m", "spark.sql.shuffle.partitions": "480"}, policy)
    assert report.passed, report.violations


def test_key_not_recommended_by_analyzer_is_rejected(policy):
    report = check({"spark.executor.cores": "4"}, policy)
    assert any("not recommended by the analyzer" in v for v in report.violations)


def test_key_outside_allow_list_is_rejected(policy):
    report = check({"spark.hadoop.fs.s3a.secret.key": "x"}, policy,
                   recommended=[{"key": "spark.hadoop.fs.s3a.secret.key", "recommended": "x"}])
    assert any("not on the allowed list" in v for v in report.violations)


def test_value_injection_is_rejected(policy):
    report = check({"spark.executor.memoryOverhead": "2g --conf spark.x=y"}, policy)
    assert any("not a plain Spark value" in v for v in report.violations)


def test_limits_and_change_factor(policy):
    report = check({"spark.executor.memory": "40g"}, policy,
                   recommended=[{"key": "spark.executor.memory", "recommended": "40g"}])
    assert any("above the maximum 32g" in v for v in report.violations)
    assert any("more than 3x" in v for v in report.violations)


def test_container_size_limit_counts_heap_and_overhead(policy):
    report = check({"spark.executor.memory": "28g", "spark.executor.memoryOverhead": "8g",
                    "spark.executor.pyspark.memory": "8g"}, policy,
                   recommended=[{"key": k, "recommended": v} for k, v in
                                {"spark.executor.memory": "28g", "spark.executor.memoryOverhead": "8g",
                                 "spark.executor.pyspark.memory": "8g"}.items()])
    assert any("executor container would need 44.0 GB" in v for v in report.violations)


def test_value_different_from_analyzer_is_a_warning(policy):
    report = check({"spark.executor.memoryOverhead": "3g"}, policy)
    assert report.passed and report.warnings


def test_empty_proposal_is_rejected(policy):
    assert not check({}, policy).passed


RULES = {"rules": [{"name": "order_id_present", "max_failed_pct": 0.5}, {"name": "currency_known"}]}


def test_dq_actions(policy):
    assert check_dq_action({"action": "pause_downstream", "rule": "currency_known"}, RULES, policy).passed
    assert not check_dq_action({"action": "drop_table"}, RULES, policy).passed
    assert not check_dq_action({"action": "open_incident", "rule_change": {"max_failed_pct": 9}}, RULES, policy).passed


def test_adjust_rule_only_changes_threshold_within_limit(policy):
    ok = check_dq_action({"action": "adjust_rule", "rule": "order_id_present",
                          "rule_change": {"max_failed_pct": 1.0}}, RULES, policy)
    assert ok.passed and ok.warnings
    too_far = check_dq_action({"action": "adjust_rule", "rule": "currency_known",
                               "rule_change": {"max_failed_pct": 50}}, RULES, policy)
    assert any("exceeds" in v for v in too_far.violations)
    sql_change = check_dq_action({"action": "adjust_rule", "rule": "currency_known",
                                  "rule_change": {"expression": "1 = 1"}}, RULES, policy)
    assert any("never a rule's SQL" in v for v in sql_change.violations)

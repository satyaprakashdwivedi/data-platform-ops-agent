"""Policy checks applied in code to every agent proposal, before any human sees it.

The LLM can only *propose*. A proposal that breaks policy is stored as `rejected_by_guardrails`
with the reasons, and never reaches an approver.
"""
import re
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path

import yaml

from .vendor.analyze_spark_log import parse_size

SIZE_KEYS = {
    "spark.executor.memory", "spark.executor.memoryOverhead", "spark.executor.pyspark.memory",
    "spark.driver.memory", "spark.sql.autoBroadcastJoinThreshold", "spark.sql.files.maxPartitionBytes",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
}
COUNT_KEYS = {"spark.executor.cores", "spark.executor.instances", "spark.sql.shuffle.partitions",
              "spark.default.parallelism", "spark.dynamicAllocation.maxExecutors",
              "spark.dynamicAllocation.minExecutors"}
SAFE_VALUE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")
OVERHEAD_FACTOR, MIN_OVERHEAD = 0.10, 384 * 1024 ** 2


def load_policy(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


@dataclass
class GuardrailReport:
    violations: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.violations

    def as_dict(self) -> dict:
        return {"passed": self.passed, "violations": self.violations, "warnings": self.warnings}


def _number(key: str, value) -> float:
    return float(parse_size(str(value))) if key in SIZE_KEYS else float(value)


def check_spark_conf(changes: dict, current_config: dict, analyzer_recommendations: list[dict],
                     policy: dict) -> GuardrailReport:
    rules = policy["spark_conf"]
    report = GuardrailReport()
    if not changes:
        report.violations.append("the proposal changes nothing")
        return report
    if len(changes) > rules["max_keys_per_proposal"]:
        report.violations.append(f"{len(changes)} keys changed; the limit is {rules['max_keys_per_proposal']}")

    recommended = {r["key"]: str(r["recommended"]) for r in analyzer_recommendations}
    for key, value in changes.items():
        value = str(value)
        if not any(fnmatch(key, pattern) for pattern in rules["allowed_keys"]):
            report.violations.append(f"{key} is not on the allowed list")
            continue
        if not SAFE_VALUE.match(value):
            report.violations.append(f"{key}={value!r} is not a plain Spark value")
            continue
        if rules.get("must_be_grounded") and key not in recommended:
            report.violations.append(f"{key} was not recommended by the analyzer for this run")
        elif key in recommended and recommended[key] != value:
            report.warnings.append(f"{key}={value} differs from the analyzer's {recommended[key]}")

        if key in SIZE_KEYS or key in COUNT_KEYS or key in rules.get("limits", {}):
            try:
                proposed = _number(key, value)
            except ValueError:
                report.violations.append(f"{key}={value} is not a valid size or number")
                continue
            limit = rules.get("limits", {}).get(key, {})
            if "min" in limit and proposed < _number(key, limit["min"]):
                report.violations.append(f"{key}={value} is below the minimum {limit['min']}")
            if "max" in limit and proposed > _number(key, limit["max"]):
                report.violations.append(f"{key}={value} is above the maximum {limit['max']}")
            if key in current_config:
                try:
                    before = _number(key, current_config[key])
                except ValueError:
                    before = 0
                factor = rules["max_change_factor"]
                if before > 0 and proposed > 0 and max(proposed / before, before / proposed) > factor:
                    report.violations.append(f"{key} moves from {current_config[key]} to {value}: more than "
                                             f"{factor}x in one step")

    after = {**current_config, **{k: str(v) for k, v in changes.items()}}
    try:
        heap = parse_size(after.get("spark.executor.memory", "1g"))
        overhead = (parse_size(after["spark.executor.memoryOverhead"]) if "spark.executor.memoryOverhead" in after
                    else max(MIN_OVERHEAD, int(heap * OVERHEAD_FACTOR)))
        pyspark = parse_size(after["spark.executor.pyspark.memory"]) if "spark.executor.pyspark.memory" in after else 0
        if heap + overhead + pyspark > parse_size(rules["max_container_memory"]):
            report.violations.append(f"executor container would need {(heap + overhead + pyspark) / 1024 ** 3:.1f} GB; "
                                     f"the limit is {rules['max_container_memory']}")
        cores = int(after.get("spark.executor.cores", 1)) * int(after.get("spark.executor.instances", 1))
        if cores > rules["max_total_cores"]:
            report.violations.append(f"{cores} executor cores in total; the limit is {rules['max_total_cores']}")
    except (ValueError, TypeError) as error:
        report.violations.append(f"could not evaluate the resulting executor size: {error}")
    return report


def check_dq_action(action: dict, rules_document: dict | None, policy: dict) -> GuardrailReport:
    quality = policy["quality"]
    report = GuardrailReport()
    name = action.get("action")
    if name not in quality["allowed_actions"]:
        report.violations.append(f"action {name!r} is not one of {quality['allowed_actions']}")
        return report
    if name != "adjust_rule":
        if action.get("rule_change"):
            report.violations.append("rule_change is only allowed with action adjust_rule")
        return report

    change = action.get("rule_change") or {}
    if set(change) != {"max_failed_pct"}:
        report.violations.append("adjust_rule may only change max_failed_pct (never a rule's SQL expression)")
        return report
    rules = {r["name"]: r for r in (rules_document or {}).get("rules", [])}
    rule = rules.get(action.get("rule"))
    if rule is None:
        report.violations.append(f"rule {action.get('rule')!r} does not exist for this pipeline")
        return report
    try:
        new_pct = float(change["max_failed_pct"])
    except (TypeError, ValueError):
        report.violations.append("max_failed_pct must be a number")
        return report
    old_pct = float(rule.get("max_failed_pct", quality["default_max_failed_pct"]))
    if not 0 <= new_pct <= 100:
        report.violations.append("max_failed_pct must be between 0 and 100")
    elif new_pct - old_pct > quality["max_rule_relaxation_pct"]:
        report.violations.append(f"relaxing {rule['name']} from {old_pct}% to {new_pct}% exceeds the "
                                 f"{quality['max_rule_relaxation_pct']} point limit")
    if new_pct > old_pct:
        report.warnings.append(f"this relaxes {rule['name']} from {old_pct}% to {new_pct}% failed rows")
    return report

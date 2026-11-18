# Vendored: spark-optimizer analyzer

`analyze_spark_log.py` is copied unchanged from
[satyaprakashdwivedi/spark-optimizer-skill](https://github.com/satyaprakashdwivedi/spark-optimizer-skill)
at commit `bd40f5c1ea7000d2d7956a7b9b2b4a14b28aa399` (`spark-optimizer/scripts/analyze_spark_log.py`).

It is the deterministic engine behind the `analyze_spark_job` tool: the agent never computes memory
or partition sizes itself, it reasons over this analyzer's findings.

Update with `scripts/sync_analyzer.sh <commit>`; CI checks the copy still matches the pinned commit.

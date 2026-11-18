"""Deterministic Spark diagnosis, built on the spark-optimizer analyzer.

The agent never sizes memory or partitions itself: it calls this module, which returns the analyzer's
findings and recommended configs, plus one primary root-cause class the workflows can branch on.
"""
import io
import re
import tempfile
import zipfile
from argparse import Namespace
from pathlib import Path

import httpx

from .vendor import analyze_spark_log as analyzer

# Most urgent first: the first class whose finding is present becomes the primary diagnosis.
DIAGNOSIS_BY_FINDING = [
    ("executor-oom", "heap_oom"),
    ("memory-overhead", "container_memory_kill"),
    ("skew", "data_skew"),
    ("shuffle-fetch", "shuffle_fetch_failure"),
    ("bad-join", "bad_join_strategy"),
    ("broadcast", "broadcast_problem"),
    ("driver-results", "driver_overload"),
    ("spill", "disk_spill"),
    ("gc", "gc_pressure"),
    ("shuffle-partitions", "partition_sizing"),
    ("aqe", "aqe_disabled"),
    ("utilisation", "over_provisioned"),
    ("parallelism", "low_parallelism"),
    ("executor-shape", "executor_shape"),
]

# Only the keys an operator needs to see; the analyzer's full config dump can be large.
CONFIG_KEYS_SHOWN = re.compile(r"^spark\.(executor|driver|memory|sql\.(shuffle|adaptive|autoBroadcast|files)|"
                               r"dynamicAllocation|default\.parallelism|serializer)")
SAFE_APP_ID = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


class EventLogNotFound(LookupError):
    pass


def find_event_logs(app_id: str, local_dir: Path, history_server_url: str = "") -> tuple[list[Path], str]:
    """Return the log files for an application and where they came from.

    Looks in <local_dir>/<app_id>/ first (event log plus any driver/executor stderr files), then asks the
    Spark History Server's REST API for the event log zip.
    """
    if not SAFE_APP_ID.match(app_id):
        raise ValueError(f"invalid Spark application id: {app_id!r}")

    app_dir = local_dir / app_id
    if app_dir.is_dir():
        files = sorted(p for p in app_dir.rglob("*") if p.is_file())
        if files:
            return files, f"local:{app_dir}"

    if history_server_url:
        url = f"{history_server_url.rstrip('/')}/api/v1/applications/{app_id}/logs"
        response = httpx.get(url, timeout=60, follow_redirects=True)
        if response.status_code == 200:
            download_dir = Path(tempfile.mkdtemp(prefix=f"eventlog-{app_id}-"))
            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                for member in archive.namelist():
                    target = (download_dir / member).resolve()
                    if download_dir.resolve() in target.parents and not member.endswith("/"):
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_bytes(archive.read(member))
            files = sorted(p for p in download_dir.rglob("*") if p.is_file())
            if files:
                return files, f"history-server:{url}"

    raise EventLogNotFound(f"no event log found for {app_id} in {local_dir} or the Spark History Server")


def run_analyzer(paths: list[Path], node_cores: int | None = None, node_memory_gb: float | None = None,
                 nodes: int | None = None, target_partition_mb: int = 128) -> dict:
    """Run spark-optimizer on event log and text log files; same output as `analyze_spark_log.py --json`."""
    options = Namespace(node_cores=node_cores, node_mem_gb=node_memory_gb, nodes=nodes,
                        target_partition_mb=target_partition_mb)
    event_log_groups, text_logs = analyzer.expand_inputs([str(p) for p in paths])
    log_signals = analyzer.scan_text_logs(text_logs) if text_logs else {}

    event_log_files = event_log_groups[0] if event_log_groups else []
    app = analyzer.load_event_log(event_log_files) if event_log_files else None
    summary = analyzer.summarize(app) if app else {}
    properties = app.props if app else {}
    empty_summary = {"stages": [], "disk_spill": 0, "mem_spill": 0, "gc_pct": 0, "total_task_run_ms": 0,
                     "fetch_wait_pct": 0, "shuffle_read": 0}

    advisor = analyzer.Advisor(properties, summary or empty_summary, log_signals, options).run()
    return {
        "summary": summary,
        "findings": advisor.findings,
        "recommended": advisor.recommended(),
        "log_signals": log_signals,
        "memory_model": advisor.memory_model(),
        "current_config": {k: v for k, v in properties.items() if CONFIG_KEYS_SHOWN.match(k)},
    }


def primary_diagnosis(findings: list[dict]) -> str:
    present = {f["id"] for f in findings}
    for finding_id, diagnosis in DIAGNOSIS_BY_FINDING:
        if finding_id in present:
            return diagnosis
    return "healthy"


def idle_core_hours(summary: dict) -> float:
    """Executor core-hours paid for but not running tasks: the weekly review ranks jobs by this."""
    cores = (summary.get("executors_total") or 0) * (summary.get("executor_cores") or 0)
    hours = (summary.get("duration_ms") or 0) / 3_600_000
    utilisation = summary.get("slot_utilisation_pct")
    if not cores or utilisation is None:
        return 0.0
    return round(max(0.0, cores * hours * (1 - utilisation / 100)), 2)


def for_agent(result: dict) -> dict:
    """A compact view of the analysis for the LLM: what it needs to reason, nothing it could misread."""
    summary = result["summary"]
    headline_keys = ["app_name", "app_id", "spark_version", "duration_ms", "jobs_failed", "tasks_failed",
                     "gc_pct", "disk_spill", "mem_spill", "peak_jvm_heap", "slot_utilisation_pct",
                     "executors_total", "executor_cores", "executors_removed", "python_udf"]
    stages = sorted(summary.get("stages", []), key=lambda s: s.get("wall_ms") or 0, reverse=True)[:3]
    return {
        "diagnosis": primary_diagnosis(result["findings"]),
        "summary": {k: summary.get(k) for k in headline_keys if k in summary},
        "slowest_stages": [{k: s.get(k) for k in ("stage", "name", "tasks", "wall_ms", "skew_ratio", "disk_spill",
                                                  "gc_pct", "failure")} for s in stages],
        "findings": [{"id": f["id"], "severity": f["severity"], "title": f["title"],
                      "evidence": f["evidence"][:600], "configs": f.get("configs") or {}}
                     for f in result["findings"]],
        "recommended": result["recommended"],
        "log_signals": {k: v.get("count") for k, v in result["log_signals"].items()},
        "current_config": result["current_config"],
        "idle_core_hours": idle_core_hours(summary),
    }

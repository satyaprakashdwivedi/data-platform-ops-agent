"""Data quality signals: which rules are failing too often, and what the failing rows look like."""
from collections import Counter

from .db import Database
from .pipelines import PipelineNotFound, PipelineRepository


def rule_thresholds(pipelines: PipelineRepository, pipeline: str, default_pct: float) -> dict[str, float]:
    rules = pipelines.rules(pipeline) or {}
    return {r["name"]: float(r.get("max_failed_pct", default_pct)) for r in rules.get("rules", [])}


def find_incidents(db: Database, pipelines: PipelineRepository, policy: dict,
                   window_minutes: int | None = None) -> list[dict]:
    """Rules whose failed-row % in the window is above their threshold, worst first."""
    quality = policy["quality"]
    window = window_minutes or quality["window_minutes"]
    rows = db.fetch_all(
        """
        WITH recent AS (
            SELECT pipeline, rule, sum(checked_rows) AS checked_rows, sum(failed_rows) AS failed_rows,
                   count(DISTINCT batch_id) AS batches, max(checked_at) AS last_checked_at
            FROM dq.check_result
            WHERE checked_at > now() - make_interval(mins => %(window)s)
            GROUP BY pipeline, rule
        ), baseline AS (
            SELECT pipeline, rule, 100.0 * sum(failed_rows) / nullif(sum(checked_rows), 0) AS failed_pct
            FROM dq.check_result
            WHERE checked_at BETWEEN now() - interval '7 days' AND now() - make_interval(mins => %(window)s)
            GROUP BY pipeline, rule
        )
        SELECT r.*, round(100.0 * r.failed_rows / nullif(r.checked_rows, 0), 3) AS failed_pct,
               round(b.failed_pct, 3) AS baseline_failed_pct
        FROM recent r LEFT JOIN baseline b USING (pipeline, rule)
        WHERE r.checked_rows >= %(min_rows)s
        """,
        {"window": window, "min_rows": quality["min_checked_rows"]},
    )
    incidents = []
    for row in rows:
        try:
            thresholds = rule_thresholds(pipelines, row["pipeline"], quality["default_max_failed_pct"])
            downstream = pipelines.config(row["pipeline"]).get("downstream", [])
        except (PipelineNotFound, ValueError):
            thresholds, downstream = {}, []
        threshold = thresholds.get(row["rule"], quality["default_max_failed_pct"])
        if float(row["failed_pct"] or 0) > threshold:
            incidents.append({**row, "threshold_pct": threshold, "downstream": downstream,
                              "failed_pct": float(row["failed_pct"]),
                              "baseline_failed_pct": float(row["baseline_failed_pct"])
                              if row["baseline_failed_pct"] is not None else None})
    return sorted(incidents, key=lambda i: i["failed_pct"] / max(i["threshold_pct"], 0.01), reverse=True)


def sample_failed_rows(db: Database, pipeline: str, rule: str, limit: int = 20) -> dict:
    """Recent quarantined rows plus a field profile, so the agent can tell a schema change from bad data."""
    rows = db.fetch_all(
        """SELECT payload, batch_id, quarantined_at FROM dq.quarantine_row
           WHERE pipeline = %s AND rule = %s ORDER BY quarantined_at DESC LIMIT %s""",
        (pipeline, rule, min(max(limit, 1), 100)))
    payloads = [r["payload"] for r in rows]
    fields = Counter(k for p in payloads for k in p)
    profile = {}
    for name in sorted(fields):
        values = [p.get(name) for p in payloads]
        missing = sum(1 for v in values if v is None)
        profile[name] = {"present_in_pct": round(100 * fields[name] / max(len(payloads), 1), 1),
                         "null_or_missing": missing + (len(payloads) - fields[name]),
                         "top_values": Counter(str(v) for v in values if v is not None).most_common(3)}
    healthy_fields = db.fetch_one(
        """SELECT array_agg(DISTINCT k ORDER BY k) AS fields
           FROM (SELECT payload FROM dq.quarantine_row WHERE pipeline = %s AND rule <> %s
                 ORDER BY quarantined_at DESC LIMIT 200) recent, jsonb_object_keys(recent.payload) AS k""",
        (pipeline, rule))
    return {"pipeline": pipeline, "rule": rule, "rows": payloads[:limit], "field_profile": profile,
            "fields_seen_in_other_rules": (healthy_fields or {}).get("fields") or []}


def failure_history(db: Database, pipeline: str, rule: str, hours: int = 24) -> list[dict]:
    return db.fetch_all(
        """SELECT date_trunc('hour', checked_at) AS hour, sum(checked_rows) AS checked_rows,
                  sum(failed_rows) AS failed_rows,
                  round(100.0 * sum(failed_rows) / nullif(sum(checked_rows), 0), 3) AS failed_pct
           FROM dq.check_result
           WHERE pipeline = %s AND rule = %s AND checked_at > now() - make_interval(hours => %s)
           GROUP BY 1 ORDER BY 1""", (pipeline, rule, min(max(hours, 1), 168)))

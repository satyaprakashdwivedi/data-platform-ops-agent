-- Schema for the ops agent, applied by ops-tools on start-up
-- (safe to run again: everything is IF NOT EXISTS).

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS dq;

-- One row per workflow execution that touched the agent (Spark triage, DQ incident, weekly review).
CREATE TABLE IF NOT EXISTS ops.agent_run (
    run_id            BIGSERIAL PRIMARY KEY,
    workflow          TEXT NOT NULL,
    subject           TEXT,                       -- pipeline or app id the run is about
    trigger_payload   JSONB NOT NULL DEFAULT '{}',
    n8n_execution_id  TEXT,
    status            TEXT NOT NULL DEFAULT 'running',
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ
);

-- Every tool the agent (MCP) or the workflow (REST) called: the audit trail.
CREATE TABLE IF NOT EXISTS ops.tool_call (
    call_id      BIGSERIAL PRIMARY KEY,
    run_id       BIGINT REFERENCES ops.agent_run(run_id),
    channel      TEXT NOT NULL CHECK (channel IN ('mcp', 'rest')),
    tool         TEXT NOT NULL,
    arguments    JSONB NOT NULL DEFAULT '{}',
    succeeded    BOOLEAN NOT NULL,
    error        TEXT,
    duration_ms  INTEGER NOT NULL,
    called_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS tool_call_run_idx ON ops.tool_call (run_id);

-- Output of the deterministic Spark analyzer for one application.
CREATE TABLE IF NOT EXISTS ops.spark_analysis (
    analysis_id      BIGSERIAL PRIMARY KEY,
    run_id           BIGINT REFERENCES ops.agent_run(run_id),
    pipeline         TEXT,
    app_id           TEXT,
    app_name         TEXT,
    diagnosis        TEXT NOT NULL,             -- primary root-cause class
    findings         JSONB NOT NULL,
    recommended      JSONB NOT NULL,
    summary          JSONB NOT NULL,
    idle_core_hours  NUMERIC,
    analysed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A change the agent wants to make. Nothing is applied until status = 'approved'.
CREATE TABLE IF NOT EXISTS ops.proposal (
    proposal_id       BIGSERIAL PRIMARY KEY,
    run_id            BIGINT REFERENCES ops.agent_run(run_id),
    kind              TEXT NOT NULL CHECK (kind IN ('spark_conf', 'dq_action')),
    pipeline          TEXT NOT NULL,
    changes           JSONB NOT NULL,
    diagnosis         TEXT,
    rationale         TEXT,
    confidence        NUMERIC,
    status            TEXT NOT NULL CHECK (status IN ('rejected_by_guardrails', 'pending_approval', 'approved',
                                                      'declined', 'expired', 'applied', 'apply_failed')),
    guardrail_report  JSONB NOT NULL DEFAULT '{}',
    idempotency_key   TEXT NOT NULL,           -- same change for the same pipeline = same key
    decided_by        TEXT,
    decision_comment  TEXT,
    decided_at        TIMESTAMPTZ,
    applied_ref       TEXT,                      -- PR URL, patch file or control-table change
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- At most one open (pending or approved-but-not-applied) proposal per change: retries and repeated
-- alerts reuse it instead of asking the approver twice.
CREATE UNIQUE INDEX IF NOT EXISTS proposal_open_once_idx ON ops.proposal (idempotency_key)
    WHERE status IN ('pending_approval', 'approved');

-- Messages sent to people (Slack when configured, always recorded here).
CREATE TABLE IF NOT EXISTS ops.notification (
    notification_id  BIGSERIAL PRIMARY KEY,
    run_id           BIGINT REFERENCES ops.agent_run(run_id),
    title            TEXT NOT NULL,
    body             TEXT NOT NULL,
    links            JSONB NOT NULL DEFAULT '{}',
    delivered_to     TEXT NOT NULL,
    sent_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Downstream consumers check this before reading a pipeline's output.
CREATE TABLE IF NOT EXISTS ops.pipeline_control (
    pipeline    TEXT PRIMARY KEY,
    paused      BOOLEAN NOT NULL DEFAULT false,
    reason      TEXT,
    updated_by  TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Written by the streaming pipelines' quality step (one row per rule per micro-batch).
CREATE TABLE IF NOT EXISTS dq.check_result (
    check_id      BIGSERIAL PRIMARY KEY,
    pipeline      TEXT NOT NULL,
    batch_id      BIGINT NOT NULL,
    rule          TEXT NOT NULL,
    checked_rows  BIGINT NOT NULL,
    failed_rows   BIGINT NOT NULL,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS check_result_recent_idx ON dq.check_result (pipeline, rule, checked_at DESC);

-- Rows that failed a rule (the pipeline's <step>_quarantine output, sunk over JDBC).
CREATE TABLE IF NOT EXISTS dq.quarantine_row (
    row_id          BIGSERIAL PRIMARY KEY,
    pipeline        TEXT NOT NULL,
    batch_id        BIGINT NOT NULL,
    rule            TEXT NOT NULL,
    payload         JSONB NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS quarantine_recent_idx ON dq.quarantine_row (pipeline, rule, quarantined_at DESC);

import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "demo"))


@pytest.fixture(scope="session")
def repo() -> Path:
    return REPO


@pytest.fixture(scope="session")
def policy(repo):
    from ops_tools.guardrails import load_policy
    return load_policy(repo / "config" / "policy.yaml")


@pytest.fixture(scope="session")
def demo_logs(tmp_path_factory) -> Path:
    import make_demo_data
    root = tmp_path_factory.mktemp("spark-events")
    make_demo_data.main(root)
    return root


@pytest.fixture()
def service(repo, demo_logs, tmp_path, monkeypatch):
    """OpsService against a real Postgres (DATABASE_URL); skipped when none is available."""
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("set TEST_DATABASE_URL to run database tests")
    for name, value in {"DATABASE_URL": url, "SCHEMA_FILE": str(repo / "db" / "schema.sql"),
                        "POLICY_FILE": str(repo / "config" / "policy.yaml"),
                        "PIPELINES_DIR": str(repo / "pipelines"), "SPARK_EVENT_LOG_DIR": str(demo_logs),
                        "OUTBOX_DIR": str(tmp_path / "outbox"), "GITHUB_TOKEN": "", "GITHUB_REPO": "",
                        "SLACK_WEBHOOK_URL": ""}.items():
        monkeypatch.setenv(name, value)
    from ops_tools.db import Database
    from ops_tools.service import OpsService
    from ops_tools.settings import load_settings
    settings = load_settings()
    db = Database(settings.database_url, settings.schema_file)
    db.create_schema()
    with db.connection() as conn:
        conn.execute("TRUNCATE ops.tool_call, ops.spark_analysis, ops.proposal, ops.notification, "
                     "ops.pipeline_control, ops.agent_run, dq.check_result, dq.quarantine_row RESTART IDENTITY CASCADE")
    yield OpsService(settings, db)
    db.close()

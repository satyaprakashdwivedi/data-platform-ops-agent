"""Runtime settings, all from environment variables (see .env.example)."""
import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=lambda: _env("DATABASE_URL", "postgresql://ops:ops@localhost:5432/ops"))
    api_token: str = field(default_factory=lambda: _env("OPS_TOOLS_TOKEN"))
    schema_file: Path = field(default_factory=lambda: Path(_env("SCHEMA_FILE", "db/schema.sql")))
    policy_file: Path = field(default_factory=lambda: Path(_env("POLICY_FILE", "config/policy.yaml")))
    pipelines_dir: Path = field(default_factory=lambda: Path(_env("PIPELINES_DIR", "pipelines")))
    spark_event_log_dir: Path = field(default_factory=lambda: Path(_env("SPARK_EVENT_LOG_DIR", "demo/spark-events")))
    spark_history_server_url: str = field(default_factory=lambda: _env("SPARK_HISTORY_SERVER_URL"))
    outbox_dir: Path = field(default_factory=lambda: Path(_env("OUTBOX_DIR", "outbox")))
    slack_webhook_url: str = field(default_factory=lambda: _env("SLACK_WEBHOOK_URL"))
    github_token: str = field(default_factory=lambda: _env("GITHUB_TOKEN"))
    github_repo: str = field(default_factory=lambda: _env("GITHUB_REPO"))          # owner/name
    github_base_branch: str = field(default_factory=lambda: _env("GITHUB_BASE_BRANCH", "main"))
    github_config_root: str = field(default_factory=lambda: _env("GITHUB_CONFIG_ROOT", "pipelines"))

    @property
    def opens_real_pull_requests(self) -> bool:
        return bool(self.github_token and self.github_repo)


def load_settings() -> Settings:
    return Settings()

"""Read pipeline configs and quality rules, and render the file change a proposal would make."""
import difflib
import re
from pathlib import Path

import yaml

SAFE_NAME = re.compile(r"^[a-z0-9_]{1,64}$")


class PipelineNotFound(LookupError):
    pass


class PipelineRepository:
    def __init__(self, root: Path):
        self.root = root

    def _path(self, pipeline: str, rules: bool = False) -> Path:
        if not SAFE_NAME.match(pipeline):
            raise ValueError(f"invalid pipeline name: {pipeline!r}")
        return self.root / "rules" / f"{pipeline}.yaml" if rules else self.root / f"{pipeline}.yaml"

    def relative_path(self, pipeline: str, rules: bool = False) -> str:
        return str(self._path(pipeline, rules).relative_to(self.root))

    def names(self) -> list[str]:
        return sorted(p.stem for p in self.root.glob("*.yaml"))

    def config_text(self, pipeline: str) -> str:
        path = self._path(pipeline)
        if not path.exists():
            raise PipelineNotFound(f"no config for pipeline {pipeline!r}")
        return path.read_text()

    def config(self, pipeline: str) -> dict:
        return yaml.safe_load(self.config_text(pipeline))

    def rules_text(self, pipeline: str) -> str | None:
        path = self._path(pipeline, rules=True)
        return path.read_text() if path.exists() else None

    def rules(self, pipeline: str) -> dict | None:
        text = self.rules_text(pipeline)
        return yaml.safe_load(text) if text else None


def with_spark_conf(config_text: str, changes: dict) -> str:
    """Return the config file with spark_conf entries changed (comments and order kept)."""
    lines = config_text.splitlines(keepends=True)
    remaining = {k: str(v) for k, v in changes.items()}
    in_block, block_end = False, len(lines)
    for i, line in enumerate(lines):
        if re.match(r"^spark_conf:\s*$", line):
            in_block = True
            continue
        if in_block:
            if line.strip() and not line.startswith((" ", "\t")):
                block_end = i
                break
            match = re.match(r"^(\s+)([\w.]+):\s*(.*?)\s*$", line)
            if match and match.group(2) in remaining:
                indent, key = match.group(1), match.group(2)
                lines[i] = f"{indent}{key}: {_yaml_scalar(remaining.pop(key))}\n"
                block_end = i + 1
            elif match:
                block_end = i + 1
    if not in_block:
        lines.append("spark_conf:\n")
        block_end = len(lines)
    lines[block_end:block_end] = [f"  {k}: {_yaml_scalar(v)}\n" for k, v in remaining.items()]
    return "".join(lines)


def with_rule_threshold(rules_text: str, rule_name: str, max_failed_pct: float) -> str:
    document = yaml.safe_load(rules_text)
    for rule in document.get("rules", []):
        if rule["name"] == rule_name:
            rule["max_failed_pct"] = max_failed_pct
    header = "".join(line for line in rules_text.splitlines(keepends=True) if line.startswith("#"))
    return header + yaml.safe_dump(document, sort_keys=False)


def unified_diff(before: str, after: str, path: str) -> str:
    return "".join(difflib.unified_diff(before.splitlines(keepends=True), after.splitlines(keepends=True),
                                        fromfile=f"a/{path}", tofile=f"b/{path}"))


def _yaml_scalar(value: str) -> str:
    # numbers and booleans stay strings so Spark gets exactly what was approved
    return f'"{value}"' if re.fullmatch(r"[\d.]+|true|false", value) else value

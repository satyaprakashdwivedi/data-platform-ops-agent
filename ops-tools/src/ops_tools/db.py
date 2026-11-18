"""Postgres access: a small connection pool and helpers that return plain dicts."""
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool


class Database:
    def __init__(self, database_url: str, schema_file: Path):
        self.pool = ConnectionPool(database_url, min_size=1, max_size=10, open=True,
                                   kwargs={"row_factory": dict_row, "autocommit": True})
        self.schema_file = schema_file

    def close(self) -> None:
        self.pool.close()

    def create_schema(self) -> None:
        if self.schema_file.exists():
            with self.connection() as conn:
                conn.execute(self.schema_file.read_text())

    @contextmanager
    def connection(self) -> Iterator[Any]:
        with self.pool.connection() as conn:
            yield conn

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        with self.pool.connection() as conn:
            with conn.transaction():
                yield conn

    def fetch_all(self, sql: str, params: dict | tuple | None = None) -> list[dict]:
        with self.connection() as conn:
            return conn.execute(sql, params).fetchall()

    def fetch_one(self, sql: str, params: dict | tuple | None = None) -> dict | None:
        with self.connection() as conn:
            return conn.execute(sql, params).fetchone()


def as_json(value: Any) -> Jsonb:
    """Wrap a Python value for a JSONB column."""
    return Jsonb(value, dumps=_dumps)


def _dumps(value: Any) -> str:
    import json
    return json.dumps(value, default=str)

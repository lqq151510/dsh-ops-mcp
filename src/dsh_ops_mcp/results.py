"""Bounded local result storage for batch runs."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
import uuid
from pathlib import Path
from typing import Any

MAX_RESULT_AGE_SECONDS = 24 * 60 * 60
MAX_STORE_BYTES = 64 * 1024 * 1024
MAX_PAGE_BYTES = 64 * 1024


def _prefix_upper_bound(prefix: str) -> str | None:
    """Return the exclusive BINARY-collation bound for all strings starting with prefix."""
    codepoints = list(prefix)
    for index in range(len(codepoints) - 1, -1, -1):
        value = ord(codepoints[index])
        if value < 0x10FFFF:
            return "".join(codepoints[:index]) + chr(value + 1)
    return None


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _state_dir() -> Path:
    override = os.environ.get("DSH_OPS_STATE_DIR")
    if override:
        path = Path(override).expanduser()
    elif sys.platform == "darwin":
        path = Path.home() / "Library" / "Application Support" / "dsh-ops-mcp"
    elif os.name == "nt":
        path = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "dsh-ops-mcp"
    else:
        path = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state")) / "dsh-ops-mcp"
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        path.chmod(0o700)
    except OSError:
        pass
    return path


class ResultStore:
    def __init__(self) -> None:
        self.db_path = _state_dir() / "results.sqlite3"
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA secure_delete=ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute(
                "CREATE TABLE IF NOT EXISTS runs ("
                "id TEXT PRIMARY KEY, created REAL NOT NULL, state TEXT NOT NULL, "
                "steps_total INTEGER NOT NULL, elapsed_ms INTEGER NOT NULL DEFAULT 0, owner_pid INTEGER NOT NULL DEFAULT 0)"
            )
            columns = {row["name"] for row in db.execute("PRAGMA table_info(runs)")}
            if "owner_pid" not in columns:
                db.execute("ALTER TABLE runs ADD COLUMN owner_pid INTEGER NOT NULL DEFAULT 0")
            db.execute(
                "CREATE TABLE IF NOT EXISTS result_rows ("
                "run_id TEXT NOT NULL, seq INTEGER NOT NULL, step_id TEXT NOT NULL, "
                "operation TEXT NOT NULL, state TEXT NOT NULL, payload TEXT NOT NULL, "
                "PRIMARY KEY (run_id, seq), FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE)"
            )
            result_columns = {row["name"] for row in db.execute("PRAGMA table_info(result_rows)")}
            added_columns: set[str] = set()
            for column in ("path", "query"):
                if column not in result_columns:
                    db.execute(f"ALTER TABLE result_rows ADD COLUMN {column} TEXT")
                    added_columns.add(column)
            if added_columns:
                # Preserve rows written by earlier versions while making common filters indexable.
                for row in db.execute("SELECT run_id, seq, payload FROM result_rows").fetchall():
                    payload = json.loads(row["payload"])
                    values = {column: payload.get(column) for column in added_columns}
                    values = {column: value for column, value in values.items() if isinstance(value, str)}
                    if values:
                        assignments = ", ".join(f"{column}=?" for column in values)
                        db.execute(
                            f"UPDATE result_rows SET {assignments} WHERE run_id=? AND seq=?",
                            [*values.values(), row["run_id"], row["seq"]],
                        )
            db.execute("CREATE INDEX IF NOT EXISTS result_rows_run ON result_rows(run_id, seq)")
            db.execute("CREATE INDEX IF NOT EXISTS result_rows_run_path_seq ON result_rows(run_id, path, seq)")
            db.execute("CREATE INDEX IF NOT EXISTS result_rows_run_query_seq ON result_rows(run_id, query, seq)")
            db.execute(
                "CREATE TABLE IF NOT EXISTS idempotency_keys ("
                "key TEXT PRIMARY KEY, request_hash TEXT NOT NULL, run_id TEXT NOT NULL, "
                "created REAL NOT NULL, summary TEXT)"
            )
            # Other MCP clients may share this store and still have active batches.
            for run in db.execute("SELECT id FROM runs WHERE state='running'").fetchall():
                self._recover_run(db, run["id"])
            self._delete_expired(db)
        try:
            self.db_path.chmod(0o600)
        except OSError:
            pass

    def create_run(
        self,
        steps_total: int,
        *,
        idempotency_key: str | None = None,
        request_hash: str | None = None,
    ) -> tuple[str, bool]:
        run_id = uuid.uuid4().hex
        created_at = time.time()
        with self._connect() as db:
            self._delete_expired(db)
            if idempotency_key is not None:
                existing = db.execute(
                    "SELECT run_id, request_hash FROM idempotency_keys WHERE key=?",
                    (idempotency_key,),
                ).fetchone()
                if existing is not None:
                    if existing["request_hash"] != request_hash:
                        raise ValueError("idempotency_key was already used for a different batch")
                    return existing["run_id"], False
            size = sum(
                candidate.stat().st_size
                for candidate in (self.db_path, Path(f"{self.db_path}-wal"), Path(f"{self.db_path}-shm"))
                if candidate.exists()
            )
            if size > MAX_STORE_BYTES:
                raise ValueError("local result store is full; release old result references first")
            db.execute(
                "INSERT INTO runs(id, created, state, steps_total, owner_pid) VALUES (?, ?, 'running', ?, ?)",
                (run_id, created_at, steps_total, os.getpid()),
            )
            if idempotency_key is not None:
                if request_hash is None:
                    raise ValueError("request_hash is required with idempotency_key")
                db.execute(
                    "INSERT INTO idempotency_keys(key, request_hash, run_id, created) VALUES (?, ?, ?, ?)",
                    (idempotency_key, request_hash, run_id, created_at),
                )
        return run_id, True

    @staticmethod
    def _delete_expired(db: sqlite3.Connection) -> None:
        cutoff = time.time() - MAX_RESULT_AGE_SECONDS
        db.execute("DELETE FROM runs WHERE created < ?", (cutoff,))
        db.execute("DELETE FROM idempotency_keys WHERE created < ?", (cutoff,))

    def append_rows(self, run_id: str, rows: list[dict[str, Any]]) -> None:
        with self._connect() as db:
            last = db.execute("SELECT COALESCE(MAX(seq), -1) FROM result_rows WHERE run_id=?", (run_id,)).fetchone()[0]
            db.executemany(
                "INSERT INTO result_rows(run_id, seq, step_id, operation, state, payload, path, query) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        run_id,
                        last + index + 1,
                        str(row.get("step_id", "")),
                        str(row.get("operation", "")),
                        str(row.get("state", "completed")),
                        json.dumps(row, ensure_ascii=False, separators=(",", ":")),
                        row.get("path") if isinstance(row.get("path"), str) else None,
                        row.get("query") if isinstance(row.get("query"), str) else None,
                    )
                    for index, row in enumerate(rows)
                ],
            )

    def replace_last_step_rows(self, run_id: str, step_id: str, rows: list[dict[str, Any]]) -> None:
        with self._connect() as db:
            old = db.execute(
                "SELECT MIN(seq) AS first_seq, COUNT(*) AS old_count FROM result_rows WHERE run_id=? AND step_id=?",
                (run_id, step_id),
            ).fetchone()
            first_seq = old["first_seq"]
            old_count = old["old_count"]
            if first_seq is None:
                return
            db.execute("UPDATE result_rows SET seq=seq+1000000 WHERE run_id=? AND seq>=?", (run_id, first_seq))
            db.execute("DELETE FROM result_rows WHERE run_id=? AND step_id=?", (run_id, step_id))
            db.executemany(
                "INSERT INTO result_rows(run_id, seq, step_id, operation, state, payload, path, query) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        run_id,
                        first_seq + index,
                        str(row.get("step_id", "")),
                        str(row.get("operation", "")),
                        str(row.get("state", "completed")),
                        json.dumps(row, ensure_ascii=False, separators=(",", ":")),
                        row.get("path") if isinstance(row.get("path"), str) else None,
                        row.get("query") if isinstance(row.get("query"), str) else None,
                    )
                    for index, row in enumerate(rows)
                ],
            )
            db.execute(
                "UPDATE result_rows SET seq=seq-1000000+? WHERE run_id=? AND seq>=1000000",
                (len(rows) - old_count, run_id),
            )

    def update_run(self, run_id: str, state: str, elapsed_ms: int) -> None:
        with self._connect() as db:
            db.execute("UPDATE runs SET state=?, elapsed_ms=? WHERE id=?", (state, elapsed_ms, run_id))

    def save_idempotent_summary(self, run_id: str, summary: dict[str, Any]) -> None:
        # Keep only counts and status so releasing details also removes file paths and contents.
        compact = {
            key: summary[key]
            for key in ("status", "steps_total", "done", "failed", "unknown", "pending", "details_total", "elapsed_ms")
        }
        with self._connect() as db:
            db.execute(
                "UPDATE idempotency_keys SET summary=? WHERE run_id=?",
                (json.dumps(compact, separators=(",", ":")), run_id),
            )

    def run_snapshot(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            self._recover_run(db, run_id)
            run = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                return None
            rows = db.execute(
                "SELECT payload FROM result_rows WHERE run_id=? ORDER BY seq", (run_id,)
            ).fetchall()
            idempotency = db.execute(
                "SELECT summary FROM idempotency_keys WHERE run_id=?", (run_id,)
            ).fetchone()
        return {
            "run": dict(run),
            "rows": [json.loads(row["payload"]) for row in rows],
            "summary": json.loads(idempotency["summary"]) if idempotency and idempotency["summary"] else None,
        }

    @staticmethod
    def _recover_run(db: sqlite3.Connection, run_id: str) -> None:
        run = db.execute("SELECT state, owner_pid FROM runs WHERE id=?", (run_id,)).fetchone()
        if run is None or run["state"] != "running" or _pid_alive(run["owner_pid"]):
            return
        rows = db.execute(
            "SELECT seq, payload FROM result_rows WHERE run_id=? AND state='running'", (run_id,)
        ).fetchall()
        for row in rows:
            payload = json.loads(row["payload"])
            payload["state"] = "unknown"
            db.execute(
                "UPDATE result_rows SET state='unknown', payload=? WHERE run_id=? AND seq=?",
                (json.dumps(payload, ensure_ascii=False, separators=(",", ":")), run_id, row["seq"]),
            )
        db.execute("UPDATE runs SET state='interrupted' WHERE id=?", (run_id,))

    def page(
        self,
        run_id: str,
        *,
        offset: int,
        limit: int,
        max_bytes: int,
        fields: list[str] | None = None,
        step_ids: list[str] | None = None,
        operations: list[str] | None = None,
        states: list[str] | None = None,
        path_prefix: str | None = None,
        queries: list[str] | None = None,
    ) -> dict[str, Any]:
        if not run_id or len(run_id) != 32 or any(ch not in "0123456789abcdef" for ch in run_id):
            raise ValueError("invalid result reference")
        if offset < 0 or not 1 <= limit <= 200:
            raise ValueError("offset must be non-negative and limit must be between 1 and 200")
        if not 1024 <= max_bytes <= MAX_PAGE_BYTES:
            raise ValueError(f"max_bytes must be between 1024 and {MAX_PAGE_BYTES}")
        allowed = {"step_id", "operation", "state", "path", "query", "other", "line", "value", "error", "count", "sha256", "bytes", "lines"}
        selected = fields or sorted(allowed)
        if not selected or set(selected) - allowed:
            raise ValueError(f"fields may only contain: {', '.join(sorted(allowed))}")

        filters: list[tuple[str, list[str]]] = []
        for label, values, permitted in (
            ("step_ids", step_ids, None),
            ("operations", operations, {"mkdir", "write_text", "replace_text", "copy", "read_text", "glob", "grep", "grep_many", "count_lines", "sha256", "assert_contains", "assert_equal", "exists"}),
            ("states", states, {"completed", "failed", "unknown", "pending", "preflight_ok", "running"}),
            ("queries", queries, None),
        ):
            if values is None:
                continue
            if not isinstance(values, list) or not 1 <= len(values) <= 100:
                raise ValueError(f"{label} must contain between 1 and 100 values")
            max_length = 128
            if any(not isinstance(value, str) or not value or len(value) > max_length for value in values):
                raise ValueError(f"{label} values must be non-empty strings no longer than {max_length} characters")
            if permitted is not None and set(values) - permitted:
                raise ValueError(f"{label} contains an unsupported value")
            filters.append((label, list(dict.fromkeys(values))))
        if path_prefix is not None and (not isinstance(path_prefix, str) or not path_prefix or len(path_prefix) > 512):
            raise ValueError("path_prefix must be a non-empty string no longer than 512 characters")

        with self._connect() as db:
            self._delete_expired(db)
            self._recover_run(db, run_id)
            run = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise ValueError("result reference not found or expired")
            total_all = db.execute("SELECT COUNT(*) FROM result_rows WHERE run_id=?", (run_id,)).fetchone()[0]
            where = ["run_id=?"]
            parameters: list[Any] = [run_id]
            for label, values in filters:
                column = {"step_ids": "step_id", "operations": "operation", "states": "state", "queries": "query"}[label]
                where.append(f"{column} IN ({','.join('?' for _ in values)})")
                parameters.extend(values)
            if path_prefix is not None:
                upper = _prefix_upper_bound(path_prefix)
                where.append("path >= ?")
                parameters.append(path_prefix)
                if upper is not None:
                    where.append("path < ?")
                    parameters.append(upper)
            where_sql = " AND ".join(where)
            total = db.execute(f"SELECT COUNT(*) FROM result_rows WHERE {where_sql}", parameters).fetchone()[0]
            rows = db.execute(
                f"SELECT payload FROM result_rows WHERE {where_sql} ORDER BY seq LIMIT ? OFFSET ?",
                [*parameters, limit, offset],
            ).fetchall()

        items: list[dict[str, Any]] = []
        used = len('{"id":"","offset":0,"total":0,"items":[],"next_offset":null,"complete":true}')
        next_offset: int | None = None
        for index, row in enumerate(rows):
            full = json.loads(row["payload"])
            item = {key: full[key] for key in selected if key in full}
            encoded = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
            if len(encoded.encode("utf-8")) + used > max_bytes:
                next_offset = offset + index
                if not items:
                    raise ValueError("one result row exceeds max_bytes; request fewer fields")
                break
            items.append(item)
            used += len(encoded.encode("utf-8")) + 1

        end = offset + len(items)
        if next_offset is None and end < total:
            next_offset = end
        return {
            "id": run_id,
            "state": run["state"],
            "offset": offset,
            "total": total,
            "total_all": total_all,
            "items": items,
            "next_offset": next_offset,
            "complete": next_offset is None,
            "elapsed_ms": run["elapsed_ms"],
        }

    def release(self, run_id: str) -> bool:
        if not run_id or len(run_id) != 32 or any(ch not in "0123456789abcdef" for ch in run_id):
            raise ValueError("invalid result reference")
        with self._connect() as db:
            self._recover_run(db, run_id)
            run = db.execute("SELECT state FROM runs WHERE id=?", (run_id,)).fetchone()
            if run is not None and run["state"] == "running":
                raise ValueError("cannot release a result reference while its batch is still running")
            idempotency = db.execute("SELECT 1 FROM idempotency_keys WHERE run_id=?", (run_id,)).fetchone()
            if idempotency is not None:
                db.execute("DELETE FROM result_rows WHERE run_id=?", (run_id,))
                db.execute("UPDATE runs SET state='released' WHERE id=?", (run_id,))
                return run is not None
            cursor = db.execute("DELETE FROM runs WHERE id=?", (run_id,))
            return cursor.rowcount > 0

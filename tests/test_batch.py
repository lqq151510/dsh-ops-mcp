from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from dsh_ops_mcp.batch import _normalize_steps, _op_rows, run_batch
from dsh_ops_mcp.filesystem import roots
from dsh_ops_mcp.results import ResultStore


class BatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.workspace = self.base / "workspace"
        self.workspace.mkdir()
        self.state = self.base / "state"
        self.env = patch.dict(os.environ, {"DSH_OPS_ROOTS": str(self.workspace), "DSH_OPS_STATE_DIR": str(self.state)})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.addCleanup(self.temp.cleanup)
        self.store = ResultStore()

    def page_all(self, result: dict, page_size: int = 20) -> list[dict]:
        rows: list[dict] = []
        offset = 0
        while True:
            page = self.store.page(result["details_ref"], offset=offset, limit=page_size, max_bytes=64_000)
            rows.extend(page["items"])
            if page["next_offset"] is None:
                return rows
            offset = page["next_offset"]

    def test_batch_mutation_uses_prior_hash_and_verifies(self) -> None:
        source = self.workspace / "README.md"
        source.write_text("# Old\nbody\n", encoding="utf-8")
        result = run_batch(
            [
                {"id": "before", "op": "read_text", "path": "README.md"},
                {
                    "op": "replace_text",
                    "path": "README.md",
                    "pattern": "# Old",
                    "replacement": "# New",
                    "expected_sha256": "$before.sha256",
                },
                {"op": "assert_contains", "path": "README.md", "text": "# New"},
            ],
            self.store,
            idempotency_key="replace-readme-1",
        )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["done"], 3)
        self.assertEqual(source.read_text(encoding="utf-8"), "# New\nbody\n")
        self.assertEqual(len(self.page_all(result)), 4)

    def test_stale_hash_fails_preflight_without_mutation(self) -> None:
        source = self.workspace / "README.md"
        source.write_text("original\n", encoding="utf-8")
        result = run_batch(
            [{"op": "write_text", "path": "README.md", "content": "replacement\n", "expected_sha256": "0" * 64}],
            self.store,
            idempotency_key="stale-write-1",
        )
        self.assertEqual(result["status"], "preflight_failed")
        self.assertEqual(result["failed"], 1)
        self.assertEqual(source.read_text(encoding="utf-8"), "original\n")
        self.assertEqual(self.page_all(result)[0]["state"], "failed")

    def test_200_result_rows_are_stored_and_paged(self) -> None:
        steps = [{"op": "mkdir", "path": "handlers"}]
        for index in range(200):
            steps.append(
                {
                    "op": "write_text",
                    "path": f"handlers/handler-{index:03}.js",
                    "content": f"export const id = {index};\n",
                    "expected_sha256": None,
                }
            )
        steps.append({"id": "all", "op": "glob", "pattern": "**/*.js", "limit": 250})
        result = run_batch(steps, self.store, idempotency_key="bulk-create-1")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["done"], 202)
        self.assertEqual(len(result["preview"]), 5)
        self.assertEqual(result["preview_next_offset"], 5)
        first = self.store.page(result["details_ref"], offset=0, limit=10, max_bytes=12_000)
        self.assertEqual(len(first["items"]), 10)
        self.assertEqual(first["next_offset"], 10)
        rows = self.page_all(result)
        glob_rows = [row for row in rows if row["step_id"] == "all"]
        self.assertEqual(len(glob_rows), 200)
        receipt_chars = len(json.dumps(result, ensure_ascii=False))
        all_detail_chars = sum(len(json.dumps(row, ensure_ascii=False)) for row in rows)
        self.assertLess(receipt_chars, all_detail_chars)

    def test_small_result_is_in_batch_preview_without_followup_page(self) -> None:
        for name in ("a.txt", "b.txt", "c.txt"):
            (self.workspace / name).write_text("target\n", encoding="utf-8")
        result = run_batch([{"op": "glob", "pattern": "*.txt", "limit": 10}], self.store)
        self.assertEqual(result["details_total"], 3)
        self.assertEqual(len(result["preview"]), 3)
        self.assertIsNone(result["preview_next_offset"])

    def test_truncated_preview_points_back_to_full_result_row(self) -> None:
        text = "x" * 600
        (self.workspace / "long.txt").write_text(text, encoding="utf-8")
        result = run_batch([{"op": "read_text", "path": "long.txt", "max_chars": 1000}], self.store)
        self.assertEqual(result["preview"][0]["value"], text[:240])
        self.assertEqual(result["preview"][0]["truncated_fields"], ["value"])
        self.assertEqual(result["preview_next_offset"], 0)
        full = self.store.page(
            result["details_ref"], offset=result["preview_next_offset"], limit=1,
            max_bytes=12_000, fields=["path", "line", "value"],
        )
        self.assertEqual(full["items"][0]["value"], text)

    def test_result_page_filters_and_reports_filtered_and_total_counts(self) -> None:
        source_dir = self.workspace / "src"
        source_dir.mkdir()
        (source_dir / "alpha.txt").write_text("target\n", encoding="utf-8")
        (source_dir / "beta.txt").write_text("target\n", encoding="utf-8")
        result = run_batch(
            [
                {"id": "first", "op": "grep", "pattern": "target"},
                {"id": "second", "op": "grep", "pattern": "target"},
            ],
            self.store,
        )
        page = self.store.page(
            result["details_ref"], offset=0, limit=10, max_bytes=12_000,
            step_ids=["first"], operations=["grep"], states=["completed"],
            path_prefix=f"{self.workspace.name}/src/alpha",
        )
        self.assertEqual(page["total"], 1)
        self.assertEqual(page["total_all"], 4)
        self.assertEqual(page["items"][0]["path"], f"{self.workspace.name}/src/alpha.txt")

    def test_expected_hash_preserves_existing_file_mode(self) -> None:
        path = self.workspace / "script.py"
        path.write_text("print('old')\n", encoding="utf-8")
        path.chmod(0o644)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        result = run_batch(
            [{"op": "write_text", "path": "script.py", "content": "print('new')\n", "expected_sha256": digest}],
            self.store,
            idempotency_key="preserve-mode-1",
        )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o644)

    def test_write_copy_count_and_compare_run_in_one_batch(self) -> None:
        result = run_batch(
            [
                {"op": "mkdir", "path": "mc-bench"},
                {"op": "write_text", "path": "mc-bench/README.md", "content": "# Demo\nStart: index.html\n", "expected_sha256": None},
                {"op": "copy", "path": "mc-bench/README.md", "to": "mc-bench/snapshot.md"},
                {"op": "assert_equal", "path": "mc-bench/README.md", "other": "mc-bench/snapshot.md"},
                {"op": "count_lines", "path": "mc-bench/snapshot.md"},
            ],
            self.store,
            idempotency_key="copy-and-compare-1",
        )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["done"], 5)
        rows = self.page_all(result)
        compare = next(row for row in rows if row["operation"] == "assert_equal")
        self.assertIs(compare["value"], True)
        self.assertEqual(compare["other"], "mc-bench/snapshot.md")
        count = next(row["count"] for row in rows if row["operation"] == "count_lines")
        self.assertEqual(count, 2)

    def test_symlink_escape_is_refused(self) -> None:
        outside = self.base / "outside.txt"
        outside.write_text("secret", encoding="utf-8")
        (self.workspace / "link.txt").symlink_to(outside)
        result = run_batch([{"op": "read_text", "path": "link.txt"}], self.store)
        self.assertEqual(result["status"], "preflight_failed")
        self.assertEqual(outside.read_text(encoding="utf-8"), "secret")

    def test_failed_mutating_operation_is_reported_unknown(self) -> None:
        source = self.workspace / "new.txt"
        with patch("dsh_ops_mcp.batch._op_rows", side_effect=RuntimeError("interrupted after dispatch")):
            result = run_batch(
                [{"op": "write_text", "path": "new.txt", "content": "hello", "expected_sha256": None}],
                self.store,
                idempotency_key="interrupted-write-1",
            )
        self.assertEqual(result["unknown"], 1)
        self.assertFalse(source.exists())
        self.assertEqual(self.page_all(result)[0]["state"], "unknown")

    def test_unknown_mutation_is_not_retried_after_side_effect_may_have_happened(self) -> None:
        source = self.workspace / "maybe-written.txt"

        def write_then_interrupt(step: dict) -> list[dict]:
            _op_rows(step)
            raise RuntimeError("simulated response loss after file write")

        with patch("dsh_ops_mcp.batch._op_rows", side_effect=write_then_interrupt):
            first = run_batch(
                [{"op": "write_text", "path": "maybe-written.txt", "content": "once", "expected_sha256": None}],
                self.store,
                idempotency_key="response-lost-1",
            )
        self.assertEqual(first["status"], "interrupted")
        self.assertEqual(source.read_text(encoding="utf-8"), "once")
        original_inode = source.stat().st_ino

        retry = run_batch(
            [{"op": "write_text", "path": "maybe-written.txt", "content": "once", "expected_sha256": None}],
            self.store,
            idempotency_key="response-lost-1",
        )
        self.assertEqual(retry["details_ref"], first["details_ref"])
        self.assertEqual(retry["status"], "interrupted")
        self.assertTrue(retry["deduplicated"])
        self.assertEqual(source.stat().st_ino, original_inode)

    def test_idempotency_survives_result_store_reinitialization(self) -> None:
        steps = [{"op": "write_text", "path": "durable.txt", "content": "once", "expected_sha256": None}]
        first = run_batch(steps, self.store, idempotency_key="restart-retry-1")
        original_inode = (self.workspace / "durable.txt").stat().st_ino

        restarted_store = ResultStore()
        retry = run_batch(steps, restarted_store, idempotency_key="restart-retry-1")

        self.assertEqual(retry["details_ref"], first["details_ref"])
        self.assertTrue(retry["deduplicated"])
        self.assertEqual((self.workspace / "durable.txt").stat().st_ino, original_inode)

    def test_retry_recovers_abandoned_running_mutation_as_unknown(self) -> None:
        steps = [{"op": "write_text", "path": "maybe.txt", "content": "value", "expected_sha256": None}]
        normalized = _normalize_steps(steps)
        fingerprint = {"roots": [str(root) for root in roots()], "steps": normalized}
        request_hash = hashlib.sha256(
            json.dumps(fingerprint, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        run_id, created = self.store.create_run(
            len(normalized), idempotency_key="abandoned-run-1", request_hash=request_hash
        )
        self.assertTrue(created)
        self.store.append_rows(
            run_id,
            [{"step_id": "s1", "operation": "write_text", "state": "running", "path": "maybe.txt"}],
        )

        with patch("dsh_ops_mcp.results._pid_alive", return_value=False):
            retry = run_batch(steps, self.store, idempotency_key="abandoned-run-1")
            page = self.store.page(run_id, offset=0, limit=10, max_bytes=12_000)

        self.assertEqual(retry["details_ref"], run_id)
        self.assertEqual(retry["status"], "interrupted")
        self.assertTrue(retry["deduplicated"])
        self.assertEqual(page["state"], "interrupted")
        self.assertEqual(page["items"][0]["state"], "unknown")
        self.assertFalse((self.workspace / "maybe.txt").exists())

    def test_mutating_batches_require_an_idempotency_key(self) -> None:
        with self.assertRaisesRegex(ValueError, "idempotency_key is required"):
            run_batch(
                [{"op": "write_text", "path": "new.txt", "content": "hello", "expected_sha256": None}],
                self.store,
            )
        self.assertFalse((self.workspace / "new.txt").exists())

    def test_retry_with_same_key_returns_original_result_without_repeating_write(self) -> None:
        steps = [{"op": "write_text", "path": "new.txt", "content": "hello", "expected_sha256": None}]
        first = run_batch(steps, self.store, idempotency_key="write-once-1")
        original_stat = (self.workspace / "new.txt").stat()

        retry = run_batch(steps, self.store, idempotency_key="write-once-1")

        self.assertEqual(first["details_ref"], retry["details_ref"])
        self.assertEqual(retry["status"], "completed")
        self.assertTrue(retry["deduplicated"])
        self.assertEqual((self.workspace / "new.txt").stat().st_ino, original_stat.st_ino)
        self.assertEqual(len(self.page_all(retry)), 1)

    def test_same_key_with_different_batch_is_rejected_without_mutation(self) -> None:
        first_steps = [{"op": "write_text", "path": "one.txt", "content": "one", "expected_sha256": None}]
        run_batch(first_steps, self.store, idempotency_key="reused-key-1")
        with self.assertRaisesRegex(ValueError, "different batch"):
            run_batch(
                [{"op": "write_text", "path": "two.txt", "content": "two", "expected_sha256": None}],
                self.store,
                idempotency_key="reused-key-1",
            )
        self.assertTrue((self.workspace / "one.txt").exists())
        self.assertFalse((self.workspace / "two.txt").exists())

    def test_same_key_cannot_be_replayed_against_a_different_workspace(self) -> None:
        steps = [{"op": "write_text", "path": "same.txt", "content": "value", "expected_sha256": None}]
        run_batch(steps, self.store, idempotency_key="workspace-scope-1")
        other_workspace = self.base / "other-workspace"
        other_workspace.mkdir()

        with patch.dict(os.environ, {"DSH_OPS_ROOTS": str(other_workspace)}):
            with self.assertRaisesRegex(ValueError, "different batch"):
                run_batch(steps, self.store, idempotency_key="workspace-scope-1")

        self.assertFalse((other_workspace / "same.txt").exists())

    def test_concurrent_retry_observes_in_progress_instead_of_mutating_twice(self) -> None:
        entered_operation = threading.Event()
        allow_operation = threading.Event()
        steps = [{"op": "write_text", "path": "new.txt", "content": "hello", "expected_sha256": None}]

        def delayed_op_rows(step: dict) -> list[dict]:
            entered_operation.set()
            if not allow_operation.wait(timeout=5):
                raise TimeoutError("test operation gate timed out")
            return _op_rows(step)

        with patch("dsh_ops_mcp.batch._op_rows", side_effect=delayed_op_rows):
            with ThreadPoolExecutor(max_workers=1) as pool:
                first_future = pool.submit(run_batch, steps, self.store, "concurrent-write-1")
                self.assertTrue(entered_operation.wait(timeout=5))
                retry = run_batch(steps, self.store, "concurrent-write-1")
                allow_operation.set()
                first = first_future.result(timeout=5)

        self.assertEqual(retry["details_ref"], first["details_ref"])
        self.assertEqual(retry["status"], "in_progress")
        self.assertEqual(first["status"], "completed")
        self.assertEqual((self.workspace / "new.txt").read_text(encoding="utf-8"), "hello")

    def test_releasing_idempotent_result_keeps_retry_tombstone(self) -> None:
        steps = [{"op": "write_text", "path": "new.txt", "content": "hello", "expected_sha256": None}]
        original = run_batch(steps, self.store, idempotency_key="released-write-1")
        self.assertTrue(self.store.release(original["details_ref"]))

        retry = run_batch(steps, self.store, idempotency_key="released-write-1")

        self.assertEqual(retry["details_ref"], original["details_ref"])
        self.assertEqual(retry["status"], "released")
        self.assertTrue(retry["deduplicated"])
        self.assertTrue(retry["details_released"])
        self.assertEqual(retry["done"], 1)
        self.assertEqual((self.workspace / "new.txt").read_text(encoding="utf-8"), "hello")


if __name__ == "__main__":
    unittest.main()

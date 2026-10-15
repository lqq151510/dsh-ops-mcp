"""Safe, bounded file-operation batches with compact, referenceable results."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
import stat
import tempfile
import time
from pathlib import Path
from typing import Any

from .filesystem import MAX_FILE_BYTES, roots, safe_pattern, safe_target, walk
from .results import ResultStore

MAX_STEPS = 512
MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
MAX_COPY_BYTES = 32 * 1024 * 1024
MAX_STEP_ROWS = 5_000
MAX_TEXT_OUTPUT = 30_000
MAX_LINE_CHARS = 2_000
MAX_PREVIEW_ROWS = 5
MAX_PREVIEW_BYTES = 3_000
MAX_PREVIEW_TEXT_CHARS = 240
_REF = re.compile(r"^\$([A-Za-z0-9_-]{1,40})\.sha256$")
_ID = re.compile(r"^[A-Za-z0-9_-]{1,40}$")
_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")

_FIELDS: dict[str, set[str]] = {
    "mkdir": {"id", "op", "path"},
    "write_text": {"id", "op", "path", "content", "expected_sha256"},
    "replace_text": {"id", "op", "path", "pattern", "replacement", "count", "expected_sha256"},
    "copy": {"id", "op", "path", "to", "expected_sha256"},
    "read_text": {"id", "op", "path", "start_line", "limit", "max_chars"},
    "glob": {"id", "op", "pattern", "limit"},
    "grep": {"id", "op", "pattern", "glob", "limit", "case_sensitive"},
    "grep_many": {"id", "op", "patterns", "glob", "limit", "case_sensitive"},
    "count_lines": {"id", "op", "path"},
    "sha256": {"id", "op", "path"},
    "assert_contains": {"id", "op", "path", "text"},
    "assert_equal": {"id", "op", "path", "other"},
    "exists": {"id", "op", "path"},
}
_REQUIRED: dict[str, set[str]] = {
    "mkdir": {"path"},
    "write_text": {"path", "content", "expected_sha256"},
    "replace_text": {"path", "pattern", "replacement", "expected_sha256"},
    "copy": {"path", "to"},
    "read_text": {"path"},
    "count_lines": {"path"},
    "sha256": {"path"},
    "assert_contains": {"path", "text"},
    "assert_equal": {"path", "other"},
    "exists": {"path"},
    "glob": set(),
    "grep": {"pattern"},
    "grep_many": {"patterns"},
}
_MUTATING = {"mkdir", "write_text", "replace_text", "copy"}


def _json_bytes(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _target(path: str, *, allow_missing: bool = False) -> tuple[Path, Path]:
    root, target = safe_target(path, allow_missing=allow_missing)
    if target == root:
        raise ValueError("a file operation cannot target the workspace root itself")
    return root, target


def _existing_file(path: str) -> tuple[Path, Path]:
    root, target = _target(path)
    if not target.is_file() or target.stat().st_size > MAX_FILE_BYTES:
        raise ValueError(f"{path!r} must be a regular file no larger than {MAX_FILE_BYTES} bytes")
    return root, target


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeError as exc:
        raise ValueError("file is not valid UTF-8 text") from exc


def _matches_glob(path: str, pattern: str) -> bool:
    # In common glob syntax, **/ also matches zero directories. fnmatch does
    # not provide that behavior, so try the pattern without its recursive prefix.
    return (
        fnmatch.fnmatchcase(path, pattern)
        or (pattern.startswith("**/") and fnmatch.fnmatchcase(path, pattern[3:]))
        or Path(path).match(pattern)
    )


def _digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _digest_file(path: Path) -> str:
    if not path.is_file() or path.stat().st_size > MAX_FILE_BYTES:
        raise ValueError(f"file must be regular and no larger than {MAX_FILE_BYTES} bytes: {path.name}")
    return _digest_bytes(path.read_bytes())


def _parent_available(target: Path, root: Path, planned_dirs: set[Path]) -> bool:
    parent = target.parent
    while parent != root:
        if parent in planned_dirs:
            return True
        if parent.exists():
            return parent.is_dir() and not parent.is_symlink()
        parent = parent.parent
    return True


def _resolve_hash(value: Any, prior: dict[str, dict[str, Any]]) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("expected_sha256 must be a SHA-256 string, null, or a prior-step $id.sha256 reference")
    match = _REF.fullmatch(value)
    if match:
        source = prior.get(match.group(1))
        if source is None or not isinstance(source.get("sha256"), str):
            raise ValueError(f"hash reference {value!r} must refer to a prior step that produced a SHA-256")
        return source["sha256"]
    if not re.fullmatch(r"[0-9a-fA-F]{64}", value):
        raise ValueError("expected_sha256 must contain 64 hexadecimal characters")
    return value.lower()


def _normalize_steps(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(steps, list) or not 1 <= len(steps) <= MAX_STEPS:
        raise ValueError(f"steps must contain between 1 and {MAX_STEPS} operations")
    if _json_bytes(steps) > MAX_INPUT_BYTES:
        raise ValueError(f"serialized batch input exceeds {MAX_INPUT_BYTES} bytes")
    normalized: list[dict[str, Any]] = []
    ids: set[str] = set()
    for index, item in enumerate(steps, 1):
        if not isinstance(item, dict):
            raise ValueError(f"step {index} must be an object")
        op = item.get("op")
        if not isinstance(op, str) or op not in _FIELDS:
            raise ValueError(f"step {index} has unsupported op; allowed: {', '.join(sorted(_FIELDS))}")
        extra = set(item) - _FIELDS[op]
        if extra:
            raise ValueError(f"step {index} has unsupported fields: {', '.join(sorted(extra))}")
        missing = _REQUIRED[op] - set(item)
        if missing:
            raise ValueError(f"step {index} is missing required fields: {', '.join(sorted(missing))}")
        step_id = item.get("id", f"s{index}")
        if not isinstance(step_id, str) or not _ID.fullmatch(step_id) or step_id in ids:
            raise ValueError(f"step {index} id must be unique and use 1-40 letters, numbers, '_' or '-'")
        ids.add(step_id)
        step = dict(item)
        step["id"] = step_id
        for field in ("path", "to", "pattern", "text", "replacement", "content", "other"):
            if field in step and not isinstance(step[field], str):
                raise ValueError(f"step {step_id}: {field} must be a string")
        for field, maximum in (("limit", 1000), ("start_line", 10_000_000), ("max_chars", MAX_TEXT_OUTPUT), ("count", 1_000_000)):
            if field in step and (not isinstance(step[field], int) or isinstance(step[field], bool) or not 1 <= step[field] <= maximum):
                raise ValueError(f"step {step_id}: {field} must be between 1 and {maximum}")
        for field in ("case_sensitive",):
            if field in step and not isinstance(step[field], bool):
                raise ValueError(f"step {step_id}: {field} must be a boolean")
        if op == "grep_many":
            patterns = step["patterns"]
            if not isinstance(patterns, list) or not 2 <= len(patterns) <= 20:
                raise ValueError(f"step {step_id}: patterns must contain between 2 and 20 strings")
            if any(not isinstance(pattern, str) or not pattern or len(pattern) > 128 for pattern in patterns):
                raise ValueError(f"step {step_id}: each pattern must be 1-128 characters")
            step["patterns"] = list(dict.fromkeys(patterns))
        if "max_chars" in step and step["op"] == "read_text" and step["max_chars"] > MAX_TEXT_OUTPUT:
            raise ValueError(f"step {step_id}: max_chars cannot exceed {MAX_TEXT_OUTPUT}")
        if op in {"write_text", "replace_text"} and "expected_sha256" not in step:
            raise ValueError(f"step {step_id}: expected_sha256 is required; use null only when creating a new file")
        normalized.append(step)
    return normalized


def _preflight(steps: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    planned_dirs: set[Path] = set()
    planned_contents: dict[Path, str] = {}
    planned_path_hashes: dict[Path, str] = {}
    planned_sizes: dict[Path, int] = {}
    total_copy_bytes = 0
    planned_hashes: dict[str, dict[str, Any]] = {}
    mutations: set[Path] = set()
    output: list[dict[str, Any]] = []

    for step in steps:
        sid, op = step["id"], step["op"]
        meta: dict[str, Any] = {"step_id": sid, "operation": op}
        try:
            if op == "mkdir":
                root, target = _target(step.get("path", ""), allow_missing=True)
                if not _parent_available(target, root, planned_dirs):
                    raise ValueError("mkdir parent does not exist or is not created by an earlier mkdir step")
                if target.exists() and not target.is_dir():
                    raise ValueError("mkdir target already exists and is not a directory")
                if target in mutations:
                    raise ValueError("multiple mutations of the same path in a single batch are not allowed")
                mutations.add(target)
                planned_dirs.add(target)
                meta.update(path=step["path"], sha256=None)

            elif op in {"write_text", "replace_text"}:
                root, target = _target(step.get("path", ""), allow_missing=True)
                if not _parent_available(target, root, planned_dirs):
                    raise ValueError("parent directory does not exist or is not created by an earlier mkdir step")
                if target.exists() and not target.is_file():
                    raise ValueError("write target is not a regular file")
                if target.exists() and target.stat().st_size > MAX_FILE_BYTES:
                    raise ValueError(f"file exceeds the {MAX_FILE_BYTES}-byte operation limit")
                if target in mutations:
                    raise ValueError("multiple mutations of the same path in a single batch are not allowed")
                mutations.add(target)
                current = planned_contents.get(target)
                if current is None and target.exists():
                    current = _read(target)
                current_bytes = current.encode("utf-8") if current is not None else None
                expected = _resolve_hash(step.get("expected_sha256"), planned_hashes)
                actual = _digest_bytes(current_bytes) if current_bytes is not None else None
                if expected != actual:
                    if current_bytes is None:
                        raise ValueError("target does not exist; pass expected_sha256: null to create it")
                    if expected is None:
                        raise ValueError("target already exists; pass its observed expected_sha256 before replacing it")
                    raise ValueError(f"target changed since it was observed (actual SHA-256: {actual})")
                meta["_resolved_expected_sha256"] = expected
                if op == "write_text":
                    content = step["content"]
                    new_content = content
                else:
                    pattern = step.get("pattern", "")
                    if not pattern:
                        raise ValueError("replace_text pattern cannot be empty")
                    if current is None:
                        raise ValueError("replace_text target must already exist")
                    count = step.get("count", 1)
                    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
                        raise ValueError("replace_text count must be a positive integer")
                    occurrences = current.count(pattern)
                    if occurrences != count:
                        raise ValueError(f"replace_text expected {count} occurrence(s), found {occurrences}")
                    new_content = current.replace(pattern, step.get("replacement", ""))
                if len(new_content.encode("utf-8")) > MAX_FILE_BYTES:
                    raise ValueError(f"new file content exceeds the {MAX_FILE_BYTES}-byte limit")
                planned_contents[target] = new_content
                digest = _digest_bytes(new_content.encode("utf-8"))
                planned_path_hashes[target] = digest
                planned_sizes[target] = len(new_content.encode("utf-8"))
                planned_hashes[sid] = {"sha256": digest}
                meta.update(path=step["path"], sha256=digest, bytes=len(new_content.encode("utf-8")))

            elif op == "copy":
                _, src = _target(step.get("path", ""), allow_missing=True)
                if src in planned_path_hashes:
                    digest = planned_path_hashes[src]
                    size = planned_sizes[src]
                    source_content = planned_contents.get(src)
                else:
                    _, src = _existing_file(step.get("path", ""))
                    digest = _digest_file(src)
                    size = src.stat().st_size
                    try:
                        source_content = _read(src)
                    except ValueError:
                        source_content = None
                total_copy_bytes += size
                if total_copy_bytes > MAX_COPY_BYTES:
                    raise ValueError(f"aggregate copy input exceeds {MAX_COPY_BYTES} bytes")
                dst_root, dst = _target(step.get("to", ""), allow_missing=True)
                if not _parent_available(dst, dst_root, planned_dirs):
                    raise ValueError("copy destination parent does not exist or is not created by an earlier mkdir step")
                if dst in mutations:
                    raise ValueError("multiple mutations of the same path in a single batch are not allowed")
                mutations.add(dst)
                expected = _resolve_hash(step.get("expected_sha256"), planned_hashes) if "expected_sha256" in step else None
                if dst.exists():
                    if expected is None or _digest_file(dst) != expected:
                        raise ValueError("copy destination exists; replacing requires its exact expected_sha256")
                elif expected is not None:
                    raise ValueError("copy destination is missing but expected_sha256 was provided")
                meta["_resolved_expected_sha256"] = expected
                meta["_resolved_source_sha256"] = digest
                planned_hashes[sid] = {"sha256": digest}
                planned_path_hashes[dst] = digest
                planned_sizes[dst] = size
                if source_content is not None:
                    planned_contents[dst] = source_content
                meta.update(path=step["path"], to=step["to"], sha256=digest, bytes=size)

            elif op == "read_text":
                _, target = _target(step.get("path", ""), allow_missing=True)
                content = planned_contents.get(target)
                if content is None:
                    if not target.exists():
                        raise ValueError("target does not exist and is not created by an earlier step")
                    _, target = _existing_file(step.get("path", ""))
                    content = _read(target)
                digest = _digest_bytes(content.encode("utf-8"))
                planned_path_hashes[target] = digest
                planned_sizes[target] = len(content.encode("utf-8"))
                planned_hashes[sid] = {"sha256": digest}
                meta.update(path=step["path"], sha256=digest, lines=len(content.splitlines()))

            elif op in {"count_lines", "sha256", "assert_contains", "exists"}:
                root, target = _target(step.get("path", ""), allow_missing=True)
                exists_after_batch = target.exists() or target in planned_path_hashes or target in planned_dirs
                if op == "exists":
                    if not exists_after_batch:
                        raise ValueError("assertion failed: target does not exist")
                    meta.update(path=step["path"], value=True)
                else:
                    if not target.exists() and target not in planned_path_hashes:
                        raise ValueError("target does not exist")
                    content = planned_contents.get(target)
                    if content is None:
                        if op == "sha256" and target in planned_path_hashes:
                            digest = planned_path_hashes[target]
                            planned_hashes[sid] = {"sha256": digest}
                            meta.update(path=step["path"], sha256=digest)
                            output.append({"step": step, "meta": meta})
                            continue
                        if not target.exists():
                            raise ValueError("target does not exist and is not created by an earlier step")
                        if not target.is_file() or target.stat().st_size > MAX_FILE_BYTES:
                            raise ValueError("target must be a regular file within the size limit")
                        content = _read(target)
                    digest = _digest_bytes(content.encode("utf-8"))
                    if op == "sha256":
                        planned_hashes[sid] = {"sha256": digest}
                        meta.update(path=step["path"], sha256=digest)
                    elif op == "count_lines":
                        meta.update(path=step["path"], count=content.count("\n"), sha256=digest)
                    else:
                        needle = step.get("text", "")
                        if needle not in content:
                            raise ValueError("assertion failed: text not found")
                        meta.update(path=step["path"], value=True, sha256=digest)

            elif op == "assert_equal":
                _, left = _target(step.get("path", ""), allow_missing=True)
                _, right = _target(step.get("other", ""), allow_missing=True)
                if not left.exists() and left not in planned_path_hashes:
                    raise ValueError("left comparison file does not exist and is not created by an earlier step")
                if not right.exists() and right not in planned_path_hashes:
                    raise ValueError("right comparison file does not exist and is not created by an earlier step")
                left_hash = planned_path_hashes.get(left) or _digest_file(left)
                right_hash = planned_path_hashes.get(right) or _digest_file(right)
                if left_hash != right_hash:
                    raise ValueError("assertion failed: files differ")
                meta.update(path=step["path"], other=step["other"], value=True, sha256=left_hash)

            elif op == "glob":
                safe_pattern(step.get("pattern", "**/*"))
                _bounded_limit(step.get("limit", 500), "glob limit", 1000)
                meta.update(pattern=step.get("pattern", "**/*"))

            elif op in {"grep", "grep_many"}:
                if op == "grep" and not step.get("pattern"):
                    raise ValueError("grep pattern cannot be empty")
                if op == "grep_many" and len(step["patterns"]) < 2:
                    raise ValueError("grep_many requires at least two distinct patterns")
                safe_pattern(step.get("glob", "**/*"))
                if op == "grep":
                    _bounded_limit(step.get("limit", 500), "grep limit", 1000)
                    meta.update(pattern=step["pattern"])
                else:
                    limit = _bounded_limit(step.get("limit", 5), "grep_many per-pattern limit", 200)
                    if len(step["patterns"]) * limit > MAX_STEP_ROWS:
                        raise ValueError(f"grep_many result bound exceeds {MAX_STEP_ROWS} rows")
                    meta.update(patterns=step["patterns"])

            output.append({"step": step, "meta": meta})
        except Exception as exc:
            meta.update(state="failed", error=str(exc)[:1000])
            output.append({"step": step, "meta": meta, "error": str(exc)[:1000]})
            break
    return output, planned_hashes


def _bounded_limit(value: Any, label: str, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= maximum:
        raise ValueError(f"{label} must be between 1 and {maximum}")
    return value


def _atomic_write(target: Path, content: bytes) -> None:
    existing_mode = stat.S_IMODE(target.stat().st_mode) if target.exists() and not target.is_symlink() else None
    if target.is_symlink():
        raise ValueError("target became a symlink before write; refusing")
    fd, temp_name = tempfile.mkstemp(prefix=".dsh-ops-", dir=target.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if existing_mode is not None:
            temp.chmod(existing_mode)
        if target.is_symlink():
            raise ValueError("target became a symlink before atomic replace; refusing")
        os.replace(temp, target)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


def _op_rows(step: dict[str, Any]) -> list[dict[str, Any]]:
    sid, op = step["id"], step["op"]
    base = {"step_id": sid, "operation": op, "state": "completed"}

    def row(**fields: Any) -> dict[str, Any]:
        return {**base, **fields}

    if op == "mkdir":
        _, target = _target(step["path"], allow_missing=True)
        target.mkdir(parents=True, exist_ok=True)
        return [row(path=step["path"], value="directory ready")]
    if op in {"write_text", "replace_text"}:
        _, target = _target(step["path"], allow_missing=True)
        if op == "write_text":
            new_content = step["content"]
        else:
            current = _read(target)
            new_content = current.replace(step["pattern"], step.get("replacement", ""), step.get("count", 1))
        expected = step.get("_resolved_expected_sha256")
        # Check the precondition again immediately before modifying the path.
        actual = _digest_file(target) if target.exists() else None
        if expected != actual:
            raise ValueError("target changed after batch preflight; no write performed")
        payload = new_content.encode("utf-8")
        if len(payload) > MAX_FILE_BYTES:
            raise ValueError(f"new file content exceeds the {MAX_FILE_BYTES}-byte limit")
        _atomic_write(target, payload)
        return [row(path=step["path"], sha256=_digest_bytes(payload), count=new_content.count("\n"), bytes=len(payload))]
    if op == "copy":
        _, src = _existing_file(step["path"])
        _, dst = _target(step["to"], allow_missing=True)
        data = src.read_bytes()
        if _digest_bytes(data) != step.get("_resolved_source_sha256"):
            raise ValueError("copy source changed after batch preflight")
        expected = step.get("_resolved_expected_sha256")
        actual = _digest_file(dst) if dst.exists() else None
        if expected != actual:
            raise ValueError("copy destination changed after batch preflight")
        _atomic_write(dst, data)
        digest = _digest_bytes(data)
        if _digest_file(dst) != digest:
            raise ValueError("copy verification failed")
        return [row(path=step["path"], value=step["to"], sha256=digest, bytes=len(data))]
    if op == "read_text":
        _, target = _existing_file(step["path"])
        lines = _read(target).splitlines()
        start = step.get("start_line", 1)
        limit = _bounded_limit(step.get("limit", 100), "read_text limit", 1000)
        max_chars = _bounded_limit(step.get("max_chars", 12_000), "read_text max_chars", MAX_TEXT_OUTPUT)
        if start < 1:
            raise ValueError("read_text start_line must be at least 1")
        selected = lines[start - 1 : start - 1 + limit]
        rows: list[dict[str, Any]] = []
        used = 0
        for line_number, line in enumerate(selected, start):
            if used >= max_chars:
                break
            value = line[: min(MAX_LINE_CHARS, max_chars - used)]
            rows.append(row(path=step["path"], line=line_number, value=value))
            used += len(value)
        return rows or [row(path=step["path"], value="")]
    if op == "glob":
        pattern = step.get("pattern", "**/*")
        limit = _bounded_limit(step.get("limit", 500), "glob limit", 1000)
        found: list[str] = []
        for root in roots():
            for file in walk(root, skip_common=True):
                rel = file.relative_to(root).as_posix()
                if _matches_glob(rel, pattern):
                    found.append(f"{root.name}/{rel}")
                    if len(found) >= limit:
                        break
            if len(found) >= limit:
                break
        if not found:
            return [row(value="No matching files found")]
        return [row(path=value) for value in found]
    if op == "grep":
        needle = step["pattern"] if step.get("case_sensitive", False) else step["pattern"].casefold()
        glob = step.get("glob", "**/*")
        limit = _bounded_limit(step.get("limit", 500), "grep limit", 1000)
        matches: list[dict[str, Any]] = []
        for root in roots():
            for file in walk(root, skip_common=True):
                rel = file.relative_to(root).as_posix()
                if not _matches_glob(rel, glob) or file.stat().st_size > MAX_FILE_BYTES:
                    continue
                try:
                    lines = file.read_text(encoding="utf-8").splitlines()
                except (OSError, UnicodeError):
                    continue
                for line_number, line in enumerate(lines, 1):
                    haystack = line if step.get("case_sensitive", False) else line.casefold()
                    if needle in haystack:
                        matches.append(row(path=f"{root.name}/{rel}", line=line_number, value=line[:MAX_LINE_CHARS]))
                        if len(matches) >= limit:
                            return matches
        return matches or [row(value="No matches found")]
    if op == "grep_many":
        patterns = step["patterns"]
        needles = [pattern if step.get("case_sensitive", False) else pattern.casefold() for pattern in patterns]
        glob = step.get("glob", "**/*")
        limit = _bounded_limit(step.get("limit", 5), "grep_many per-pattern limit", 200)
        found: list[list[dict[str, Any]]] = [[] for _ in patterns]
        for root in roots():
            for file in walk(root, skip_common=True):
                rel = file.relative_to(root).as_posix()
                if not _matches_glob(rel, glob):
                    continue
                try:
                    if file.stat().st_size > MAX_FILE_BYTES:
                        continue
                    lines = file.read_text(encoding="utf-8").splitlines()
                except (OSError, UnicodeError):
                    continue
                for line_number, line in enumerate(lines, 1):
                    haystack = line if step.get("case_sensitive", False) else line.casefold()
                    for index, needle in enumerate(needles):
                        if len(found[index]) < limit and needle in haystack:
                            found[index].append(row(query=patterns[index], path=f"{root.name}/{rel}", line=line_number, value=line[:MAX_LINE_CHARS]))
                if all(len(matches) >= limit for matches in found):
                    break
            if all(len(matches) >= limit for matches in found):
                break
        for index, matches in enumerate(found):
            if not matches:
                found[index] = [row(query=patterns[index], value="No matches found")]
        # Interleave results so the bounded preview represents each query before
        # spending its remaining rows on the first high-frequency query.
        return [found[index][rank] for rank in range(max(map(len, found))) for index in range(len(found)) if rank < len(found[index])]
    if op in {"count_lines", "sha256", "assert_contains", "exists"}:
        _, target = _target(step["path"], allow_missing=op == "exists")
        if op == "exists":
            if not target.exists():
                raise ValueError("assertion failed: target no longer exists")
            return [row(path=step["path"], value=True)]
        _, target = _existing_file(step["path"])
        if op == "sha256":
            return [row(path=step["path"], sha256=_digest_file(target))]
        content = _read(target)
        if op == "count_lines":
            return [row(path=step["path"], count=content.count("\n"), sha256=_digest_file(target))]
        if step["text"] not in content:
            raise ValueError("assertion failed: text not found")
        return [row(path=step["path"], value=True, sha256=_digest_file(target))]
    if op == "assert_equal":
        _, left = _existing_file(step["path"])
        _, right = _existing_file(step["other"])
        left_hash, right_hash = _digest_file(left), _digest_file(right)
        if left_hash != right_hash:
            raise ValueError("assertion failed: files differ")
        return [row(path=step["path"], other=step["other"], value=True, sha256=left_hash)]
    raise ValueError(f"unsupported operation: {op}")


def run_batch(
    steps: list[dict[str, Any]],
    store: ResultStore,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    normalized = _normalize_steps(steps)
    if idempotency_key is not None and (
        not isinstance(idempotency_key, str) or not _IDEMPOTENCY_KEY.fullmatch(idempotency_key)
    ):
        raise ValueError("idempotency_key must be 1-128 ASCII letters, numbers, '.', '_', ':' or '-'")
    if idempotency_key is None and any(step["op"] in _MUTATING for step in normalized):
        raise ValueError("idempotency_key is required for batches containing file mutations")
    request_fingerprint = {"roots": [str(root) for root in roots()], "steps": normalized}
    request_hash = hashlib.sha256(
        json.dumps(request_fingerprint, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    run_id, created = store.create_run(
        len(normalized), idempotency_key=idempotency_key, request_hash=request_hash if idempotency_key else None
    )
    if not created:
        snapshot = store.run_snapshot(run_id)
        if snapshot is None:
            raise ValueError("idempotent batch reference is no longer available; use a new idempotency_key")
        run = snapshot["run"]
        if run["state"] == "released":
            saved = snapshot["summary"] or {
                "steps_total": run["steps_total"], "done": 0, "failed": 0, "unknown": 0,
                "pending": 0, "details_total": 0, "elapsed_ms": run["elapsed_ms"],
            }
            return {
                **saved,
                "status": "released",
                "details_ref": run_id,
                "preview": [],
                "preview_next_offset": None,
                "deduplicated": True,
                "details_released": True,
            }
        status = "in_progress" if run["state"] == "running" else run["state"]
        return _summary(
            run_id, run["steps_total"], snapshot["rows"], run["elapsed_ms"],
            status_override=status, deduplicated=True,
        )

    started = time.monotonic()
    prepared, prior = _preflight(normalized)

    failed_index = next((i for i, item in enumerate(prepared) if "error" in item), None)
    if failed_index is not None:
        rows: list[dict[str, Any]] = []
        for item in prepared[: failed_index + 1]:
            meta = item["meta"]
            if "error" in item:
                rows.append({**meta, "state": "failed"})
            else:
                rows.append({**meta, "state": "preflight_ok"})
        for step in normalized[len(prepared) :]:
            rows.append({"step_id": step["id"], "operation": step["op"], "state": "pending"})
        store.append_rows(run_id, rows)
        elapsed = int((time.monotonic() - started) * 1000)
        store.update_run(run_id, "preflight_failed", elapsed)
        result = _summary(run_id, len(normalized), rows, elapsed, status_override="preflight_failed")
        if idempotency_key is not None:
            store.save_idempotent_summary(run_id, result)
        return result

    rows = []
    total_output = 0
    stop_index: int | None = None
    for index, item in enumerate(prepared):
        step = item["step"]
        step = {
            **step,
            "_resolved_expected_sha256": item["meta"].get("_resolved_expected_sha256"),
            "_resolved_source_sha256": item["meta"].get("_resolved_source_sha256"),
        }
        store.append_rows(run_id, [{"step_id": step["id"], "operation": step["op"], "state": "running", **item["meta"]}])
        try:
            produced = _op_rows(step)
            if len(produced) > MAX_STEP_ROWS:
                raise ValueError(f"operation produced more than {MAX_STEP_ROWS} rows")
            step_bytes = sum(_json_bytes(row) for row in produced)
            if total_output + step_bytes > MAX_OUTPUT_BYTES:
                raise ValueError(f"batch result exceeds the {MAX_OUTPUT_BYTES}-byte local storage limit")
            total_output += step_bytes
            # Replace the temporary running marker with completed rows atomically.
            store.replace_last_step_rows(run_id, step["id"], produced)
            rows.extend(produced)
        except Exception as exc:
            state = "unknown" if step["op"] in _MUTATING else "failed"
            failure = {"step_id": step["id"], "operation": step["op"], "state": state, "error": str(exc)[:1000]}
            store.replace_last_step_rows(run_id, step["id"], [failure])
            rows.append(failure)
            stop_index = index
            break

    if stop_index is not None:
        for step in normalized[stop_index + 1 :]:
            row = {"step_id": step["id"], "operation": step["op"], "state": "pending"}
            store.append_rows(run_id, [row])
            rows.append(row)

    elapsed = int((time.monotonic() - started) * 1000)
    state = "completed" if stop_index is None else ("interrupted" if rows and any(row.get("state") == "unknown" for row in rows) else "partial_failure")
    store.update_run(run_id, state, elapsed)
    result = _summary(run_id, len(normalized), rows, elapsed, status_override=state)
    if idempotency_key is not None:
        store.save_idempotent_summary(run_id, result)
    return result


def _summary(
    run_id: str,
    steps_total: int,
    rows: list[dict[str, Any]],
    elapsed_ms: int,
    *,
    status_override: str | None = None,
    deduplicated: bool = False,
) -> dict[str, Any]:
    completed_steps = {row["step_id"] for row in rows if row.get("state") == "completed"}
    failed_steps = {row["step_id"] for row in rows if row.get("state") == "failed"}
    unknown_steps = {row["step_id"] for row in rows if row.get("state") == "unknown"}
    pending_steps = max(0, steps_total - len(completed_steps) - len(failed_steps) - len(unknown_steps))
    status = status_override or (
        "completed" if not failed_steps and not pending_steps and not unknown_steps
        else ("preflight_failed" if failed_steps and not completed_steps else "partial_failure")
    )

    compact_search = (
        status == "completed"
        and steps_total == 1
        and bool(rows)
        and all(row.get("operation") == "grep_many" and row.get("state") == "completed" for row in rows)
    )
    if compact_search:
        compact_preview: dict[str, list[list[Any]]] = {}
        preview_rows = 0
        preview_next_offset: int | None = None
        for index, row in enumerate(rows):
            if preview_rows >= MAX_PREVIEW_ROWS:
                preview_next_offset = index
                break
            path = row.get("path")
            truncated = False
            if isinstance(path, str) and len(path) > MAX_PREVIEW_TEXT_CHARS:
                path = path[:MAX_PREVIEW_TEXT_CHARS]
                truncated = True
            query = str(row.get("query", ""))
            is_no_match = path is None
            previous = compact_preview.get(query)
            if previous is None:
                compact_preview[query] = []
            if not is_no_match:
                compact_preview[query].append([path, row.get("line")])
            current_bytes = _json_bytes(compact_preview)
            if current_bytes > MAX_PREVIEW_BYTES:
                if not is_no_match:
                    compact_preview[query].pop()
                if previous is None:
                    compact_preview.pop(query)
                preview_next_offset = index
                break
            preview_rows += 1
            if truncated:
                preview_next_offset = index
                break
        else:
            if preview_rows < len(rows):
                preview_next_offset = preview_rows

        compact_result: dict[str, Any] = {
            "status": status,
            "details_ref": run_id,
            "details_total": len(rows),
            "preview": compact_preview,
        }
        if preview_next_offset is not None:
            compact_result["preview_next_offset"] = preview_next_offset
        if deduplicated:
            compact_result["deduplicated"] = True
        return compact_result

    preview: list[dict[str, Any]] = []
    preview_bytes = 0
    for row in rows[:MAX_PREVIEW_ROWS]:
        item = {key: value for key, value in row.items() if key in {
            "step_id", "operation", "state", "query", "path", "other", "line", "value",
            "error", "count", "sha256", "bytes", "lines",
        }}
        if item.get("state") == "completed":
            item.pop("operation", None)
            item.pop("state", None)
        truncated_fields = []
        for field in ("path", "other", "value", "error"):
            value = item.get(field)
            if isinstance(value, str) and len(value) > MAX_PREVIEW_TEXT_CHARS:
                item[field] = value[:MAX_PREVIEW_TEXT_CHARS]
                truncated_fields.append(field)
        if truncated_fields:
            item["truncated_fields"] = truncated_fields
        item_bytes = _json_bytes(item)
        if preview_bytes + item_bytes > MAX_PREVIEW_BYTES:
            break
        preview.append(item)
        preview_bytes += item_bytes
    first_truncated = next((index for index, row in enumerate(preview) if row.get("truncated_fields")), None)
    preview_next_offset = first_truncated if first_truncated is not None else (len(preview) if len(preview) < len(rows) else None)
    return {
        "status": status,
        "steps_total": steps_total,
        "done": len(completed_steps),
        "failed": len(failed_steps),
        "unknown": len(unknown_steps),
        "pending": pending_steps,
        "details_ref": run_id,
        "details_total": len(rows),
        "preview": preview,
        "preview_next_offset": preview_next_offset,
        "elapsed_ms": elapsed_ms,
        "deduplicated": deduplicated,
    }

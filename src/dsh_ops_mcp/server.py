from __future__ import annotations

import fnmatch
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP

from .batch import MAX_STEPS, run_batch
from .filesystem import MAX_FILE_BYTES, roots as _roots, safe_pattern, safe_target as _safe_target, walk as _walk
from .results import ResultStore

mcp = FastMCP("dsh-ops-mcp")
MAX_READ_CHARS = 60_000
MAX_MATCHES = 200


@lru_cache(maxsize=1)
def _result_store() -> ResultStore:
    return ResultStore()


@mcp.tool()
def ops_inspect_local_file(path: str, start_line: int = 1, max_chars: int = 12000) -> str:
    """Read one bounded UTF-8 file. For several related reads/searches, prefer ops_batch and fetch details with ops_result."""
    if start_line < 1:
        raise ValueError("start_line must be at least 1")
    if not 1 <= max_chars <= MAX_READ_CHARS:
        raise ValueError(f"max_chars must be between 1 and {MAX_READ_CHARS}")
    root, target = _safe_target(path)
    if not target.is_file():
        raise ValueError("path is not a regular file")
    if target.stat().st_size > MAX_FILE_BYTES:
        raise ValueError(f"file exceeds the {MAX_FILE_BYTES}-byte read limit")
    content = target.read_text(encoding="utf-8")
    lines = content.splitlines()
    excerpt = "\n".join(lines[start_line - 1 :])[:max_chars]
    return f"# {target.relative_to(root)} (lines {start_line}-{min(len(lines), start_line + excerpt.count(chr(10)))} of {len(lines)})\n{excerpt}"


@mcp.tool()
def ops_glob(pattern: str = "**/*", limit: int = 100) -> str:
    """List matching relative file paths. For multi-step or large-result work, prefer ops_batch and fetch details with ops_result."""
    if not 1 <= limit <= MAX_MATCHES:
        raise ValueError(f"limit must be between 1 and {MAX_MATCHES}")
    safe_pattern(pattern)
    matches: list[str] = []
    for root in _roots():
        for file in _walk(root):
            rel = file.relative_to(root).as_posix()
            if PurePosixPath(rel).match(pattern) or fnmatch.fnmatchcase(rel, pattern):
                matches.append(f"{root.name}/{rel}")
                if len(matches) >= limit:
                    break
        if len(matches) >= limit:
            break
    return "\n".join(matches) if matches else "No matching files found."


@mcp.tool()
def ops_grep(pattern: str, glob: str = "**/*", limit: int = 50, case_sensitive: bool = False) -> str:
    """Search for a literal string. For batched searches or large results, prefer ops_batch and fetch details with ops_result."""
    if not pattern:
        raise ValueError("pattern cannot be empty")
    if not 1 <= limit <= MAX_MATCHES:
        raise ValueError(f"limit must be between 1 and {MAX_MATCHES}")
    safe_pattern(glob)
    needle = pattern if case_sensitive else pattern.casefold()
    results: list[str] = []
    for root in _roots():
        for file in _walk(root):
            rel = file.relative_to(root).as_posix()
            if not (PurePosixPath(rel).match(glob) or fnmatch.fnmatchcase(rel, glob)):
                continue
            try:
                if file.stat().st_size > MAX_FILE_BYTES:
                    continue
                content = file.read_text(encoding="utf-8")
            except (OSError, UnicodeError):
                continue
            for line_no, line in enumerate(content.splitlines(), 1):
                haystack = line if case_sensitive else line.casefold()
                if needle in haystack:
                    results.append(f"{root.name}/{rel}:{line_no}: {line[:500]}")
                    if len(results) >= limit:
                        return "\n".join(results)
    return "\n".join(results) if results else "No matches found."


@mcp.tool()
def ops_batch(steps: list[dict[str, Any]], idempotency_key: str | None = None) -> dict[str, Any]:
    """Execute 1-512 related, allowlisted file operations in one call. Use grep_many to search 2-20 literal terms in one filesystem pass. Mutating batches require an idempotency_key reused unchanged when retrying. Results stay local with a bounded preview and details_ref."""
    return run_batch(steps, _result_store(), idempotency_key)


@mcp.tool()
def ops_result(
    details_ref: str,
    action: Literal["page", "release"] = "page",
    offset: int = 0,
    limit: int = 50,
    max_bytes: int = 12_000,
    fields: list[str] | None = None,
    step_ids: list[str] | None = None,
    operations: list[str] | None = None,
    states: list[str] | None = None,
    path_prefix: str | None = None,
    queries: list[str] | None = None,
) -> dict[str, Any]:
    """Retrieve batch results by page, fields, step_ids, operations, states, literal path_prefix, or grep_many queries; or release the reference."""
    store = _result_store()
    if action == "release":
        return {"released": store.release(details_ref)}
    return store.page(
        details_ref,
        offset=offset,
        limit=limit,
        max_bytes=max_bytes,
        fields=fields,
        step_ids=step_ids,
        operations=operations,
        states=states,
        path_prefix=path_prefix,
        queries=queries,
    )


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

"""Filesystem access helpers shared by the direct and batched MCP tools."""

from __future__ import annotations

import os
import re
from pathlib import Path, PurePosixPath

MAX_FILE_BYTES = 1_000_000
MAX_WALK_FILES = 20_000
SKIP_DIRS = {".git", ".venv", "node_modules", "__pycache__", "target", ".dsh-ops"}


def roots() -> list[Path]:
    raw = os.environ.get("DSH_OPS_ROOTS", "")
    configured: list[Path] = []
    for item in raw.split(os.pathsep):
        if not item.strip():
            continue
        path = Path(item).expanduser()
        if path.is_symlink():
            continue
        try:
            resolved = path.resolve(strict=True)
        except OSError:
            continue
        if resolved.is_dir():
            configured.append(resolved)
    return configured


def safe_target(relative_path: str, *, allow_missing: bool = False) -> tuple[Path, Path]:
    raw = relative_path.strip()
    if not raw or "\x00" in raw or "\\" in raw or re.match(r"^[A-Za-z]:", raw):
        raise ValueError("path must be a non-empty relative path using forward slashes")
    rel = PurePosixPath(raw)
    if not rel.parts or rel.is_absolute() or ".." in rel.parts:
        raise ValueError("absolute paths and parent traversal are not allowed")

    configured = roots()
    if not configured:
        raise ValueError("no valid DSH_OPS_ROOTS directories are configured")

    for root in configured:
        current = root
        crossed_link = False
        for part in rel.parts:
            current = current / part
            if current.is_symlink():
                crossed_link = True
                break
        if crossed_link:
            continue
        try:
            resolved = current.resolve(strict=not allow_missing)
            resolved.relative_to(root)
        except (OSError, ValueError):
            continue
        return root, resolved
    state = "missing, outside configured roots, or crosses a symlink"
    if allow_missing:
        state = "outside configured roots or crosses a symlink"
    raise ValueError(f"path is {state}")


def safe_pattern(pattern: str) -> str:
    if not pattern or "\x00" in pattern or "\\" in pattern:
        raise ValueError("pattern must be non-empty and use forward slashes")
    if PurePosixPath(pattern).is_absolute() or ".." in PurePosixPath(pattern).parts:
        raise ValueError("pattern must be relative and cannot contain '..'")
    return pattern


def walk(root: Path, *, skip_common: bool = False):
    visited = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        base_path = Path(base)
        dirs[:] = sorted(
            name
            for name in dirs
            if (not skip_common or (name not in SKIP_DIRS and not name.startswith(".")))
            and name != ".git"
            and not (base_path / name).is_symlink()
        )
        for name in sorted(files):
            path = base_path / name
            if path.is_symlink() or not path.is_file():
                continue
            visited += 1
            if visited > MAX_WALK_FILES:
                return
            yield path

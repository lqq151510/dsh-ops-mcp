from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


class ServerSurfaceTests(unittest.TestCase):
    def test_default_surface_is_batched_and_legacy_tools_are_opt_in(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="dsh-ops-surface-") as temp:
            base = Path(temp)
            roots = base / "workspace"
            roots.mkdir()

            async def list_tools(legacy: bool) -> list[dict[str, object]]:
                env = os.environ.copy()
                env["DSH_OPS_ROOTS"] = str(roots)
                env["DSH_OPS_STATE_DIR"] = str(base / ("state-legacy" if legacy else "state-default"))
                env.pop("DSH_OPS_ENABLE_LEGACY_TOOLS", None)
                if legacy:
                    env["DSH_OPS_ENABLE_LEGACY_TOOLS"] = "1"
                params = StdioServerParameters(
                    command=sys.executable,
                    args=["-m", "dsh_ops_mcp.server"],
                    env=env,
                    cwd=str(repo),
                )
                async with stdio_client(params) as (read_stream, write_stream):
                    async with ClientSession(read_stream, write_stream) as session:
                        await session.initialize()
                        response = await session.list_tools()
                        return [tool.model_dump(exclude_none=True) for tool in response.tools]

            default_tools = asyncio.run(list_tools(legacy=False))
            legacy_tools = asyncio.run(list_tools(legacy=True))
            default_names = {str(tool["name"]) for tool in default_tools}
            legacy_names = {str(tool["name"]) for tool in legacy_tools}
            self.assertEqual(default_names, {"ops_batch", "ops_result"})
            self.assertEqual(
                legacy_names,
                default_names | {"ops_inspect_local_file", "ops_glob", "ops_grep"},
            )
            default_bytes = len(json.dumps(default_tools, ensure_ascii=False, separators=(",", ":")).encode())
            legacy_bytes = len(json.dumps(legacy_tools, ensure_ascii=False, separators=(",", ":")).encode())
            self.assertLess(default_bytes, legacy_bytes)


if __name__ == "__main__":
    unittest.main()

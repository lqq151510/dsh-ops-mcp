# dsh-ops-mcp

跨平台本地 stdio MCP 服务，默认只向模型公开 `ops_batch` 和 `ops_result` 两个工具。多项明确的文件操作合并在一次 MCP 调用里，完整结果留在本机；模型先拿紧凑预览，需要时再分页读取。这借鉴了视频中 PTC 的“减少模型往返、压缩结果回传”思路，但不是上游 dsh-ops 的完整移植，也不等同于宿主内置的 `run_code` PTC。

## 安装

```sh
git clone https://github.com/lqq151510/dsh-ops-mcp.git
cd dsh-ops-mcp
uv sync
```

启动前通过 `DSH_OPS_ROOTS` 显式配置允许访问的项目目录。macOS/Linux 使用冒号分隔，Windows 使用分号分隔。服务只允许在这些根目录内读写、搜索、复制和校验文件。例如：

```sh
export DSH_OPS_ROOTS="/path/to/your/project"
uv run dsh-ops-mcp
```

默认不注册旧的 `ops_inspect_local_file`、`ops_glob`、`ops_grep` 单项工具，以减少工具定义并让模型优先使用批处理。迁移期间需要兼容旧客户端时，可设置 `DSH_OPS_ENABLE_LEGACY_TOOLS=1` 后重启 MCP 服务；启用后工具清单会增加这 3 项。

保护：拒绝绝对路径、`..` 和符号链接；单文件操作上限 1 MB；批次最多 512 步、输入和完整结果各不超过 8 MB；批次会先整体预检，写入使用临时文件原子替换；覆盖文件必须提供预期 SHA-256，避免覆盖已变化的文件。`mkdir`、`write_text`、`replace_text` 和 `copy` 等有副作用的批次必须带 `idempotency_key`。同一逻辑请求重试时复用同一个键：相同键和相同步骤只返回原执行结果，键相同但步骤或允许目录不同会被拒绝。键与精简状态保留 24 小时；释放明细后仍保留键，避免旧请求重放文件写入。执行中断且副作用结果不确定时会记录为 `unknown`，不会自动盲目重试；检查文件状态后，若确实要重新执行，应使用新的键。

批量结果默认保存在本机应用状态目录；超过 24 小时后会在下一次 MCP 调用时清理。结果文件夹仅当前系统用户可读；读文件内容可能包含项目敏感信息。可以通过 `DSH_OPS_STATE_DIR` 改变存储目录。复制操作的累计输入上限为 32 MB。

## MCP 工具

- `ops_batch(steps, idempotency_key?)`：单次调用串行预检并执行多项允许的文件操作。要搜多个字面词时，用一个 `grep_many` 步骤（2–20 个词）一次遍历目录，默认每词最多返回 5 条。有副作用的批次必须传幂等键；普通预览最多 5 条。单步 `grep_many` 使用紧凑映射：`preview[查询词]` 保存 `[路径, 行号]` 对，先返回定位信息；命中行文本仍留在本机结果库，只有需要时再用 `ops_result` 取回。
- `ops_result(details_ref, action?, offset?, limit?, max_bytes?, fields?, step_ids?, operations?, states?, path_prefix?, queries?)`：按页查看完整结果，可按步骤、操作类型、状态、路径前缀或 `grep_many` 搜索词过滤。路径和查询词从 SQLite 索引列筛选，不必先把整批 JSON 明细加载到内存；也可以释放本地结果。

旧单项工具只在 `DSH_OPS_ENABLE_LEGACY_TOOLS=1` 时额外注册；对应的读取、列举和单词搜索能力仍可作为 `read_text`、`glob`、`grep` 批次步骤使用。

批次操作类型：`mkdir`、`write_text`、`replace_text`、`copy`、`read_text`、`glob`、`grep`、`grep_many`、`count_lines`、`sha256`、`assert_contains`、`assert_equal`、`exists`。`grep_many` 的每条结果带 `query`，后续可以用 `ops_result(queries=[...])` 只取指定词的命中。每步可以指定 `id`，后续写入可用 `$step_id.sha256` 引用之前读取或计算出的哈希。`count_lines` 与 `wc -l` 一致，统计换行符数量。

示例：

```json
{
  "idempotency_key": "update-readme-20261015-1",
  "steps": [
    {"id": "before", "op": "read_text", "path": "README.md"},
    {"op": "replace_text", "path": "README.md", "pattern": "旧标题", "replacement": "新标题", "expected_sha256": "$before.sha256"},
    {"op": "assert_contains", "path": "README.md", "text": "新标题"},
    {"id": "files", "op": "glob", "pattern": "**/*.py", "limit": 200}
  ]
}
```

幂等键由调用方为一次逻辑写入生成，并在该次调用的网络重试中保持不变；有意发起一轮新的写入时生成新键。纯只读批次不要求幂等键。若同键调用仍在执行，服务返回 `in_progress` 和原 `details_ref`，可稍后用 `ops_result` 查看；若结果已释放，重试会返回 `released`，不会再次写文件。

普通批次回执包含 `details_ref`、步骤计数、结果总数和有界预览；单步 `grep_many` 使用更紧凑的回执，只包含状态、结果引用、行数和查询词到路径/行号的映射。若 `preview_next_offset` 存在且不为 `null`，可把它作为 `ops_result` 的 `offset`，继续读取完整明细。小结果有机会直接从 `ops_batch` 回执获得所需信息，从而省掉一次结果查询；大结果仍需分页，实际调用节省取决于任务及模型是否需要命中内容。

```json
{
  "details_ref": "<batch reference>",
  "offset": 0,
  "step_ids": ["matches"],
  "operations": ["grep_many"],
  "path_prefix": "src/",
  "queries": ["TODO"],
  "limit": 20
}
```

过滤后的 `total` 表示匹配数，`total_all` 表示批次全部结果数。运行测试、任意 shell 命令和动态执行模型生成的代码仍由客户端宿主处理，当前 MCP 不会代替它们。是否减少模型 token 取决于返回内容和客户端上下文；不能仅凭 MCP 调用减少推断 token 一定下降。基准文档记录历史读取对照和多词检索对照，并区分工具调用、响应体大小、模型轮次及模型 token。

基准文档 [`docs/call-reduction-benchmark.md`](docs/call-reduction-benchmark.md) 记录了对照方法、实测结果和指标口径；单一任务不能外推为通用收益。

将服务配置到 MCP 客户端时，把命令指向本地安装环境中的 `dsh-ops-mcp` 可执行文件，并通过 `DSH_OPS_ROOTS` 设置该客户端允许访问的项目目录。客户端的配置格式可能不同，请按对应客户端的 MCP 文档填写。

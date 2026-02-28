# memU Plugin for OpenClaw

> **致谢与声明**
> 本插件是在 [memU Engine for OpenClaw](https://github.com/duxiaoxiong/memu-engine-for-OpenClaw) 基础上的windows移植与部分优化版本，请关注原作者。
> 本插件在其基础上做了以下改进：
> - **Windows 原生支持**（锁文件、路径、信号处理均适配 Windows）
> - 基于 `uv` 的 Python 环境管理，无需手动 virtualenv
> - 多语言身份感知记忆提取（中文优化）
> - 文档内容变更检测 + 删除同步
> - 会话 part 精确时间戳（毫秒级 sidecar）
> - Web 管理面板（中文暗色主题）
> - 修复了部分bug并增加了部分bug

OpenClaw 长期记忆插件，基于 memU SDK 实现语义记忆存储与检索。对话内容、工作区文档自动摄入，跨会话持久保存。

## 目录

- [前置依赖](#前置依赖)
- [安装](#安装)
- [配置选项](#配置选项)
- [工具使用](#工具使用)
- [管理面板](#管理面板)
- [发布与分发](#发布与分发)
- [环境变量](#环境变量)
- [开发](#开发)
- [故障排除](#故障排除)

---

## 前置依赖

### 1. uv（Python 包管理器）—— **必须**

本插件所有 Python 脚本通过 `uv run` 执行，**不依赖系统 Python**，但需要安装 `uv`。

**Windows（PowerShell）：**

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**macOS / Linux：**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

安装后验证（需重新打开终端）：

```powershell
uv --version
```

> uv 会在首次运行时自动创建虚拟环境并安装所有依赖，无需手动执行 `pip install`。

### 2. OpenClaw

```bash
npm install -g openclaw
openclaw --version
```

### 3. API 密钥

至少需要：
- **嵌入模型 API**（用于向量搜索，如 `BAAI/bge-m3`、`text-embedding-3-small`）
- **聊天模型 API**（用于记忆提取，推荐 `qwen-max` 或 `gpt-4o`）

两者可使用同一服务商，也可分开配置。支持所有 OpenAI 兼容接口（DashScope、SiliconFlow 等）。

---

## 安装

### 方式一：本地链接（开发 / 本地使用，推荐）

```powershell
git clone https://github.com/your-org/memu-plugin.git
cd memu-plugin
openclaw plugins install -l .
```

### 2. 配置 OpenClaw

编辑 `~/.openclaw/openclaw.json`：

```json
{
  "plugins": {
    "slots": {
      "memory": "memu-plugin"
    },
    "entries": {
      "memu-plugin": {
        "enabled": true,
        "config": {
          "embedding": {
            "provider": "openai",
            "baseUrl": "https://api.siliconflow.cn/v1",
            "apiKey": "sk-xxx",
            "model": "BAAI/bge-m3"
          },
          "extraction": {
            "provider": "openai",
            "baseUrl": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "apiKey": "sk-xxx",
            "model": "qwen-max"
          },
          "language": "zh",
          "userName": "小明",
          "assistantName": "Claw",
          "dataDir": "~/.openclaw/memUdata",
          "ingest": {
            "includeDefaultPaths": true,
            "extraPaths": []
          },
          "retrieval": {
            "mode": "fast",
            "defaultMaxResults": 10,
            "outputMode": "compact"
          }
        }
      }
    }
  }
}
```

### 3. 重启 OpenClaw

```bash
openclaw gateway restart
```

## 配置选项

### embedding（嵌入模型）

用于向量搜索的嵌入模型配置。

| 字段 | 默认值 | 说明 |
|------|--------|------|
| provider | openai | 服务商（支持 openai 兼容接口） |
| baseUrl | https://api.openai.com/v1 | API 地址 |
| apiKey | — | API 密钥 |
| model | text-embedding-3-small | 模型名称 |

### extraction (提取模型)

用于从对话和文档中提取记忆条目的 LLM。

| 字段 | 默认值 | 说明 |
|------|--------|------|
| provider | openai | 服务商 |
| baseUrl | https://api.openai.com/v1 | API 地址 |
| apiKey | — | API 密钥 |
| model | gpt-4o-mini | 模型名称（推荐 qwen-max 或 gpt-4o） |

### language（输出语言）

记忆摘要的写入语言。

| 值 | 说明 |
|----|------|
| `auto` | 跟随输入语言（默认） |
| `zh` | 强制中文 |
| `en` | 强制英文 |
| `ja` | 强制日文 |

### dataDir (数据目录)

memU 数据库和文件存储位置。

- 默认：`~/.openclaw/memUdata`
- 包含：`memu.db`, `conversations/`, `resources/`

### identity (身份标识)

配置 AI 助手和人类用户的名称，用于记忆提取时正确归属主语。未配置时，描述 AI 行为规则的文档（`AGENTS.md`、`SOUL.md` 等）内容可能被错误归属给用户。

| 字段 | 说明 |
|------|------|
| `userName` | 人类用户的称呼（如 `"小明"`） |
| `assistantName` | AI 助手的称呼（如 `"Claw"`） |

**建议始终配置此项。**

### dataDir（数据目录）

memU 数据库和文件存储位置，默认 `~/.openclaw/memUdata`。

包含：
- `memu.db`：SQLite 记忆数据库
- `conversations/`：处理后的会话 part 文件及时间戳 sidecar（`.session.meta.json`）
- `resources/`：文档 blob 缓存

### ingest（文档摄入）

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `includeDefaultPaths` | `true` | 包含工作区默认文档 |
| `extraPaths` | `[]` | 额外需要摄入的路径（支持 glob） |

**默认摄入路径：**
`AGENTS.md`、`SOUL.md`、`TOOLS.md`、`MEMORY.md`、`HEARTBEAT.md`、`BOOTSTRAP.md`、`memory/*.md`

> `skills/` 目录被硬排除，永远不会被摄入。OpenClaw 有原生能力列表支持，由平台自行管理。

**内容变更检测：** 采用 SHA-256 哈希追踪，仅重新摄入内容发生变化的文件。文件被删除时，数据库中对应的资源和记忆条目会被级联清理。

**文档类型路由：**

| 文件 | 摄入类型 |
|------|----------|
| `SOUL.md`、`AGENTS.md`、`HEARTBEAT.md` 等身份类文档 | profile + event + knowledge + behavior |
| `TOOLS.md` | knowledge |
| 其他 `.md` | knowledge + event + behavior |

### retrieval（检索行为）

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `mode` | `fast` | `fast`（纯向量）或 `full`（向量 + LLM 复核） |
| `defaultMaxResults` | `10` | 默认最大结果数 |
| `outputMode` | `compact` | `compact`（精简）或 `full`（完整 JSON） |

### dashboard（管理面板）

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `dashboard` | `true` | 是否随 gateway 自动启动面板 |
| `dashboardPort` | `8377` | 面板端口 |

---

## 工具使用

### memory_search

语义搜索记忆。

```json
{
  "name": "memory_search",
  "arguments": {
    "query": "上次讨论的项目方向",
    "maxResults": 5,
    "minScore": 0.5
  }
}
```

### memory_get

获取特定记忆内容。

```json
{
  "name": "memory_get",
  "arguments": {
    "path": "memu://category/events",
    "from": 1,
    "lines": 100
  }
}
```

### memory_flush

强制将当前会话暂存内容立即写入数据库（跳过空闲等待）。

```json
{
  "name": "memory_flush",
  "arguments": {}
}
```

---

## 管理面板

随 gateway 自动启动，默认访问地址：`http://127.0.0.1:8377`

| 页面 | 功能 |
|------|------|
| 概览 | 资源总数、记忆条目数、分类数统计；按类型分布；最近记忆 |
| 记忆 | 浏览全部记忆，按类型/时间筛选，逐条删除 |
| 资源 | 浏览已摄入文档和会话，支持级联删除（资源 + 关联记忆） |
| 分类 | 查看所有记忆分类及关联数量 |
| 搜索 | 关键词搜索，类型过滤，结果高亮 |
| 文件追踪 | 文档摄入状态（哈希、路径、导入时间） |

禁用自动启动：

```json
{ "config": { "dashboard": false } }
```

手动启动：

```powershell
cd python
uv run scripts/dashboard.py
```

---

## 发布与分发

### 官方限制

OpenClaw 插件安装**只支持以下来源**：

| 来源 | 命令 | 支持 |
|------|------|------|
| npm 包名 | `openclaw plugins install @scope/pkg` | 支持 |
| 本地目录 | `openclaw plugins install -l ./my-plugin` | 支持 |
| 本地 tgz/zip | `openclaw plugins install ./plugin.tgz` | 支持 |
| GitHub spec | `openclaw plugins install github:org/repo` | 不支持 |
| Git URL | `openclaw plugins install git+https://...` | 不支持 |

> `github:` 协议会报错 `unsupported npm spec: protocol specs are not allowed`，这是 OpenClaw 的硬性限制。

### 从 GitHub 分发给用户

**方式一：GitHub Releases 附 tgz**

```powershell
npm pack
# 将生成的 memu-plugin-x.x.x.tgz 上传到 GitHub Release
```

用户下载后：

```powershell
openclaw plugins install ./memu-plugin-1.0.0.tgz
```

**方式二：发布到 npm**

```powershell
npm publish --access public
```

用户安装：

```bash
openclaw plugins install @your-scope/memu-plugin
```

### npm 发布准备

确认 `package.json` 包含以下字段：

```json
{
  "openclaw": {
    "extensions": ["./index.ts"]
  },
  "files": [
    "index.ts",
    "openclaw.plugin.json",
    "tsconfig.json",
    "python/pyproject.toml",
    "python/scripts/"
  ]
}
```

打包前验证内容：

```powershell
npm pack --dry-run
```

---

## 环境变量

| 变量 | 说明 |
|------|------|
| `MEMU_DATA_DIR` | 数据目录（覆盖 config 中的 `dataDir`） |
| `MEMU_DASHBOARD_PORT` | 面板端口（覆盖 `dashboardPort`） |
| `MEMU_USER_ID` | 用户标识 |
| `MEMU_USER_NAME` | 人类用户的称呼 |
| `MEMU_ASSISTANT_NAME` | AI 助手的称呼 |
| `MEMU_OUTPUT_LANG` | 输出语言（zh/en/ja 等） |
| `MEMU_DOC_SUBJECT_MAP` | 自定义文档主语映射（JSON 格式） |
| `MEMU_EMBED_API_KEY` | 嵌入 API 密钥 |
| `MEMU_CHAT_API_KEY` | 聊天 API 密钥 |
| `OPENCLAW_SESSIONS_DIR` | 会话日志目录 |
| `OPENCLAW_WORKSPACE_DIR` | 工作区目录 |
| `MEMU_FORCE_FLUSH` | 设为 `1` 强制立即提交暂存 tail |
| `MEMU_FLUSH_IDLE_SECONDS` | 覆盖空闲等待时间（秒） |

---

## 开发

### 项目结构

```
memu-plugin/
  index.ts                 # TypeScript 插件入口
  openclaw.plugin.json     # 插件清单
  package.json
  tsconfig.json
  python/
    pyproject.toml         # Python 依赖（uv 管理）
    scripts/
      search.py            # memory_search 工具
      get.py               # memory_get 工具
      flush.py             # 会话同步（含时间戳注入）
      docs_ingest.py       # 文档摄入（含变更检测）
      convert_sessions.py  # JSONL 会话转换（含 sidecar 时间戳）
      watch_sync.py        # 文件监控守护进程
      dashboard.py         # Web 管理面板（FastAPI + Uvicorn）
  docs/
    DEVELOPMENT_PLAN.md
    SPECIFICATIONS.md
    CODING_STANDARDS.md
```

### 本地调试

```powershell
cd python
$env:MEMU_DATA_DIR = "$env:USERPROFILE\.openclaw\memUdata"
$env:OPENCLAW_SESSIONS_DIR = "$env:USERPROFILE\.openclaw\agents\main\sessions"
$env:OPENCLAW_WORKSPACE_DIR = "E:\openclaw\workspace"
uv run scripts/docs_ingest.py
```

### 主要 Python 依赖

| 包 | 用途 |
|----|------|
| `memu-py >= 1.4.0` | memU SDK 核心 |
| `watchdog >= 4.0.0` | 文件系统监控 |
| `fastapi + uvicorn` | 管理面板 HTTP 服务 |
| `openai >= 1.0.0` | API 客户端 |

### 编码规范

- 代码中禁止使用 emoji
- 使用文本标记：[INFO], [SUCCESS], [ERROR], [WARNING]
- 所有文件 UTF-8 编码，LF 换行符

---

## 故障排除

### uv 命令未找到

安装 uv 后需要重新打开终端或重启 gateway，使 PATH 生效。

```powershell
uv --version
# 如果未找到，检查 PATH 是否包含 uv 安装目录
# Windows 默认：C:\Users\<用户名>\.local\bin\uv.exe
```

### 插件在 `plugins install` 时意外启动

此问题已在本版本修复。插件只在 gateway 交互模式下自动启动，`plugins install`、`plugins list`、`--version` 等 CLI 子命令不会触发后台服务。

### memory_search 返回空结果

1. 运行 `memory_flush` 确认数据已同步
2. 检查 embedding API 密钥和 baseUrl 是否正确
3. 打开管理面板 `http://127.0.0.1:8377` 查看资源和记忆条目数量
4. 确认工作区文档存在（默认路径：`~/workspace/AGENTS.md` 等）

### skills/ 目录被摄入

此问题已在本版本修复（三层硬排除）。如历史数据库中已有 skills 资源，可在管理面板"资源"页面手动删除，或清空全部后重新摄入。

### Python 依赖安装失败

```powershell
cd python
uv sync
```

### 数据库损坏

```powershell
# 备份并删除数据库，重启自动重建
mv ~/.openclaw/memUdata/memu.db ~/.openclaw/memUdata/memu.db.bak
openclaw gateway restart
```

### 端口冲突（8377）

```powershell
# 查找占用端口的进程
Get-NetTCPConnection -LocalPort 8377 | Select-Object OwningProcess
# 终止进程，或在配置中修改 dashboardPort
```

---

## 许可证

MIT License

本插件依赖 [memu-py](https://pypi.org/project/memu-py/)（NevaMind-AI 团队开发）。原始 SDK 版权归 NevaMind-AI 所有。

## 参考链接

- [OpenClaw 文档](https://openclaw.dev/docs)
- [memU 原始仓库](https://github.com/NevaMind-AI/memU)
- [memu-py on PyPI](https://pypi.org/project/memu-py/)
- [uv 安装文档](https://docs.astral.sh/uv/getting-started/installation/)

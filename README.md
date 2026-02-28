# memU Plugin for OpenClaw

OpenClaw 记忆插件，基于 memU SDK 实现长期记忆存储和语义搜索。

## 功能特性

- **memory_search**: 语义搜索所有存储的记忆
- **memory_get**: 获取特定记忆内容
- **memory_flush**: 强制同步会话日志到数据库

## 安装

### 1. 本地安装（开发模式）

```powershell
# 克隆或复制插件到扩展目录
cd E:\openclaw\workspace\projects\memu-plugin
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

### embedding (嵌入模型)

用于向量搜索的嵌入模型配置。

| 字段 | 默认值 | 说明 |
|------|--------|------|
| provider | openai | 提供商 |
| baseUrl | https://api.openai.com/v1 | API 基础 URL |
| apiKey | - | API 密钥 |
| model | text-embedding-3-small | 模型名称 |

### extraction (提取模型)

用于记忆提取的 LLM 配置。

| 字段 | 默认值 | 说明 |
|------|--------|------|
| provider | openai | 提供商 |
| baseUrl | https://api.openai.com/v1 | API 基础 URL |
| apiKey | - | API 密钥 |
| model | gpt-4o-mini | 模型名称 |

### language (输出语言)

记忆摘要的输出语言。

- `auto`: 跟随输入语言
- `zh`: 中文
- `en`: 英文
- `ja`: 日文
- 或其他语言名称

### dataDir (数据目录)

memU 数据库和文件存储位置。

- 默认：`~/.openclaw/memUdata`
- 包含：`memu.db`, `conversations/`, `resources/`

### identity (身份标识)

配置 AI 助手和人类用户的名称，用于记忆提取时正确归属主语。

| 字段 | 默认值 | 说明 |
|------|--------|------|
| userName | `""` | 人类用户的称呼 |
| assistantName | `""` | AI 助手的称呼 |

配置示例：

```json
{
  "config": {
    "userName": "小明",
    "assistantName": "Claw"
  }
}
```

**为什么需要配置身份？**

默认情况下，memU SDK 在提取记忆时会将所有内容归属为"用户"。当文档描述的是 AI 助手的行为规则（如 `AGENTS.md`、`SOUL.md`）时，这会导致错误的归属。配置身份后，系统会自动注入身份感知提示词，让 LLM 正确区分主语。

### ingest (文档摄入)

配置要摄入的 Markdown 文档。

| 字段 | 默认值 | 说明 |
|------|--------|------|
| includeDefaultPaths | true | 包含默认工作区文档 |
| extraPaths | [] | 额外的文档路径 |

默认路径包括：
- `AGENTS.md`
- `SOUL.md`
- `TOOLS.md`
- `MEMORY.md`
- `HEARTBEAT.md`
- `BOOTSTRAP.md`
- `memory/` 目录

#### 内容变更检测

文档摄入采用 SHA-256 哈希追踪机制：

- **首次摄入**：计算文件哈希并存入状态文件
- **后续扫描**：比对哈希，仅重新摄入内容发生变更的文件
- **删除同步**：文件被删除时，自动级联清理数据库中对应的资源和记忆条目

状态文件位置：`{dataDir}/docs_ingest_state.json`

#### 文档主语映射

系统内置文件名到主语的默认映射：

| 文件 | 主语 | 说明 |
|------|------|------|
| `agents.md`, `soul.md`, `tools.md`, `heartbeat.md`, `bootstrap.md`, `identity.md` | AI 助手 | 描述 AI 行为规则的文档 |
| `user.md` | 人类用户 | 描述人类用户特征的文档 |
| 其他文件 | 自动推断 | LLM 根据内容上下文判断 |

可通过 `MEMU_DOC_SUBJECT_MAP` 环境变量自定义映射（JSON 格式）：

```json
{"my-rules.md": "assistant", "profile.md": "user"}
```

### retrieval (检索行为)

| 字段 | 默认值 | 说明 |
|------|--------|------|
| mode | fast | fast 或 full |
| defaultMaxResults | 10 | 默认最大结果数 |
| outputMode | compact | compact 或 full |

### dashboard (管理面板)

| 字段 | 默认值 | 说明 |
|------|--------|------|
| dashboard | true | 是否随插件自动启动管理面板（设为 `false` 禁用） |
| dashboardPort | 8377 | 管理面板端口 |

## 工具使用

### memory_search

语义搜索记忆。

```json
{
  "name": "memory_search",
  "arguments": {
    "query": "上次讨论的项目",
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

强制同步会话日志。

```json
{
  "name": "memory_flush",
  "arguments": {}
}
```

## 可视化管理面板

插件内置一个中文 Web 管理面板（暗色主题），用于查看、搜索和管理 memU 数据库中的所有数据。

### 启动

控制台随插件自动启动，无需手动操作。当 OpenClaw Gateway 启动时，控制台会作为后台服务自动运行。

如需禁用自动启动，可在插件配置中设置：

```json
{
  "config": {
    "dashboard": false
  }
}
```

也可手动单独启动：

```powershell
cd python
uv run scripts/dashboard.py
```

面板默认运行在 `http://127.0.0.1:8377`，可通过环境变量 `MEMU_DASHBOARD_PORT` 或配置项 `dashboardPort` 自定义端口。

### 页面说明

| 页面 | 导航标签 | 功能 |
|------|----------|------|
| 概览 | 概览 | 资源总数、记忆条目总数、分类数量、关联数量统计卡片；按类型分布统计；最近记忆列表 |
| 记忆 | 记忆 | 浏览全部记忆条目，支持按类型筛选、按时间排序（最新/最早/最近更新），分页浏览，逐条删除 |
| 资源 | 资源 | 浏览所有已摄入的文档和会话资源，显示关联条目数，支持级联删除（删除资源及其全部记忆条目） |
| 资源详情 | — | 查看单个资源的元信息（ID、模态、创建时间、关联条目数），浏览并逐条管理关联的记忆条目 |
| 分类 | 分类 | 查看所有记忆分类的名称、描述和关联条目数 |
| 搜索 | 搜索 | 按关键词搜索记忆内容（支持类型过滤），搜索结果高亮显示，可直接从结果中删除条目 |
| 文件追踪 | 文件追踪 | 查看文档摄入的哈希追踪状态（文件路径、SHA-256、资源 ID、导入时间），以及原始状态 JSON |

### 主要操作

- **逐条删除**：在记忆列表、搜索结果、资源详情页面，每条记忆都有独立的"删除"按钮
- **级联删除**：在资源列表和资源详情页面，可一键删除整个资源及其关联的所有记忆条目和分类链接
- **确认弹窗**：级联删除操作会弹出确认对话框，防止误删
- **即时反馈**：删除操作完成后显示绿色/红色提示条，无需手动刷新

## 环境变量

| 变量 | 说明 |
|------|------|
| MEMU_DATA_DIR | 数据目录 |
| MEMU_DASHBOARD_PORT | 管理面板端口（默认 8377） |
| MEMU_USER_ID | 用户标识 |
| MEMU_USER_NAME | 人类用户的称呼（用于记忆主语归属） |
| MEMU_ASSISTANT_NAME | AI 助手的称呼（用于记忆主语归属） |
| MEMU_DOC_SUBJECT_MAP | 自定义文档主语映射（JSON 格式） |
| MEMU_EMBED_API_KEY | 嵌入 API 密钥 |
| MEMU_CHAT_API_KEY | 聊天 API 密钥 |
| MEMU_OUTPUT_LANG | 记忆输出语言（zh/en/ja 等） |
| OPENCLAW_SESSIONS_DIR | 会话日志目录 |
| OPENCLAW_WORKSPACE_DIR | 工作区目录 |

## 开发

### 项目结构

```
memu-plugin/
  index.ts                 # TypeScript 入口
  openclaw.plugin.json     # 插件清单
  package.json             # npm 配置
  tsconfig.json            # TypeScript 配置
  python/
    pyproject.toml         # Python 项目配置
    scripts/
      search.py            # 搜索脚本
      get.py               # 获取脚本
      flush.py             # 会话同步脚本
      docs_ingest.py       # 文档摄入脚本
      convert_sessions.py  # 会话转换脚本
      watch_sync.py        # 文件监控守护进程
      dashboard.py         # Web 管理面板
  docs/
    DEVELOPMENT_PLAN.md    # 开发计划
    SPECIFICATIONS.md      # 技术规格
    CODING_STANDARDS.md    # 编码规范
  README.md                # 本文档
```

### 编码规范

- 代码中禁止使用 emoji
- 使用文本标记：[INFO], [SUCCESS], [ERROR], [WARNING]
- 所有文件 UTF-8 编码
- 使用 LF 换行符

### 依赖

**TypeScript:**
- @types/node
- typescript

**Python:**
- memu-py>=1.4.0
- watchdog>=4.0.0
- sqlmodel>=0.0.14
- openai>=1.0.0
- httpx>=0.28.0
- pydantic>=2.0.0
- fastapi>=0.115.0
- uvicorn[standard]>=0.34.0

## 故障排除

### 插件无法加载

1. 检查 `openclaw.plugin.json` 格式
2. 确认插件已启用
3. 查看 Gateway 日志

### memory_search 返回空结果

1. 确认 embedding 配置正确
2. 检查 API 密钥是否有效
3. 运行 `memory_flush` 同步数据
4. 确认工作区文档存在

### 数据库错误

1. 检查 `dataDir` 路径权限
2. 删除 `memu.db` 重建数据库
3. 确认 SQLite 支持

## 许可证

MIT License

## 参考

- [OpenClaw 文档](https://docs.openclaw.ai)
- [memU 上游仓库](https://github.com/NevaMind-AI/memU)
- [memu-py PyPI](https://pypi.org/project/memu-py/)

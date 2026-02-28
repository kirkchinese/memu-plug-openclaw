#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Memory flush script - sync session logs to memU database.

Features:
- Pending queue: resume after failures
- Rate limit backoff: exponential backoff on 429 errors
- Idle flush: only trigger when session is idle
- Deduplication: skip already-ingested resources
- Comprehensive logging
"""

import asyncio
import sys
import io

# Force UTF-8 encoding for Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
import json
import os
import sqlite3
import tempfile
import time
from datetime import datetime

# Ensure scripts directory is on path for convert_sessions import
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)

# Set required env vars before importing
def _init_env():
    if not os.getenv("MEMU_DATA_DIR"):
        os.environ["MEMU_DATA_DIR"] = os.path.expanduser("~/.openclaw/memUdata")
    if not os.getenv("OPENCLAW_SESSIONS_DIR"):
        home = os.path.expanduser("~")
        default = os.path.join(home, ".openclaw", "agents", "main", "sessions")
        os.environ["OPENCLAW_SESSIONS_DIR"] = default

_init_env()

from memu.app.service import MemoryService
from memu.app.settings import (
    CustomPrompt,
    DatabaseConfig,
    LLMConfig,
    MemorizeConfig,
    MetadataStoreConfig,
    PromptBlock,
)

import convert_sessions

# --- Lock Management ---

FLUSH_LOCK = os.path.join(tempfile.gettempdir(), "memu_sync.lock_auto_sync")


def _pid_alive(pid: int) -> bool:
    if pid <= 1:
        return False
    # Windows: use tasklist for reliable PID detection
    if sys.platform == "win32":
        try:
            import subprocess
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True, timeout=5,
            )
            return str(pid) in result.stdout
        except Exception:
            return False
    # Unix: use os.kill(pid, 0)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def try_acquire_lock(lock_path: str) -> int | None:
    """Try to acquire a lock. Returns file descriptor or None."""
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(lock_path, flags)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        return fd
    except FileExistsError:
        try:
            with open(lock_path, "r", encoding="utf-8") as f:
                pid_str = f.read().strip()
            pid = int(pid_str)
            if not _pid_alive(pid):
                try:
                    os.remove(lock_path)
                except FileNotFoundError:
                    pass
                return try_acquire_lock(lock_path)
        except Exception:
            pass
        return None
    except Exception:
        return None


def release_lock(lock_path: str, fd: int | None) -> None:
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass
    try:
        os.remove(lock_path)
    except FileNotFoundError:
        pass


# --- Environment Helpers ---

def get_env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is not None and str(v).strip():
        return v
    # Fallback: manual parse .env
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, val = line.split("=", 1)
                    if k.strip() == name:
                        return val.strip().strip("'").strip('"')
        except Exception:
            pass
    return default


def get_db_path() -> str:
    data_dir = get_env("MEMU_DATA_DIR")
    if not data_dir:
        data_dir = os.path.expanduser("~/.openclaw/memUdata")
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "memu.db")


def get_sessions_dir() -> str:
    home = os.path.expanduser("~")
    default_path = os.path.join(home, ".openclaw", "agents", "main", "sessions")
    if os.path.exists(default_path):
        return default_path
    return ""


def get_data_dir() -> str:
    data_dir = get_env("MEMU_DATA_DIR")
    if not data_dir:
        data_dir = os.path.expanduser("~/.openclaw/memUdata")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


# --- State Persistence ---

def get_last_sync_marker_path() -> str:
    return os.path.join(get_data_dir(), "last_sync_ts")


def get_pending_queue_path() -> str:
    return os.path.join(get_data_dir(), "pending_ingest.json")


def get_backoff_path() -> str:
    return os.path.join(get_data_dir(), "pending_backoff.json")


def get_empty_sync_log_marker_path() -> str:
    return os.path.join(get_data_dir(), "empty_sync_log.marker")


def read_last_sync() -> float:
    try:
        with open(get_last_sync_marker_path(), "r", encoding="utf-8") as f:
            return float(f.read().strip() or "0")
    except Exception:
        return 0.0


def write_last_sync(ts: float) -> None:
    marker = get_last_sync_marker_path()
    os.makedirs(os.path.dirname(marker), exist_ok=True)
    with open(marker, "w", encoding="utf-8") as f:
        f.write(str(ts))


def load_pending_queue() -> list[str]:
    try:
        with open(get_pending_queue_path(), "r", encoding="utf-8") as f:
            payload = json.load(f)
        paths = payload.get("paths") if isinstance(payload, dict) else None
        if isinstance(paths, list):
            return [p for p in paths if isinstance(p, str) and p.strip()]
    except Exception:
        pass
    return []


def save_pending_queue(paths: list[str]) -> None:
    marker = get_pending_queue_path()
    tmp = marker + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"version": 1, "paths": paths}, f, ensure_ascii=False, indent=2)
    os.replace(tmp, marker)


def load_backoff_state() -> dict:
    try:
        with open(get_backoff_path(), "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {"next_retry_ts": 0.0, "consecutive_rate_limits": 0, "reason": ""}


def save_backoff_state(state: dict) -> None:
    marker = get_backoff_path()
    tmp = marker + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, marker)


def should_log_empty_sync() -> bool:
    interval = int(get_env("MEMU_EMPTY_SYNC_LOG_INTERVAL_SECONDS", "300") or "300")
    if interval <= 0:
        return True
    marker = get_empty_sync_log_marker_path()
    now_ts = time.time()
    try:
        with open(marker, "r", encoding="utf-8") as f:
            last = float((f.read() or "0").strip())
        if now_ts - last < interval:
            return False
    except Exception:
        pass
    try:
        os.makedirs(os.path.dirname(marker), exist_ok=True)
        with open(marker, "w", encoding="utf-8") as f:
            f.write(str(now_ts))
    except Exception:
        pass
    return True


# --- Logging ---

def log(msg: str) -> None:
    """Log to stderr and file."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, file=sys.stderr, flush=True)
    
    log_file = os.path.join(get_data_dir(), "sync.log")
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# --- Database Helpers ---

def db_has_column(conn: sqlite3.Connection, *, table: str, column: str) -> bool:
    try:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = [row[1] for row in cur.fetchall() if len(row) > 1]
        return column in set(cols)
    except Exception:
        return False


def resource_exists(resource_url: str, user_id: str) -> bool:
    """Check if resource already exists in database."""
    try:
        db_path = get_db_path()
        if not os.path.exists(db_path):
            return False
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='memu_resources'"
        )
        if not cursor.fetchone():
            conn.close()
            return False
        
        if db_has_column(conn, table="memu_resources", column="user_id"):
            cursor.execute(
                "SELECT 1 FROM memu_resources WHERE url = ? AND user_id = ? LIMIT 1",
                (resource_url, user_id),
            )
        else:
            cursor.execute(
                "SELECT 1 FROM memu_resources WHERE url = ? LIMIT 1",
                (resource_url,),
            )
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    except Exception as e:
        log(f"DB check failed: {e}")
        return False


def get_db_stats() -> dict:
    """Get database statistics."""
    db_path = get_db_path()
    
    if not os.path.exists(db_path):
        return {"exists": False, "items": 0, "categories": 0, "resources": 0}
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    stats = {"exists": True, "items": 0, "categories": 0, "resources": 0}
    
    try:
        cur.execute("SELECT COUNT(*) FROM memu_memory_items")
        stats["items"] = cur.fetchone()[0]
    except Exception:
        pass
    
    try:
        cur.execute("SELECT COUNT(*) FROM memu_memory_categories")
        stats["categories"] = cur.fetchone()[0]
    except Exception:
        pass
    
    try:
        cur.execute("SELECT COUNT(*) FROM memu_resources")
        stats["resources"] = cur.fetchone()[0]
    except Exception:
        pass
    
    conn.close()
    return stats


# --- Language Config ---

LANGUAGE_PROMPTS = {
    "zh": """
## Language Override (CRITICAL - MUST FOLLOW)
- ALL output MUST be in Chinese (中文), regardless of example language.
- Use "用户" instead of "the user" or "User".
- The examples in this prompt are in English for reference only.
- You MUST write all memory content in Chinese.
""",
    "en": """
## Language Override
- ALL output MUST be in English.
- Use "the user" to refer to the user.
""",
    "ja": """
## Language Override (重要)
- ALL output MUST be in Japanese (日本語).
- Use "ユーザー" instead of "the user".
""",
}


def _build_identity_prompt() -> str | None:
    """Build an identity-awareness prompt block for memory extraction.

    Ensures the LLM correctly distinguishes human user messages from AI
    assistant messages when extracting memories.
    Uses bilingual Chinese instructions for maximum clarity with Chinese LLMs.
    """
    user_name = get_env("MEMU_USER_NAME", "").strip()
    assistant_name = get_env("MEMU_ASSISTANT_NAME", "").strip()

    user_label = f'"{user_name}"' if user_name else '"用户"'
    asst_label = f'"{assistant_name}"' if assistant_name else '"助手"'

    lines = [
        "## 身份与角色归属规则 (Identity & Role Attribution - CRITICAL)",
        "",
        "### 角色定义",
        "这是一段人类用户与 AI 助手之间的对话。",
    ]
    if user_name:
        lines.append(f'- 人类用户名字是 "{user_name}"，消息 role="user" 来自此人。')
    else:
        lines.append('- 消息 role="user" 来自人类用户。')
    if assistant_name:
        lines.append(f'- AI 助手名字是 "{assistant_name}"，消息 role="assistant" 来自此 AI。')
    else:
        lines.append('- 消息 role="assistant" 来自 AI 助手。')

    lines.extend([
        "",
        "### 归属规则",
        "",
        "规则1：区分「发出指令」与「执行行为」",
        f"当用户说「读取 HEARTBEAT.md 并遵循其指示」时：",
        f"  - 执行读取的是助手 => 记录为：{asst_label} 执行心跳检查流程",
        f"  - 错误：「用户会定期读取 HEARTBEAT.md 文件」(用户没有读取，是助手在读取)",
        "",
        "规则2：助手的行为模式归属于助手",
        f"对话中助手展现的工作流程、检查步骤、回复模式，全部归属于 {asst_label}。",
        f"  - 正确：{asst_label} 在处理任务前会先进行安全检查、日志检查",
        f"  - 错误：「用户在处理任务时，会先进行安全检查」",
        "",
        "规则3：system 消息中的指令描述的是助手的职责",
        f"  - 正确：{asst_label} 遵循心跳检查流程",
        f"  - 错误：「用户遵循心跳检查流程」",
        "",
        "规则4：skill / tool / behavior 类型的主语判断",
        f"  - skill/tool 通常是助手展示/使用的 => 主语用 {asst_label}",
        f"  - 用户明确表达的习惯/偏好 => 主语用 {user_label}",
        "",
        "规则5：HEARTBEAT 内容的归类",
        "  - HEARTBEAT.md 描述的是助手的运行时行为协议（检查日志、读取记忆等步骤）",
        "  - 这些步骤应归类为 behavior（行为模式），而不是 skill（技术能力）或 tool（工具调用）",
        f"  - 正确示例：{asst_label} 在每次对话开始时执行心跳检查 [behavior]",
        f"  - 错误示例：{asst_label} 具备心跳检查技能 [skill]",
        "",
        "规则6：skill / tool 的正确语义（在对话提取中）",
        "  - skill = 在本次对话中观察到的、助手实际展现的技术能力或解决问题的方法论",
        "    例：助手在对话中演示了从多个来源综合研究信息的方法 => skill",
        "  - tool = 在本次对话中实际发生的工具调用经验、调用结果、踩坑记录",
        "    例：tavily-search 返回了无关结果，助手通过改写 query 解决 => tool",
        "  - 禁止：仅凭文档内容描述某工具/技能的存在就创建 skill/tool 条目",
        "    （工具的存在性是 knowledge，不是 skill/tool）",
        "",
        "规则7：绝对禁止",
        f'  - 禁止将所有记忆都使用 "用户" 作为主语',
        f"  - 禁止将助手的工作流、检查流程归属给用户",
        f"  - 若不确定，记录为客观知识（knowledge）",
    ])

    return "\n".join(lines)


def build_memorize_config(lang: str | None, session_ts: float | None = None) -> MemorizeConfig:
    """Build MemorizeConfig with custom prompts for identity-aware extraction."""
    memory_types = ["profile", "event", "knowledge", "behavior", "skill", "tool"]
    base_config = {
        "memory_types": memory_types,
        "enable_item_references": True,
        "enable_item_reinforcement": True,
    }

    type_prompts: dict[str, CustomPrompt] = {}
    blocks: dict[str, PromptBlock] = {}

    # Identity/attribution prompt (ordinal 30)
    identity_prompt = _build_identity_prompt()
    if identity_prompt:
        blocks["identity"] = PromptBlock(ordinal=30, prompt=identity_prompt)

    # Temporal context (ordinal 32): inject session time so LLM can timestamp events.
    # memu's XML parser only captures <content>, so timestamps must live inside
    # the content string itself. Format: [YYYY-MM-DD HH:MM]
    if session_ts:
        from datetime import datetime, timezone
        # Convert UTC unix timestamp to local time string
        session_dt = datetime.fromtimestamp(session_ts).strftime("%Y-%m-%d %H:%M")
    else:
        from datetime import datetime
        session_dt = datetime.now().strftime("%Y-%m-%d %H:%M")
    blocks["temporal"] = PromptBlock(
        ordinal=32,
        prompt=(
            f"## 时间标记规则 (Temporal Anchoring - CRITICAL)\n"
            f"本次对话会话发生于: **{session_dt}**\n"
            f"\n"
            f"请在 event 类型的每条记忆的 content 末尾附加时间标记：\n"
            f"  - 对话中有明确时间提及时 → 使用具体时间，格式: [发生于 YYYY-MM-DD HH:MM]\n"
            f"  - 对话中无明确时间时 → 使用会话时间，格式: [发生于 {session_dt}]\n"
            f"\n"
            f"示例：\n"
            f"  正确: 夕河阳和白泽完成了 memU 插件开发并通过测试 [发生于 {session_dt}]\n"
            f"  错误: 夕河阳和白泽完成了 memU 插件开发并通过测试\n"
            f"\n"
            f"profile / knowledge / behavior / skill / tool 类型**无需**时间标记。"
        ),
    )

    # Language prompt (ordinal 35)
    if lang and lang in LANGUAGE_PROMPTS:
        blocks["language"] = PromptBlock(ordinal=35, prompt=LANGUAGE_PROMPTS[lang])

    # XML safety rule (ordinal 40): memu-py parses LLM output as XML;
    # bare < > & characters in summaries break the parser ("Failed to parse XML")
    blocks["xml_safe"] = PromptBlock(
        ordinal=40,
        prompt=(
            "## XML Output Safety (CRITICAL)\n"
            "Your output is parsed as XML. NEVER use bare < > & characters in any field.\n"
            "- Do NOT write file paths like angle-bracket filename, code comparisons like a < b\n"
            "- Replace < with 'lt', > with 'gt', & with 'and'\n"
            "- Do NOT include markdown code blocks or angle-bracket HTML in memory summaries"
        ),
    )

    if blocks:
        for mt in memory_types:
            type_prompts[mt] = CustomPrompt(root=blocks.copy())

    return MemorizeConfig(
        **base_config,
        **(dict(memory_type_prompts=type_prompts) if type_prompts else {}),
    )


def _read_part_session_ts(part_path: str) -> float | None:
    """Derive the session start Unix timestamp from the sidecar meta file.

    convert_sessions writes {session_id}.session.meta.json alongside part files.
    Returns None if the file is missing or the timestamp cannot be parsed.
    """
    import re as _re
    from datetime import datetime as _dt

    base = os.path.basename(part_path)
    # Session id is the UUID portion before '.part{NNN}.json' or '.json'
    m = _re.match(r'^(.+?)(?:\.part\d+)?\.json$', base)
    if not m:
        return None
    session_id = m.group(1)
    # Remove any trailing '.tail.tmp' that might appear in edge cases
    session_id = session_id.replace(".tail.tmp", "")
    meta_dir = os.path.dirname(part_path)
    meta_path = os.path.join(meta_dir, f"{session_id}.session.meta.json")
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        first_ts = str(meta.get("session_start", ""))
        if not first_ts:
            return None
        # ISO-8601 with optional trailing Z
        dt = _dt.fromisoformat(first_ts.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return None


def build_service(session_ts: float | None = None) -> MemoryService:
    """Build MemoryService with configuration from environment."""
    chat_kwargs = {}
    if p := get_env("MEMU_CHAT_PROVIDER"):
        chat_kwargs["provider"] = p
    if u := get_env("MEMU_CHAT_BASE_URL"):
        chat_kwargs["base_url"] = u
    if k := get_env("MEMU_CHAT_API_KEY"):
        chat_kwargs["api_key"] = k
    if m := get_env("MEMU_CHAT_MODEL"):
        chat_kwargs["chat_model"] = m
    chat_config = LLMConfig(**chat_kwargs) if chat_kwargs else LLMConfig()

    embed_kwargs = {}
    if p := get_env("MEMU_EMBED_PROVIDER"):
        embed_kwargs["provider"] = p
    if u := get_env("MEMU_EMBED_BASE_URL"):
        embed_kwargs["base_url"] = u
    if k := get_env("MEMU_EMBED_API_KEY"):
        embed_kwargs["api_key"] = k
    if m := get_env("MEMU_EMBED_MODEL"):
        embed_kwargs["embed_model"] = m
    embed_config = LLMConfig(**embed_kwargs) if embed_kwargs else LLMConfig()

    db_config = DatabaseConfig(
        metadata_store=MetadataStoreConfig(
            provider="sqlite",
            dsn=f"sqlite:///{get_db_path()}",
        )
    )

    output_lang = get_env("MEMU_OUTPUT_LANG", "")
    memorize_config = build_memorize_config(output_lang, session_ts=session_ts)

    return MemoryService(
        llm_profiles={"default": chat_config, "embedding": embed_config},
        database_config=db_config,
        memorize_config=memorize_config,
    )


# --- Rate Limit Detection ---

def is_rate_limited_error(e: Exception) -> bool:
    """Detect rate limit errors from various providers."""
    text = f"{type(e).__name__}: {e}".lower()
    return (
        "ratelimit" in text
        or "rate limit" in text
        or "error code: 429" in text
        or "'code': '1302'" in text
        or '"code": "1302"' in text
    )


# --- Main Sync Logic ---

async def sync_sessions() -> dict:
    """Sync OpenClaw sessions to memU database."""
    user_id = get_env("MEMU_USER_ID") or "default"
    
    # Acquire lock to prevent concurrent runs
    lock_fd = try_acquire_lock(FLUSH_LOCK)
    if lock_fd is None:
        log("flush already running; skip")
        return {
            "status": "skipped",
            "message": "Another sync is already running",
            "synced_items": 0,
        }
    
    try:
        last_sync = read_last_sync()
        sync_start_ts = time.time()
        
        # Load pending queue from previous runs
        pending_paths = load_pending_queue()
        
        # Convert updated sessions
        converted_paths = convert_sessions.convert(since_ts=last_sync)
        
        # Merge with pending queue (dedupe, preserve order)
        merged: list[str] = []
        seen: set[str] = set()
        for p in [*pending_paths, *converted_paths]:
            if not isinstance(p, str) or not p.strip():
                continue
            if p in seen:
                continue
            seen.add(p)
            merged.append(p)
        save_pending_queue(merged)
        
        # Check backoff
        backoff = load_backoff_state()
        now_ts = time.time()
        next_retry_ts = float(backoff.get("next_retry_ts", 0.0) or 0.0)
        
        log(f"sync start. since_ts={last_sync}")
        log(f"converted_paths: {len(converted_paths)}")
        log(f"pending_paths: {len(merged)}")
        
        if merged and next_retry_ts > now_ts:
            wait_s = int(next_retry_ts - now_ts)
            log(f"backoff active: skip ingest for {wait_s}s (reason={backoff.get('reason', 'rate_limit')})")
            return {
                "status": "backoff",
                "message": f"Rate limit backoff: {wait_s}s remaining",
                "synced_items": get_db_stats().get("items", 0),
                "pending": len(merged),
            }
        
        if not merged:
            if should_log_empty_sync():
                log("no updated sessions to ingest")
            write_last_sync(sync_start_ts)
            save_backoff_state({"next_retry_ts": 0.0, "consecutive_rate_limits": 0, "reason": ""})
            return {
                "status": "success",
                "message": "No new sessions to sync",
                "synced_items": get_db_stats().get("items", 0),
            }
        
        # Build service and ingest
        log(
            f"llm profiles: chat={get_env('MEMU_CHAT_PROVIDER', 'openai')}/{get_env('MEMU_CHAT_MODEL', 'unknown')} "
            f"embed={get_env('MEMU_EMBED_PROVIDER', 'openai')}/{get_env('MEMU_EMBED_MODEL', 'unknown')}"
        )

        ok = 0
        fail = 0
        skipped = 0
        errors = []

        timeout_s = int(get_env("MEMU_MEMORIZE_TIMEOUT_SECONDS", "600") or "600")
        base_backoff_s = int(get_env("MEMU_RATE_LIMIT_BACKOFF_SECONDS", "60") or "60")
        max_backoff_s = int(get_env("MEMU_RATE_LIMIT_BACKOFF_MAX_SECONDS", "900") or "900")
        consecutive_rate_limits = int(backoff.get("consecutive_rate_limits", 0) or 0)
        saw_rate_limit = False

        remaining: list[str] = []

        for p in merged:
            # Skip if already exists
            if resource_exists(p, user_id):
                log(f"skip existing: {os.path.basename(p)}")
                skipped += 1
                continue

            # Build per-part service so the temporal block carries the real conversation time
            session_ts = _read_part_session_ts(p)
            try:
                service = build_service(session_ts=session_ts)
            except Exception as e:
                log(f"Failed to create MemoryService for {os.path.basename(p)}: {e}")
                fail += 1
                remaining.append(p)
                continue

            try:
                base = os.path.basename(p)
                t0 = time.time()
                log(f"ingest: {base}")
                
                await asyncio.wait_for(
                    service.memorize(
                        resource_url=p,
                        modality="conversation",
                        user={"user_id": user_id},
                    ),
                    timeout=timeout_s,
                )
                ok += 1
                log(f"done: {base} ({time.time() - t0:.1f}s)")
                
            except asyncio.TimeoutError:
                log(f"TIMEOUT: {os.path.basename(p)} (>{timeout_s}s)")
                fail += 1
                remaining.append(p)
            except Exception as e:
                log(f"ERROR: {os.path.basename(p)} - {type(e).__name__}: {e}")
                fail += 1
                remaining.append(p)
                if is_rate_limited_error(e):
                    saw_rate_limit = True
        
        # Summary
        log(f"sync complete. success={ok}, failed={fail}, skipped={skipped}")
        
        # Save remaining queue
        save_pending_queue(remaining)
        
        # Update state
        if fail == 0:
            write_last_sync(sync_start_ts)
            save_backoff_state({"next_retry_ts": 0.0, "consecutive_rate_limits": 0, "reason": ""})
        else:
            log("sync cursor not advanced due to failures")
            if saw_rate_limit:
                consecutive_rate_limits += 1
                wait_s = min(max_backoff_s, base_backoff_s * (2 ** (consecutive_rate_limits - 1)))
                next_retry_ts = time.time() + wait_s
                save_backoff_state({
                    "next_retry_ts": next_retry_ts,
                    "consecutive_rate_limits": consecutive_rate_limits,
                    "reason": "rate_limit",
                })
                log(f"rate-limit backoff set: {wait_s}s (attempt={consecutive_rate_limits})")
        
        db_stats = get_db_stats()
        result = {
            "status": "success" if fail == 0 else "partial",
            "message": f"Synced {ok} session(s), failed {fail}, skipped {skipped}",
            "synced_items": db_stats.get("items", 0),
            "converted": len(converted_paths),
            "ingested": ok,
            "failed": fail,
            "skipped": skipped,
            "pending": len(remaining),
            "database": db_stats,
        }
        
        if errors:
            result["errors"] = errors[:5]
        
        return result
        
    finally:
        release_lock(FLUSH_LOCK, lock_fd)


if __name__ == "__main__":
    try:
        result = asyncio.run(sync_sessions())
        print(json.dumps(result, ensure_ascii=False))
    except Exception as e:
        print(
            json.dumps(
                {
                    "status": "error",
                    "message": str(e),
                    "synced_items": 0,
                },
                ensure_ascii=False,
            )
        )
        sys.exit(1)
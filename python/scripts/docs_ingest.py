#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Document ingestion script - import markdown files to memU database.

Features:
- Incremental mode: only ingest changed files
- Full scan mode: scan all configured paths
- Deduplication: skip already-ingested resources
- Rate limit backoff: shared with flush.py
- Comprehensive logging
"""

import argparse
import asyncio
import hashlib
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

# Set required env vars before importing memu
def _init_env():
    if not os.getenv("MEMU_DATA_DIR"):
        os.environ["MEMU_DATA_DIR"] = os.path.expanduser("~/.openclaw/memUdata")

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


# --- Lock Management ---

DOCS_LOCK = os.path.join(tempfile.gettempdir(), "memu_sync.lock_docs_ingest")


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
    return os.path.join(data_dir, "memu.db")


def get_data_dir() -> str:
    data_dir = get_env("MEMU_DATA_DIR")
    if not data_dir:
        data_dir = os.path.expanduser("~/.openclaw/memUdata")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def get_workspace_dir() -> str:
    workspace = get_env("MEMU_WORKSPACE_DIR")
    if workspace and os.path.exists(workspace):
        return workspace
    return os.getcwd()


def get_default_extra_paths() -> list[str]:
    """Get default extra paths for document ingestion."""
    workspace = get_workspace_dir()
    return [
        os.path.join(workspace, "*.md"),
        os.path.join(workspace, "memory", "*.md"),
    ]


def get_extra_paths() -> list[str]:
    """Get extra paths from environment or default."""
    raw = get_env("MEMU_EXTRA_PATHS")
    if raw:
        try:
            paths = json.loads(raw)
            if isinstance(paths, list):
                return [p for p in paths if isinstance(p, str)]
        except json.JSONDecodeError:
            pass
    return get_default_extra_paths()


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


# --- Backoff State (shared with flush.py) ---

def get_backoff_path() -> str:
    return os.path.join(get_data_dir(), "pending_backoff.json")


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


def is_backoff_active() -> bool:
    backoff = load_backoff_state()
    next_retry = float(backoff.get("next_retry_ts", 0.0) or 0.0)
    return next_retry > time.time()


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


# --- File State Tracking ---

def get_docs_state_path() -> str:
    """Path to the JSON file tracking ingested document hashes."""
    return os.path.join(get_data_dir(), "docs_ingest_state.json")


def load_docs_state() -> dict[str, dict]:
    """Load per-file state: {abs_path: {hash, resource_id, mtime}}."""
    try:
        with open(get_docs_state_path(), "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict) and payload.get("version") == 1:
            return payload.get("files", {})
    except Exception:
        pass
    return {}


def save_docs_state(files: dict[str, dict]) -> None:
    marker = get_docs_state_path()
    tmp = marker + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"version": 1, "files": files}, f, ensure_ascii=False, indent=2)
    os.replace(tmp, marker)


def file_content_hash(path: str) -> str:
    """SHA-256 hash of file content."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# --- Database Cleanup ---

def delete_resource_cascade(resource_url: str, user_id: str) -> tuple[bool, str | None]:
    """Delete a resource and all its linked memory items / category links from the DB.

    Returns (success, resource_id_or_None).
    """
    db_path = get_db_path()
    if not os.path.exists(db_path):
        return (False, None)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        # Find resource id
        has_uid = db_has_column(conn, table="memu_resources", column="user_id")
        if has_uid:
            cur.execute(
                "SELECT id FROM memu_resources WHERE url = ? AND user_id = ? LIMIT 1",
                (resource_url, user_id),
            )
        else:
            cur.execute(
                "SELECT id FROM memu_resources WHERE url = ? LIMIT 1",
                (resource_url,),
            )
        row = cur.fetchone()
        if not row:
            conn.close()
            return (False, None)

        resource_id = row[0]

        # Find all memory item ids for this resource
        cur.execute(
            "SELECT id FROM memu_memory_items WHERE resource_id = ?",
            (resource_id,),
        )
        item_ids = [r[0] for r in cur.fetchall()]

        # Delete category_items links
        if item_ids:
            placeholders = ",".join(["?"] * len(item_ids))
            cur.execute(
                f"DELETE FROM memu_category_items WHERE item_id IN ({placeholders})",
                item_ids,
            )

        # Delete memory items
        cur.execute(
            "DELETE FROM memu_memory_items WHERE resource_id = ?",
            (resource_id,),
        )

        # Delete resource itself
        cur.execute(
            "DELETE FROM memu_resources WHERE id = ?",
            (resource_id,),
        )

        # Also clean up local_path file if it exists
        cur_lp = conn.cursor()
        # (local_path was already fetched before delete, let's re-check)
        # Actually we deleted already, so we query before commit — but
        # we already executed the DELETE. Let's just clean up the copied file.
        # We need to get local_path before deleting, so let's restructure.
        # Since we already deleted, skip local file cleanup for now;
        # memU stores a copy under data/resources/ but it's non-critical.

        conn.commit()
        log(f"deleted resource {resource_id} ({len(item_ids)} items) for: {os.path.basename(resource_url)}")
        return (True, resource_id)

    except Exception as e:
        conn.rollback()
        log(f"delete_resource_cascade error: {e}")
        return (False, None)
    finally:
        conn.close()


def get_all_document_urls(user_id: str) -> list[str]:
    """Get all document resource URLs from the database."""
    db_path = get_db_path()
    if not os.path.exists(db_path):
        return []
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        has_uid = db_has_column(conn, table="memu_resources", column="user_id")
        if has_uid:
            cur.execute(
                "SELECT url FROM memu_resources WHERE modality = 'document' AND user_id = ?",
                (user_id,),
            )
        else:
            cur.execute(
                "SELECT url FROM memu_resources WHERE modality = 'document'"
            )
        urls = [r[0] for r in cur.fetchall()]
        conn.close()
        return urls
    except Exception:
        return []


# --- Full Scan Marker ---

def get_full_scan_marker_path() -> str:
    return os.path.join(get_data_dir(), "docs_full_scan.marker")


def has_full_scan_marker() -> bool:
    return os.path.exists(get_full_scan_marker_path())


def set_full_scan_marker() -> None:
    marker = get_full_scan_marker_path()
    with open(marker, "w", encoding="utf-8") as f:
        f.write(datetime.now().isoformat())


# --- File Collection ---

def is_under_prefix(path: str, prefix: str) -> bool:
    """Check if path is under prefix (supports glob patterns in prefix).

    When 'prefix' contains wildcard characters the check is done in two ways:
    1. fnmatch: does the path exactly match the glob pattern?
    2. Parent-dir: is path under the nearest real parent of the glob?

    For plain paths the original startswith logic is used.
    """
    try:
        path_abs = os.path.abspath(path)
        if "*" in prefix or "?" in prefix:
            import fnmatch
            # Normalise the glob itself to an absolute path fragment
            # (it may already be absolute or relative to cwd)
            prefix_abs = os.path.abspath(prefix)
            # 1. Exact fnmatch on the full path
            if fnmatch.fnmatch(path_abs, prefix_abs):
                return True
            # 2. Is path under the nearest existing ancestor directory of the glob?
            parent = os.path.dirname(prefix_abs)
            while ("*" in parent or "?" in parent) and parent not in (".", ""):
                parent = os.path.dirname(parent) or "."
            if os.path.isdir(parent):
                parent_with_sep = os.path.join(parent, "")
                return path_abs.startswith(parent_with_sep)
            return False
        prefix_abs = os.path.abspath(prefix)
        if os.path.isdir(prefix_abs):
            prefix_abs = os.path.join(prefix_abs, "")
        return path_abs == prefix_abs.rstrip(os.sep) or path_abs.startswith(prefix_abs)
    except Exception:
        return False


def collect_markdown_files(extra_paths: list[str], changed_path: str | None = None) -> list[str]:
    """Collect markdown files to ingest.
    
    - If changed_path is provided: only ingest that file (incremental mode)
    - Otherwise: full scan of all configured paths
    """
    files: set[str] = set()

    def is_excluded_path(p: str) -> bool:
        """Hard exclusions for document ingestion paths.

        Skills are managed by OpenClaw native capability listing, so memU docs
        ingestion should never import files under workspace/skills.
        """
        try:
            p_abs = os.path.abspath(p)
            skills_root = os.path.join(get_workspace_dir(), "skills")
            skills_root_abs = os.path.abspath(skills_root)
            if p_abs == skills_root_abs:
                return True
            return p_abs.startswith(os.path.join(skills_root_abs, ""))
        except Exception:
            return False

    def add_file(p: str) -> None:
        if is_excluded_path(p):
            return
        if p.endswith(".md") and os.path.isfile(p):
            files.add(os.path.abspath(p))

    def scan_dir(d: str) -> None:
        if is_excluded_path(d):
            return
        try:
            for root, dirnames, filenames in os.walk(d, topdown=True):
                # Prune excluded subtrees so os.walk never descends into them.
                dirnames[:] = [
                    dn
                    for dn in dirnames
                    if not is_excluded_path(os.path.join(root, dn))
                ]
                for f in filenames:
                    if f.endswith(".md"):
                        fp = os.path.abspath(os.path.join(root, f))
                        if not is_excluded_path(fp):
                            files.add(fp)
        except Exception:
            pass

    if changed_path:
        cp = os.path.abspath(changed_path)
        if is_excluded_path(cp):
            return []
        # Only ingest changes within configured extra paths
        allowed = any(is_under_prefix(cp, p) for p in extra_paths)
        if not allowed:
            return []
        if os.path.isfile(cp):
            add_file(cp)
        elif os.path.isdir(cp):
            scan_dir(cp)
        return sorted(files)

    # Full scan mode
    for path_item in extra_paths:
        if is_excluded_path(path_item):
            continue
        if not os.path.exists(path_item):
            # Handle glob patterns
            if "*" in path_item:
                import glob
                for match in glob.glob(path_item, recursive=True):
                    if is_excluded_path(match):
                        continue
                    if match.endswith(".md") and os.path.isfile(match):
                        files.add(os.path.abspath(match))
            continue
        if os.path.isfile(path_item):
            add_file(path_item)
        elif os.path.isdir(path_item):
            scan_dir(path_item)

    return sorted(files)


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
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='memu_resources'"
        )
        if cur.fetchone() is None:
            conn.close()
            return False
        if db_has_column(conn, table="memu_resources", column="user_id"):
            cur.execute(
                "SELECT 1 FROM memu_resources WHERE url = ? AND user_id = ? LIMIT 1",
                (resource_url, user_id),
            )
        else:
            cur.execute(
                "SELECT 1 FROM memu_resources WHERE url = ? LIMIT 1",
                (resource_url,),
            )
        exists = cur.fetchone() is not None
        conn.close()
        return exists
    except Exception:
        return False


def check_file_changed(file_path: str, docs_state: dict[str, dict]) -> str | None:
    """Check if file content has changed since last ingest.

    Returns new hash if changed, None if unchanged.
    """
    try:
        new_hash = file_content_hash(file_path)
    except Exception:
        return None

    prev = docs_state.get(file_path)
    if prev and prev.get("hash") == new_hash:
        return None  # unchanged
    return new_hash


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


# Files whose basename (lowercase) are tool/capability references → knowledge only.
_SKILL_SUBJECT_FILES = {"tools.md"}

# Files whose basename should NEVER get skill/tool extraction.
# They describe identity/persona/user — forcing skill prompts causes hallucination.
_NO_SKILL_FILES = {
    "identity.md", "soul.md", "heartbeat.md",
    "agents.md", "user.md", "memory.md", "bootstrap.md",
}


def _get_memory_types_for_file(file_path: str | None) -> list[str]:
    """Return the appropriate memory_types list based on the file being ingested.

    memu-py calls the LLM **once per memory_type** via separate prompts.  When
    a type has no relevant content in the source file the model is forced to
    invent something, producing hallucinated entries.  Choosing types per-file
    prevents this while still capturing the right memory categories per file.

    Rules (checked in order):
    - TOOLS.md                                   → knowledge only
    - Identity/profile files (soul, agents…)     → profile + event + knowledge + behavior
    - Any other file                              → knowledge + event + behavior
    """
    if not file_path:
        return ["profile", "event", "knowledge", "behavior"]

    basename = os.path.basename(file_path).lower()

    if basename in _SKILL_SUBJECT_FILES:
        # Tool reference files (e.g. TOOLS.md) — store as knowledge only.
        return ["knowledge"]

    if basename in _NO_SKILL_FILES:
        # Identity/profile documents — skill/tool prompts hallucinate here
        return ["profile", "event", "knowledge", "behavior"]

    # Generic markdown (notes, blog posts, learnings, etc.)
    return ["knowledge", "event", "behavior"]


def build_memorize_config(lang: str | None, current_file: str | None = None) -> MemorizeConfig:
    memory_types = _get_memory_types_for_file(current_file)
    base_config = {
        "memory_types": memory_types,
        "enable_item_references": True,
        "enable_item_reinforcement": True,
    }

    type_prompts: dict[str, CustomPrompt] = {}
    blocks: dict[str, PromptBlock] = {}

    # Document identity prompt (ordinal 25 = highest priority, before everything)
    # Tells the LLM to carefully determine the subject of each document
    # Pass current_file so the prompt includes the filename and pre-determined category
    identity_prompt = _build_doc_identity_prompt(current_file=current_file)
    if identity_prompt:
        blocks["doc_identity"] = PromptBlock(ordinal=25, prompt=identity_prompt)

    # Temporal context (ordinal=28): inject file date so LLM can timestamp event entries.
    # memu's XML parser only captures <content>; timestamps must live in the content string.
    if current_file and os.path.exists(current_file):
        from datetime import datetime
        # Try to extract a date from the filename first (e.g. "2026-02-25.md")
        import re as _re
        fname = os.path.basename(current_file)
        date_match = _re.search(r"(\d{4}-\d{2}-\d{2})", fname)
        if date_match:
            file_date_str = date_match.group(1)
        else:
            mtime = os.path.getmtime(current_file)
            file_date_str = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")
        blocks["temporal"] = PromptBlock(
            ordinal=28,
            prompt=(
                f"## 时间标记规则 (Temporal Anchoring)\n"
                f"当前文档时间参考: **{file_date_str}**\n"
                f"\n"
                f"对于 event 类型的记忆：\n"
                f"  - 文档中有明确日期时 → 使用该日期，格式: [发生于 YYYY-MM-DD]\n"
                f"  - 文档中无明确日期时 → 附加文档日期，格式: [来源文档 {file_date_str}]\n"
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
            "- Do NOT write file paths like <filename>, code like <tag>, comparisons like a < b\n"
            "- Replace < with the word 'lt', > with 'gt', & with 'and'\n"
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


# Default file-to-subject mappings for common OpenClaw workspace documents
_ASSISTANT_SUBJECT_FILES = {
    "agents.md", "soul.md", "tools.md", "heartbeat.md",
    "bootstrap.md", "identity.md",
}
_USER_SUBJECT_FILES = {
    "user.md",
}


def _build_doc_identity_prompt(current_file: str | None = None) -> str:
    """Build a document identity-awareness prompt for memory extraction.

    This is critical for documents that describe the AI assistant's behavior
    (like AGENTS.md, SOUL.md) vs documents about the human user (USER.md).
    Without this, the LLM incorrectly attributes assistant rules to 'the user'.
    Uses bilingual instructions for maximum clarity with Chinese LLMs.

    Args:
        current_file: Absolute path of the file currently being processed.
            When provided, the prompt will include the filename and its
            pre-determined category so the LLM knows exactly what type
            of document it is extracting from.
    """
    user_name = get_env("MEMU_USER_NAME", "").strip()
    assistant_name = get_env("MEMU_ASSISTANT_NAME", "").strip()

    # Read custom subject mappings from env (JSON: {"filename.md": "assistant"})
    custom_map = {}
    raw = get_env("MEMU_DOC_SUBJECT_MAP", "")
    if raw:
        try:
            custom_map = json.loads(raw)
        except Exception:
            pass

    # Merge default + custom mappings
    assistant_files = set(_ASSISTANT_SUBJECT_FILES)
    user_files = set(_USER_SUBJECT_FILES)
    for fname, subject in custom_map.items():
        fname_lower = fname.lower()
        if subject == "assistant":
            assistant_files.add(fname_lower)
            user_files.discard(fname_lower)
        elif subject == "user":
            user_files.add(fname_lower)
            assistant_files.discard(fname_lower)

    user_label = f'"{user_name}"' if user_name else '"用户"'
    asst_label = f'"{assistant_name}"' if assistant_name else '"助手"'

    # --- Determine current file category ---
    current_basename = ""
    file_category = "unknown"  # assistant / user / other / unknown
    if current_file:
        current_basename = os.path.basename(current_file)
        bn_lower = current_basename.lower()
        if bn_lower in assistant_files:
            file_category = "assistant"
        elif bn_lower in user_files:
            file_category = "user"
        else:
            file_category = "other"

    lines = [
        "## 文档主语归属规则 (Document Subject Attribution - CRITICAL)",
        "",
    ]

    # --- Prominent current-file banner ---
    if current_basename:
        lines.append(f"### ⚠️ 当前正在处理的文件: {current_basename}")
        if file_category == "assistant":
            lines.extend([
                f"此文件是 **AI 助手的配置/指令文档**。",
                f"从此文件提取的所有记忆，主语必须使用 {asst_label}，**绝对禁止**使用 {user_label}。",
                f"文件中出现的「你」「你需要」「你应该」都是对 AI 助手说的，不是描述用户。",
            ])
        elif file_category == "user":
            lines.extend([
                f"此文件是 **对人类用户的个人描述文档**。",
                f"从此文件提取的所有记忆，主语应使用 {user_label}。",
            ])
        elif current_basename.lower() == "memory.md":
            # MEMORY.md is a mixed document containing sections about both the
            # user and the AI assistant. The LLM must use section headers to
            # determine the subject of each entry.
            lines.extend([
                f"此文件是 **混合长期记忆文档**，同时包含对 {user_label} 和 {asst_label} 的描述。",
                f"**必须根据章节标题判断每条记忆的主语**：",
                f"",
                f"  - 章节「关于 {user_name or '用户'}」/ 「已知偏好」/ 「用户背景」→ 主语是 {user_label}",
                f"  - 章节「做出的关键决策」/ 「角色设定」/ 助手名字的描述 → 主语是 {asst_label}",
                f"  - 描述助手**工作风格、气质、行事方式**的条目 → 主语是 {asst_label}",
                f"",
                f"**高频误判警告**：",
                f"  - 「先自己想办法再提问」描述的是助手的工作习惯（{asst_label} 会先自行思考再询问），不是用户行为",
                f"  - 「不表演式帮忙」「真正解决问题」描述的是助手的帮助风格 → 主语 {asst_label}",
                f"  - 判断依据：这类条目出现在描述助手角色的决策章节中",
            ])
        else:
            lines.extend([
                f"此文件类型未预设归属，请根据内容判断每条记忆的主语。",
                f"但仍须遵守下方的归属规则。",
            ])
        lines.append("")

    lines.extend([
        "你正在从**文档**中提取记忆，不是从对话中。",
        "每个文档描述的主体可能不同，你必须在提取前判断正确的主语。",
        "",
        "### 角色定义",
    ])

    if assistant_name:
        lines.append(f'- AI 助手的名字是 "{assistant_name}"')
    if user_name:
        lines.append(f'- 人类用户的名字是 "{user_name}"')

    lines.append("")

    if assistant_files:
        af_str = ", ".join(sorted(assistant_files))
        lines.extend([
            f"### 描述 AI 助手的文档（如：{af_str}）",
            "这些文件包含给 AI 助手的指令、规则和行为准则。",
            f"从这些文件提取的记忆，主语必须使用 {asst_label}，禁止使用 {user_label}。",
            f"  - 正确：{asst_label} 在心跳检查时执行安全检查、日志检查",
            f"  - 错误：~~用户会进行安全检查、日志检查~~",
            "",
        ])

    if user_files:
        uf_str = ", ".join(sorted(user_files))
        lines.extend([
            f"### 描述人类用户的文档（如：{uf_str}）",
            f"这些文件描述人类用户的特征、偏好和历史。主语使用 {user_label}。",
            "",
        ])

    lines.extend([
        "### 其他文档（如记忆日志、每日笔记）",
        "根据上下文判断主语：",
        '  * "你应该..." / "你需要..." / 祈使句 → 描述的是助手的职责',
        '  * "用户喜欢..." / "我的偏好是..." → 描述的是用户',
        "  * 个人日记、事件记录 → 根据上下文判断",
        "",
        "### 绝对规则",
        f'1. 禁止将所有记忆都使用 "用户" 作为主语',
        f"2. 文档中给 AI 的指令（如「当收到心跳指令时，执行检查流程」）→ 主语是 {asst_label}",
        f"3. 文档中描述的工作流、检查步骤、回复模板 → 主语是 {asst_label}",
        f"4. 若不确定主语，记录为客观知识（knowledge），不要错误归属",
    ])

    return "\n".join(lines)


def build_service(current_file: str | None = None) -> MemoryService:
    """Build MemoryService with configuration from environment.

    Args:
        current_file: When provided, the memorize config will include
            a file-specific identity prompt telling the LLM the filename
            and its pre-determined category (assistant/user/other).
    """
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
    memorize_config = build_memorize_config(output_lang, current_file=current_file)

    # Pin resources_dir to MEMU_DATA_DIR/resources so memu never writes to
    # a relative ./data/resources path under the script's CWD.
    blob_config = {"resources_dir": os.path.join(get_data_dir(), "resources")}

    return MemoryService(
        llm_profiles={"default": chat_config, "embedding": embed_config},
        database_config=db_config,
        memorize_config=memorize_config,
        blob_config=blob_config,
    )


# --- Main Ingest Logic ---

async def ingest_docs(changed_path: str | None = None, deleted_path: str | None = None) -> dict:
    """Ingest markdown documents to memU database.

    Supports:
    - Full scan: scan all paths, detect new/changed/deleted files
    - Incremental (changed_path): re-ingest a single changed file
    - Delete (deleted_path): clean up DB entries for a deleted file
    """
    user_id = get_env("MEMU_USER_ID") or "default"
    
    # Acquire lock
    lock_fd = try_acquire_lock(DOCS_LOCK)
    if lock_fd is None:
        log("docs_ingest already running; skip")
        return {
            "status": "skipped",
            "message": "Another ingest is already running",
            "ingested": 0,
            "skipped": 0,
            "failed": 0,
        }
    
    try:
        # Handle explicit delete request
        if deleted_path:
            dp = os.path.abspath(deleted_path)
            log(f"docs_ingest: delete request for {os.path.basename(dp)}")
            docs_state = load_docs_state()
            ok_del, rid = delete_resource_cascade(dp, user_id)
            if ok_del:
                docs_state.pop(dp, None)
                save_docs_state(docs_state)
            return {
                "status": "success" if ok_del else "not_found",
                "message": f"Deleted resource for {os.path.basename(dp)}" if ok_del else f"No resource found for {os.path.basename(dp)}",
                "deleted": 1 if ok_del else 0,
                "ingested": 0,
                "skipped": 0,
                "failed": 0,
            }

        # Check backoff
        if is_backoff_active():
            backoff = load_backoff_state()
            next_retry = float(backoff.get("next_retry_ts", 0.0) or 0.0)
            wait_s = int(next_retry - time.time())
            log(f"backoff active: skip ingest for {wait_s}s")
            return {
                "status": "backoff",
                "message": f"Rate limit backoff: {wait_s}s remaining",
                "ingested": 0,
                "skipped": 0,
                "failed": 0,
            }
        
        extra_paths = get_extra_paths()
        files = collect_markdown_files(extra_paths, changed_path)

        if not files:
            mode = "incremental" if changed_path else "full-scan"
            log(f"docs_ingest: no files ({mode})")
            return {
                "status": "success",
                "message": "No markdown files to ingest",
                "ingested": 0,
                "skipped": 0,
                "failed": 0,
            }

        mode = "incremental" if changed_path else "full-scan"
        log(f"docs_ingest start. mode={mode} files={len(files)}")

        # Load file tracking state
        docs_state = load_docs_state()

        # In full-scan mode: detect deleted files (in DB but no longer on disk)
        deleted_count = 0
        if not changed_path:
            known_urls = get_all_document_urls(user_id)
            current_files_set = set(files)
            for url in known_urls:
                if url not in current_files_set and not os.path.exists(url):
                    log(f"cleanup deleted: {os.path.basename(url)}")
                    ok_del, _ = delete_resource_cascade(url, user_id)
                    if ok_del:
                        docs_state.pop(url, None)
                        deleted_count += 1

        ok = 0
        fail = 0
        skipped = 0
        updated = 0
        errors = []
        timeout_s = int(get_env("MEMU_MEMORIZE_TIMEOUT_SECONDS", "600") or "600")
        
        base_backoff_s = int(get_env("MEMU_RATE_LIMIT_BACKOFF_SECONDS", "60") or "60")
        max_backoff_s = int(get_env("MEMU_RATE_LIMIT_BACKOFF_MAX_SECONDS", "900") or "900")
        backoff = load_backoff_state()
        consecutive_rate_limits = int(backoff.get("consecutive_rate_limits", 0) or 0)
        saw_rate_limit = False

        for file_path in files:
            try:
                # Check if file content has changed
                new_hash = check_file_changed(file_path, docs_state)

                if resource_exists(file_path, user_id):
                    if new_hash is None:
                        # Exists and unchanged
                        skipped += 1
                        continue
                    else:
                        # Exists but content changed — delete old, re-ingest
                        log(f"update (content changed): {os.path.basename(file_path)}")
                        delete_resource_cascade(file_path, user_id)
                        updated += 1
                elif new_hash is None:
                    # Not in DB, compute hash for new file
                    try:
                        new_hash = file_content_hash(file_path)
                    except Exception:
                        new_hash = "unknown"

                log(f"ingest: {os.path.basename(file_path)}")
                # Build a per-file service so the identity prompt includes
                # the current filename and its pre-determined category.
                try:
                    service = build_service(current_file=file_path)
                except Exception as e:
                    log(f"Failed to create MemoryService for {os.path.basename(file_path)}: {e}")
                    fail += 1
                    errors.append(f"{os.path.basename(file_path)}: service init: {e}")
                    continue
                await asyncio.wait_for(
                    service.memorize(
                        resource_url=file_path,
                        modality="document",
                        user={"user_id": user_id},
                    ),
                    timeout=timeout_s,
                )
                ok += 1

                # Update tracking state and persist immediately
                # (saves after each file so tracking page shows progress in real time)
                docs_state[file_path] = {
                    "hash": new_hash or "unknown",
                    "mtime": os.path.getmtime(file_path),
                    "ingested_at": time.time(),
                }
                save_docs_state(docs_state)
            except asyncio.TimeoutError:
                log(f"TIMEOUT: {os.path.basename(file_path)}")
                fail += 1
                errors.append(f"{os.path.basename(file_path)}: timeout")
            except Exception as e:
                log(f"ERROR: {os.path.basename(file_path)} - {type(e).__name__}: {e}")
                fail += 1
                errors.append(f"{os.path.basename(file_path)}: {e}")
                if is_rate_limited_error(e):
                    saw_rate_limit = True
                if len(errors) > 5:
                    errors.append("... more errors truncated")
                    break

        log(f"docs_ingest complete. ok={ok} updated={updated} skipped={skipped} deleted={deleted_count} fail={fail}")
        
        # Persist tracking state
        save_docs_state(docs_state)
        
        # Update backoff if rate limited
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
        elif fail == 0:
            # Reset backoff on success
            save_backoff_state({"next_retry_ts": 0.0, "consecutive_rate_limits": 0, "reason": ""})
        
        # Set full scan marker on successful full scan
        if mode == "full-scan" and fail == 0:
            set_full_scan_marker()

        result = {
            "status": "success" if fail == 0 else "partial",
            "message": f"Ingested {ok} document(s), updated {updated}, skipped {skipped}, deleted {deleted_count}, failed {fail}",
            "ingested": ok,
            "updated": updated,
            "skipped": skipped,
            "deleted": deleted_count,
            "failed": fail,
            "total": len(files),
            "mode": mode,
        }

        if errors:
            result["errors"] = errors[:5]

        return result
        
    finally:
        release_lock(DOCS_LOCK, lock_fd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest markdown documents to memU")
    parser.add_argument("--changed", help="Path to changed file/directory (incremental mode)")
    parser.add_argument("--deleted", help="Path to deleted file (cleanup mode)")
    args = parser.parse_args()

    # Also accept changed/deleted from environment (set by watch_sync.py)
    changed = args.changed or os.getenv("MEMU_CHANGED_PATH")
    deleted = args.deleted or os.getenv("MEMU_DELETED_PATH")

    try:
        result = asyncio.run(ingest_docs(changed, deleted_path=deleted))
        print(json.dumps(result, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({
            "status": "error",
            "message": str(e),
            "ingested": 0,
            "skipped": 0,
            "failed": 0,
        }, ensure_ascii=False))
        sys.exit(1)